/*
 * Copyright 2026 ChaconneAI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaconneai.spreader.transport.nio;

import com.chaconneai.spreader.transport.DatagramFragmenter;
import com.chaconneai.spreader.transport.Frames;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;

import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.protocol.Codec;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The UDP transport built on plain NIO.
 *
 * <p>UDP is the conventional choice for SWIM and gossip: a probe is two datagrams, one
 * each way, with no connection setup, and at scale it costs an order of magnitude less
 * than TCP.
 *
 * <p>A single datagram holds at most {@link Frames#MAX_DATAGRAM_BYTES} bytes; anything
 * larger is fragmented by {@link DatagramFragmenter} and reassembled at the receiver, so
 * <b>message size is bounded by {@code maxMessageBytes}, exactly as over TCP</b>.
 * Messages within the limit are sent as they are, with no overhead whatsoever.
 *
 * <p>Lost packets are not retransmitted. Gossip beats packet loss by repeating
 * periodically: one failed probe leads to SWIM's indirect probe, and only a second
 * failure moves the node to SUSPECT -- so the transport need not guarantee reliability
 * itself. Fragments follow the same rule: lose one, lose the message, and no NAK is
 * sent.
 *
 * <p>Requests and responses are matched by the message sequence number {@code seq}, and
 * a stale response arriving late is simply discarded.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class NioUdpTransport implements Transport {

    private static final long SELECT_TIMEOUT_MS = 1_000L;

    private final GossipConfig config;
    private final String bindHost;
    private final int maxMessageBytes;
    private final Logger log;
    private final DatagramFragmenter fragmenter;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong seqGen = new AtomicLong();

    /** Every port currently listened on: the work port plus any claimed cluster port. */
    private final ConcurrentHashMap<Integer, DatagramChannel> listeners = new ConcurrentHashMap<>();

    private volatile MessageHandler handler;
    private volatile int boundPort = -1;

    private Selector selector;
    private Thread eventLoop;
    private final ExecutorService workers;

    public NioUdpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        // With fragmentation, UDP's message ceiling is no longer one datagram but
        // maxMessageBytes, the same as TCP
        this.maxMessageBytes = config.maxMessageBytes();
        this.log = config.log();
        this.fragmenter = new DatagramFragmenter(Frames.MAX_DATAGRAM_BYTES, maxMessageBytes, config.log());
        int workerThreads = config.workerThreads();
        // The producer is the network receive thread: a full queue can only discard,
        // never push back -- stalling that thread would stop gossip heartbeats going out
        // and get this node declared failed
        this.workers = ExecutorUtils.discarding(
                        "gossip-worker", Math.max(2, workerThreads), 4096);
    }

    @Override
    public void setHandler(MessageHandler handler) {
        this.handler = handler;
    }

    @Override
    public int boundPort() {
        return boundPort;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public long nextSeq() {
        return seqGen.incrementAndGet();
    }

    @Override
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            selector = Selector.open();

            boundPort = WorkPortBinder.bind(config, this::bindListener, NioUdpTransport::actualPort);

            eventLoop = new Thread(this::runEventLoop, "gossip-udp-selector");
            eventLoop.setDaemon(true);
            eventLoop.start();

            log.info("Gossip UDP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException e) {
            running.set(false);
            closeAllListeners();
            NioChannelIO.closeQuietly(selector);
            throw e;
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (selector != null) {
            selector.wakeup();
        }
        if (eventLoop != null) {
            try {
                eventLoop.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeAllListeners();
        NioChannelIO.closeQuietly(selector);
        workers.shutdownNow();
        log.info("Gossip UDP transport stopped");
    }

    // ------------------------------------------------------------------
    // Claiming ports
    // ------------------------------------------------------------------

    @Override
    public InetSocketAddress open(int port) {
        if (!running.get()) {
            return null;
        }
        DatagramChannel existing = listeners.get(port);
        if (existing != null) {
            return (InetSocketAddress) quietLocalAddress(existing);
        }
        try {
            DatagramChannel ch = bindListener(port);
            selector.wakeup();
            log.info("Claimed the cluster port {}:{}", bindHost, port);
            return (InetSocketAddress) quietLocalAddress(ch);
        } catch (IOException e) {
            log.debug("Cluster port {} is already taken: {}", port, e.toString());
            return null;
        }
    }

    @Override
    public void close(int port) {
        if (port == boundPort) {
            return;
        }
        DatagramChannel ch = listeners.remove(port);
        if (ch == null) {
            return;
        }
        SelectionKey key = selector == null ? null : ch.keyFor(selector);
        if (key != null) {
            key.cancel();
        }
        NioChannelIO.closeQuietly(ch);
        if (selector != null) {
            selector.wakeup();
        }
        log.info("Released the cluster port {}", port);
    }

    @Override
    public boolean holds(int port) {
        // The socket's REAL state has to be checked; the presence of a key in the map is
        // not enough.
        //
        // "Holds the cluster port" means "I am the leader" -- six places above depend on
        // this answer, isLeader() among them. Once the socket fails without our knowing
        // (the interface went down, the kernel closed it, some code path closed it by
        // mistake), the port is free as far as the kernel is concerned and another node
        // will claim it and become leader. A node consulting only the map would go on
        // believing it is still the leader: two leaders, and not one error anywhere.
        //
        // Dead entries are dropped here as well, so the question is not re-asked every time
        DatagramChannel ch = listeners.get(port);
        if (ch == null) {
            return false;
        }
        if (!ch.isOpen()) {
            listeners.remove(port, ch);
            log.warn("The socket for cluster port {} has failed; this node no longer holds it", port);
            return false;
        }
        return true;
    }

    @Override
    public boolean checkAndRepairWorkPort() {
        if (!running.get()) {
            return false;
        }
        DatagramChannel ch = listeners.get(boundPort);
        if (ch != null && ch.isOpen()) {
            return true;
        }
        // It has failed. Rebind the SAME port, which is completely invisible from outside:
        // the work port has long since travelled the whole cluster with the member list,
        // and changing it would invalidate the address everyone holds.
        //
        // The cleanup must follow the same order as close(port): cancel the key, close the
        // channel, wake the selector. Removing without cancelling the key leaves the JDK to
        // DEFER the channel's close until the selector deregisters it on its next pass, and
        // the port stays occupied until then -- so the rebind is certain to hit
        // "Address already in use". This trap was walked into for real
        DatagramChannel dead = listeners.remove(boundPort);
        if (dead != null) {
            SelectionKey key = selector == null ? null : dead.keyFor(selector);
            if (key != null) {
                key.cancel();
            }
            NioChannelIO.closeQuietly(dead);
            if (selector != null) {
                selector.wakeup();
            }
        }
        return rebindWorkPort();
    }

    /**
     * Rebinds the work port, giving the selector a moment to finish deregistering the
     * previous channel.
     *
     * <p>Deregistration is asynchronous -- it takes effect only after the event loop
     * completes a {@code select()} round -- so hitting "still occupied" on the first bind
     * is entirely normal and does not mean it cannot be fixed. This retries briefly; and
     * failing that, no harm done, since the check is periodic and comes round again.
     */
    private boolean rebindWorkPort() {
        IOException last = null;
        for (int i = 0; i < 10; i++) {
            try {
                bindListener(boundPort);
                log.warn("The socket for work port {} had failed and has been rebound in place", boundPort);
                return true;
            } catch (IOException e) {
                last = e;
                try {
                    Thread.sleep(20L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.error("Work port " + boundPort + " has failed and cannot be rebound for now, so "
                + "this node receives nothing at all (the next check will try again)", last);
        return false;
    }

    private DatagramChannel bindListener(int port) throws IOException {
        DatagramChannel ch = DatagramChannel.open();
        try {
            ch.configureBlocking(false);
            // This MUST be false, and it is not a tuning choice -- it is the correctness
            // of leader election.
            //
            // It once read true, with a comment saying "do not enable SO_REUSEPORT or the
            // port stops being exclusive". But for UDP, SO_REUSEADDR on BSD and macOS
            // behaves EXACTLY LIKE SO_REUSEPORT: two sockets may bind the very same
            // addr:port and both binds succeed. SO_REUSEADDR was meant to solve TCP's
            // TIME_WAIT, and UDP has no TIME_WAIT at all.
            //
            // The consequence is that "whoever claims the cluster port leads" stops working
            // as an election: BOTH nodes claim it successfully, BOTH announce themselves as
            // leader, and the cluster is split -- with no error reported anywhere. Two nodes
            // were caught "claiming successfully" 16ms apart in testing
            ch.setOption(StandardSocketOptions.SO_REUSEADDR, false);
            // Left unset this inherits the system default, which differs by more than
            // threefold between platforms. See Frames.SOCKET_BUFFER_BYTES
            ch.setOption(StandardSocketOptions.SO_RCVBUF, Frames.SOCKET_BUFFER_BYTES);
            ch.setOption(StandardSocketOptions.SO_SNDBUF, Frames.SOCKET_BUFFER_BYTES);
            ch.bind(new InetSocketAddress(bindHost, port));
            ch.register(selector, SelectionKey.OP_READ);
            // Port 0 is assigned by the system, so read back the actual port; the argument
            // must not be used as the key
            listeners.put(actualPort(ch), ch);
            return ch;
        } catch (IOException e) {
            NioChannelIO.closeQuietly(ch);
            throw e;
        }
    }

    private static int actualPort(DatagramChannel ch) throws IOException {
        return ((InetSocketAddress) ch.getLocalAddress()).getPort();
    }

    private void closeAllListeners() {
        for (DatagramChannel ch : listeners.values()) {
            NioChannelIO.closeQuietly(ch);
        }
        listeners.clear();
    }

    private static SocketAddress quietLocalAddress(DatagramChannel ch) {
        try {
            return ch.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Outbound
    // ------------------------------------------------------------------

    @Override
    public Message request(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        List<ByteBuffer> out = encodeFrames(request);
        DatagramChannel ch = DatagramChannel.open();
        // The Selector is reused per thread: opening one per call means an epoll handle
        // per call, which at any real send rate costs more than the data itself
        Selector sel = NioChannelIO.sharedSelector();
        SelectionKey key = null;
        try {
            ch.configureBlocking(false);
            // A response may come back as many fragments in quick succession, and too
            // small a buffer drops some of them right here -- surfacing as "the request
            // went out but a complete answer never arrives"
            ch.setOption(StandardSocketOptions.SO_RCVBUF, Frames.SOCKET_BUFFER_BYTES);
            ch.setOption(StandardSocketOptions.SO_SNDBUF, Frames.SOCKET_BUFFER_BYTES);
            ch.bind(null);
            for (ByteBuffer buf : out) {
                ch.send(buf, addr);
            }

            key = ch.register(sel, SelectionKey.OP_READ);

            ByteBuffer in = allocate(Frames.MAX_DATAGRAM_BYTES);
            long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
            while (true) {
                long remainMs = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainMs <= 0) {
                    throw new SocketTimeoutException("timed out waiting for a response: " + addr);
                }
                sel.select(remainMs);
                sel.selectedKeys().clear();

                in.clear();
                SocketAddress from = ch.receive(in);
                if (from == null) {
                    continue;
                }
                in.flip();
                // A large response spans several datagrams; the complete frame appears
                // only once they have all arrived
                ByteBuffer frame = fragmenter.reassemble(in, (InetSocketAddress) from);
                if (frame == null) {
                    continue;
                }
                Message response = Frames.decode(frame, maxMessageBytes);
                // A mismatched sequence number means this is a late response to an earlier
                // request; discard it and keep waiting
                if (response.seq() == request.seq()) {
                    return response;
                }
            }
        } finally {
            // Cancel the key before closing the channel: a leftover key would interfere
            // with the next use of this Selector
            NioChannelIO.release(sel, key);
            NioChannelIO.closeQuietly(ch);
        }
    }

    /** UDP has no connection to reuse, so this is identical to {@link #request}. */
    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        return request(addr, request, timeoutMs);
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        List<ByteBuffer> out = encodeFrames(message);
        DatagramChannel ch = DatagramChannel.open();
        try {
            ch.configureBlocking(false);
            // A large message goes out as dozens of fragments back to back, and too small
            // a send buffer has the kernel discard them on the spot -- while in non-blocking
            // mode send still reports success, giving not the slightest hint
            ch.setOption(StandardSocketOptions.SO_SNDBUF, Frames.SOCKET_BUFFER_BYTES);
            for (ByteBuffer buf : out) {
                ch.send(buf, addr);
            }
        } finally {
            NioChannelIO.closeQuietly(ch);
        }
    }

    /**
     * Encodes, fragmenting when the result exceeds one datagram.
     *
     * <p>What comes back is a sequence of buffers to send in order; for the overwhelming
     * majority of messages it holds a single element.
     */
    private List<ByteBuffer> encodeFrames(Message msg) throws IOException {
        return fragmenter.split(Codec.encode(msg));
    }

    // ------------------------------------------------------------------
    // Inbound event loop
    // ------------------------------------------------------------------

    /**
     * Allocates the receive buffer according to configuration.
     *
     * @see GossipConfig#directBuffers()
     */
    private ByteBuffer allocate(int capacity) {
        return config.directBuffers()
                ? ByteBuffer.allocateDirect(capacity)
                : ByteBuffer.allocate(capacity);
    }

    private void runEventLoop() {
        // This buffer lives for the whole event loop and is allocated exactly once, which
        // is precisely the shape a direct buffer pays off in: the allocation cost is paid
        // once, and every datagram afterwards saves a copy.
        //
        // A heap buffer would in fact be the most expensive route here. Handed a heap
        // ByteBuffer, the JDK's DatagramChannel ALLOCATES A TEMPORARY DIRECT BUFFER to
        // receive into and then copies back to the heap -- two copies plus an allocation,
        // and that allocation happens every single time
        ByteBuffer in = allocate(Frames.MAX_DATAGRAM_BYTES);
        while (running.get()) {
            try {
                selector.select(SELECT_TIMEOUT_MS);
                if (!running.get()) {
                    break;
                }
                Set<SelectionKey> keys = selector.selectedKeys();
                for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext(); ) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid() || !key.isReadable()) {
                        continue;
                    }
                    drain((DatagramChannel) key.channel(), in);
                }
            } catch (ClosedChannelException e) {
                if (running.get()) {
                    log.error("The selector channel is closed", e);
                }
                break;
            } catch (Throwable t) {
                if (running.get()) {
                    log.error("The gossip UDP event loop threw", t);
                }
            }
        }
    }

    /** Reads every datagram already waiting on a channel and dispatches them. */
    private void drain(DatagramChannel ch, ByteBuffer in) {
        while (true) {
            SocketAddress from;
            in.clear();
            try {
                from = ch.receive(in);
            } catch (IOException e) {
                log.debug("Failed to receive a datagram: {}", e.toString());
                return;
            }
            if (from == null) {
                return;
            }
            in.flip();
            ByteBuffer frame = fragmenter.reassemble(in, (InetSocketAddress) from);
            if (frame == null) {
                // Not all fragments have arrived; wait for the rest
                continue;
            }
            Message msg;
            try {
                msg = Frames.decode(frame, maxMessageBytes);
            } catch (IOException e) {
                // Stray traffic from a port scanner and the like; simply ignore it
                log.debug("Discarding a malformed message from {}: {}", from, e.toString());
                continue;
            }
            if (msg.type().isResponse()) {
                // A response has no business arriving on a listening port: responses are
                // read synchronously on request()'s temporary socket. Anything here is
                // stray or out of step, so discard it -- all four UDP implementations agree
                log.debug("Discarding a response-type message from {}: type={} seq={}; "
                                + "responses should not arrive on a listening port",
                        from, msg.type(), msg.seq());
                continue;
            }
            dispatch(ch, msg, (InetSocketAddress) from);
        }
    }

    /** Processing moves to the worker pool; the response goes straight back to the source address. */
    private void dispatch(DatagramChannel ch, Message msg, InetSocketAddress remote) {
        MessageHandler h = handler;
        if (h == null) {
            return;
        }
        workers.execute(() -> {
            Message response;
            try {
                response = h.handle(msg, remote);
            } catch (Throwable t) {
                log.error("Failed to handle an inbound message: " + msg, t);
                return;
            }
            if (response == null) {
                return;
            }
            try {
                // DatagramChannel.send is thread-safe, so several workers replying at once
                // is fine. Another worker's datagrams may interleave between fragments, and
                // that is harmless -- reassembly groups by msgId
                for (ByteBuffer buf : encodeFrames(response)) {
                    ch.send(buf, remote);
                }
            } catch (IOException e) {
                log.warn("Failed to send a response back to {}: {}", remote, e.toString());
            }
        });
    }
}
