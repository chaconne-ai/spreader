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
package com.chaconneai.spreader.transport.grizzly;

import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.transport.DatagramFragmenter;
import com.chaconneai.spreader.transport.Frames;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.ChannelConfigurator;
import org.glassfish.grizzly.nio.NIOConnection;
import org.glassfish.grizzly.nio.NIOTransport;
import org.glassfish.grizzly.nio.transport.UDPNIOTransport;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.ByteBufferManager;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.nio.transport.UDPNIOTransportBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The UDP transport built on Grizzly.
 *
 * <h2>No framing filter here</h2>
 * A UDP datagram is a complete boundary in itself, so there is no stream framing to do.
 * The real question is what happens when a message exceeds 64KB, which is an application
 * protocol decision, and it goes to {@link DatagramFragmenter} -- the same code and the
 * same wire format as the other three implementations. So the filter chain holds only
 * {@code TransportFilter} and the business filter, and what arrives is one raw datagram.
 *
 * <h2>The read buffer must be enlarged explicitly</h2>
 * See {@code setReadBufferSize} in {@link #start()}. Whatever UDP cannot read into the
 * buffer is dropped, so a buffer smaller than the datagram truncates it silently.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class GrizzlyUdpTransport implements Transport {

    private final GossipConfig config;
    private final String bindHost;
    private final int maxMessageBytes;
    private final Logger log;
    private final DatagramFragmenter fragmenter;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong seqGen = new AtomicLong();

    private volatile MessageHandler handler;
    private volatile int boundPort = -1;

    /** The memory manager used for sending and receiving; heap or direct as configured. */
    private volatile MemoryManager<?> memoryManager = new ByteBufferManager(false);

    private UDPNIOTransport transport;

    /** Listening port -> server-side connection. */
    private final Map<Integer, Connection<?>> listeners = new ConcurrentHashMap<>();

    /** Outbound connections, cached by target address. */
    private final Map<InetSocketAddress, Connection<?>> outbound = new ConcurrentHashMap<>();

    /** Requests awaiting a response, indexed by {@code seq}. */
    private final Map<Long, CompletableFuture<Message>> pending = new ConcurrentHashMap<>();

    private final ExecutorService workers;

    public GrizzlyUdpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        this.maxMessageBytes = config.maxMessageBytes();
        this.log = config.log();
        this.fragmenter = new DatagramFragmenter(
                Frames.MAX_DATAGRAM_BYTES, maxMessageBytes, config.log());
        int n = config.workerThreads();
        // The producer is the network receive thread: a full queue can only discard,
        // never push back -- stalling it would stop gossip heartbeats going out and get
        // this node declared failed
        this.workers = ExecutorUtils.discarding("grizzly-gossip-worker", Math.max(2, n), 4096);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Override
    public void setHandler(MessageHandler handler) {
        this.handler = handler;
    }

    @Override
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            // Heap or direct, decided explicitly rather than inheriting the global
            // DEFAULT_MEMORY_MANAGER.
            //
            // Grizzly's default manager is determined jointly by system properties and the
            // runtime environment -- which is to say, without this line the behaviour would
            // rest on external factors we never declared, could change with the environment,
            // and would give no hint that it had.
            //
            // Heap is the default here: received data ends up as a byte[] handed upward
            // anyway, and the copy direct buffers save is paid straight back at the
            // direct-to-heap step
            memoryManager = new ByteBufferManager(config.directBuffers());
            transport = UDPNIOTransportBuilder.newInstance()
                    .setMemoryManager(memoryManager)
                    .build();
            transport.setProcessor(FilterChainBuilder.stateless()
                    .add(new TransportFilter())
                    .add(new InboundFilter())
                    .build());
            // MUST be false. Port exclusion is the leader election itself, and for UDP,
            // SO_REUSEADDR on BSD and macOS behaves exactly like SO_REUSEPORT: leave it on
            // and two nodes will both "claim" the same cluster port successfully -- two
            // leaders, split brain, and not one error anywhere.
            // This bug was caught in a full Grizzly/UDP regression: two nodes 16ms apart
            // each logged "Claimed the cluster port 127.0.0.1:54164".
            // See the matching comment in NioUdpTransport for the full reasoning
            transport.setReuseAddress(false);
            // Whatever UDP cannot read into the buffer is dropped, so a buffer smaller than
            // the datagram truncates it silently -- surfacing as "small messages are fine,
            // large ones never arrive", which is very hard to track down
            transport.setReadBufferSize(Frames.MAX_DATAGRAM_BYTES);
            transport.setWriteBufferSize(Frames.MAX_DATAGRAM_BYTES);
            // The two lines above govern how many bytes move per read or write; how deep
            // the kernel queues are is a different matter. Grizzly has no direct setter, so
            // a configurator is attached to set the socket options by hand. Of the four
            // implementations, this was once the only place it was missing.
            // See Frames.SOCKET_BUFFER_BYTES
            transport.setChannelConfigurator(socketBufferConfigurator(
                    transport.getChannelConfigurator()));
            transport.start();

            boundPort = WorkPortBinder.bind(config, this::bindListener,
                    GrizzlyUdpTransport::actualPort);
            log.info("Gossip Grizzly UDP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException | RuntimeException e) {
            running.set(false);
            shutdownQuietly();
            throw e;
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        pending.values().forEach(f -> f.completeExceptionally(new IOException("the transport has stopped")));
        pending.clear();
        outbound.values().forEach(Connection::closeSilently);
        outbound.clear();
        shutdownQuietly();
        workers.shutdownNow();
        log.info("Gossip Grizzly UDP transport stopped");
    }

    /**
     * Wraps a configurator to set the kernel queue sizes alongside whatever configuration
     * was already there.
     *
     * <p>It goes in {@code preConfigure} rather than {@code postConfigure}:
     * {@code SO_RCVBUF} has to be set <b>before</b> the bind to take effect on every
     * platform, and some systems ignore it outright afterwards.
     *
     * <p>Failing to set it is not fatal -- at worst it falls back to the system default,
     * which is no reason to stop a node from starting -- so this only logs.
     */
    private ChannelConfigurator socketBufferConfigurator(ChannelConfigurator delegate) {
        return new ChannelConfigurator() {
            @Override
            public void preConfigure(NIOTransport t, SelectableChannel channel) throws IOException {
                delegate.preConfigure(t, channel);
                if (channel instanceof DatagramChannel dc) {
                    try {
                        dc.setOption(StandardSocketOptions.SO_RCVBUF, Frames.SOCKET_BUFFER_BYTES);
                        dc.setOption(StandardSocketOptions.SO_SNDBUF, Frames.SOCKET_BUFFER_BYTES);
                    } catch (IOException | RuntimeException e) {
                        log.debug("Could not set the UDP socket buffers; falling back to the system defaults: {}",
                                e.toString());
                    }
                }
            }

            @Override
            public void postConfigure(NIOTransport t, SelectableChannel channel) throws IOException {
                delegate.postConfigure(t, channel);
            }
        };
    }

    private void shutdownQuietly() {
        listeners.clear();
        if (transport != null) {
            try {
                transport.shutdownNow();
            } catch (IOException e) {
                log.debug("Failed to close the Grizzly transport: {}", e.toString());
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int boundPort() {
        return boundPort;
    }

    @Override
    public long nextSeq() {
        return seqGen.incrementAndGet();
    }

    // ------------------------------------------------------------------
    // Claiming ports
    // ------------------------------------------------------------------

    @Override
    public InetSocketAddress open(int port) {
        if (!running.get()) {
            return null;
        }
        Connection<?> existing = listeners.get(port);
        if (existing != null) {
            return (InetSocketAddress) existing.getLocalAddress();
        }
        try {
            Connection<?> c = bindListener(port);
            log.info("Claimed the cluster port {}:{}", bindHost, port);
            return (InetSocketAddress) c.getLocalAddress();
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
        Connection<?> c = listeners.remove(port);
        if (c == null) {
            return;
        }
        transport.unbind(c);
        log.info("Released the cluster port {}", port);
    }

    @Override
    public boolean holds(int port) {
        // Check the connection's real state, not merely whether the map holds the key --
        // with a dead socket and a stale map entry this node would go on believing it is
        // the leader while the port had long since been freed and taken by someone else.
        // See the matching comment in NioUdpTransport
        Connection<?> c = listeners.get(port);
        if (c == null) {
            return false;
        }
        if (!alive(c)) {
            listeners.remove(port, c);
            log.warn("The socket for cluster port {} has failed; this node no longer holds it", port);
            return false;
        }
        return true;
    }

    /**
     * Whether this listening connection is still alive.
     *
     * <p>{@link Connection#isOpen()} cannot be used: in Grizzly 2.3 it is unreliable for
     * <b>UDP server-side connections</b>. It was observed <b>still returning true</b> after
     * {@code close().get()} had returned, with {@code getCloseReason()} still null. Relying
     * on it would have a node go on claiming to hold the cluster port long after the socket
     * was gone.
     *
     * <p>The underlying {@code SelectableChannel} is accurate, so it is asked directly.
     */
    private static boolean alive(Connection<?> c) {
        if (c instanceof NIOConnection nio) {
            SelectableChannel ch = nio.getChannel();
            return ch != null && ch.isOpen();
        }
        return c.isOpen();
    }

    @Override
    public boolean checkAndRepairWorkPort() {
        if (!running.get()) {
            return false;
        }
        Connection<?> c = listeners.get(boundPort);
        if (c != null && alive(c)) {
            return true;
        }
        // Rebind the same port; invisible from outside. See Transport#checkAndRepairWorkPort
        //
        // Clearing the map is not enough: the broken connection is still attached to the
        // transport and its selector key is still there, so the port is never really
        // released and the rebind is certain to hit "Address already in use".
        // It has to go through the same unbind as close(port)
        Connection<?> dead = listeners.remove(boundPort);
        if (dead != null) {
            try {
                transport.unbind(dead);
            } catch (RuntimeException e) {
                log.debug("Error while unbinding the dead work-port connection; ignored: {}", e.toString());
            }
        }
        // The port is not necessarily usable the instant unbind returns, so retry briefly;
        // and failing that, no harm done -- the check is periodic and comes round again
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

    private Connection<?> bindListener(int port) throws IOException {
        Connection<?> c = transport.bind(bindHost, port);
        // Port 0 is assigned by the system, so read back the actual port; the argument
        // must not be used as the key
        listeners.put(actualPort(c), c);
        return c;
    }

    private static int actualPort(Connection<?> c) {
        return ((InetSocketAddress) c.getLocalAddress()).getPort();
    }

    // ------------------------------------------------------------------
    // Outbound
    // ------------------------------------------------------------------

    @Override
    public Message request(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.put(request.seq(), future);
        try {
            emit(outboundConnection(addr), request);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out waiting for a response: " + addr);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for a response", e);
        } catch (ExecutionException e) {
            throw new IOException("request failed: " + addr, e.getCause());
        } finally {
            pending.remove(request.seq());
        }
    }

    /**
     * A one-shot request that <b>never enters the connection cache</b>.
     *
     * <p>Range scanning takes this path, and it meets a great many addresses that do not
     * exist. Cached by address, one sweep would leave hundreds of connections behind, and
     * once sockets ran out every JOIN would time out.
     *
     * <p>{@code requestDirect}'s difference from {@code request} was never "does it wait
     * for an answer",
     * but <b>"is this address worth remembering"</b>.
     */
    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.put(request.seq(), future);
        Connection<?> connection = null;
        try {
            connection = connectOnce(addr);
            emit(connection, request);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out waiting for a response: " + addr);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for a response", e);
        } catch (ExecutionException e) {
            throw new IOException("request failed: " + addr, e.getCause());
        } finally {
            pending.remove(request.seq());
            if (connection != null) {
                connection.closeSilently();
            }
        }
    }

    /** Builds a throwaway connection that never enters the {@link #outbound} cache. */
    private Connection<?> connectOnce(InetSocketAddress addr) throws IOException {
        try {
            return transport.connect(addr)
                    .get(config.connectTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while opening an outbound connection: " + addr, e);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out opening an outbound connection: " + addr);
        } catch (ExecutionException e) {
            throw new IOException("could not open an outbound connection: " + addr, e.getCause());
        }
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        emit(outboundConnection(addr), message);
    }

    /** Gets, or builds, an outbound connection to a target address. */
    private Connection<?> outboundConnection(InetSocketAddress addr) throws IOException {
        Connection<?> cached = outbound.get(addr);
        if (cached != null && cached.isOpen()) {
            return cached;
        }
        try {
            Connection<?> c = transport.connect(addr)
                    .get(config.connectTimeoutMs(), TimeUnit.MILLISECONDS);
            Connection<?> previous = outbound.put(addr, c);
            if (previous != null && previous != c) {
                previous.closeSilently();
            }
            return c;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while opening an outbound connection: " + addr, e);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out opening an outbound connection: " + addr);
        } catch (ExecutionException e) {
            throw new IOException("could not open an outbound connection: " + addr, e.getCause());
        }
    }

    /** Encodes, fragments if needed, and writes out. */
    private void emit(Connection<?> c, Message msg) throws IOException {
        for (ByteBuffer buf : fragmenter.split(Frames.encode(msg))) {
            c.write(Buffers.wrap(memoryManager, buf));
        }
    }

    /** Sends a response back. A server-side connection is unconnected, so the target
     *  address has to be given explicitly. */
    private void emitTo(Connection<Object> c, InetSocketAddress to, Message msg) throws IOException {
        for (ByteBuffer buf : fragmenter.split(Frames.encode(msg))) {
            c.write(to, Buffers.wrap(memoryManager, buf), null);
        }
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /**
     * Shared by listening connections and outbound connections.
     *
     * <p>Telling a request from a response comes down to whether the {@code seq} is in the
     * waiting table, regardless of which connection it arrived on.
     */
    private final class InboundFilter extends BaseFilter {

        @Override
        public NextAction handleRead(FilterChainContext ctx) {
            Buffer buf = ctx.getMessage();
            if (buf == null) {
                return ctx.getStopAction();
            }
            InetSocketAddress from = (InetSocketAddress) ctx.getAddress();
            ByteBuffer datagram = buf.toByteBuffer();

            ByteBuffer frame = fragmenter.reassemble(datagram, from);
            if (frame == null) {
                // Not all fragments have arrived yet
                return ctx.getStopAction();
            }
            Message msg;
            try {
                msg = Frames.decode(frame, maxMessageBytes);
            } catch (IOException e) {
                // Stray traffic from a port scanner and the like; simply ignore it
                log.debug("Discarding a malformed message from {}: {}", from, e.toString());
                return ctx.getStopAction();
            }

            // A message must be confirmed to be a response before it may claim an in-flight
            // request: a seq is unique only on the sender's side, and messages the peer sends
            // on its own initiative draw from a range that overlaps this node's.
            // See MessageType#isResponse()
            if (msg.type().isResponse()) {
                CompletableFuture<Message> waiting = pending.remove(msg.seq());
                if (waiting != null) {
                    waiting.complete(msg);
                } else {
                    log.debug("Discarding a late response seq={} from {}", msg.seq(), from);
                }
                return ctx.getStopAction();
            }
            dispatch(ctx.getConnection(), msg, from);
            return ctx.getStopAction();
        }
    }

    /** Processing moves to the worker pool; the response goes back out on the very connection it arrived on. */
    @SuppressWarnings("unchecked")
    private void dispatch(Connection<?> c, Message msg, InetSocketAddress remote) {
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
                emitTo((Connection<Object>) c, remote, response);
            } catch (IOException e) {
                log.warn("Failed to send a response back to {}: {}", remote, e.toString());
            }
        });
    }
}
