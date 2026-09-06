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
package com.chaconneai.spreader.transport.netty;

import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.transport.DatagramFragmenter;
import com.chaconneai.spreader.transport.Frames;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.slf4j.Logger;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
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
 * The UDP transport built on Netty.
 *
 * <p>Protocol-identical to the built-in implementation, so nodes running either can share
 * one cluster.
 *
 * <h2>Fragmentation stays ours</h2>
 * Netty ships a decoder for TCP's stream framing, but for UDP's "what happens when a
 * message exceeds 64KB" it has <b>nothing</b> -- that is an application protocol decision
 * and no framework will make it for you. So {@link DatagramFragmenter} is reused
 * directly: the same code and the same wire format as the built-in implementation.
 *
 * <h2>One client socket, reused for everything outbound</h2>
 * The built-in implementation opens a temporary socket per request; this keeps a single
 * one and matches responses back by {@code seq}. What that saves is a socket creation and
 * destruction per request -- and with UDP probing as frequently as it does, that adds up.
 *
 * <p>The price is that every in-flight request shares one waiting table, and there is a
 * trap here that was walked into once. A {@code seq} comes from {@link #nextSeq()}, which
 * only guarantees uniqueness <b>among requests this node sends</b>; it is not globally
 * unique, and a message the peer sends on its own initiative carries the peer's
 * {@code seq}, drawn from a completely overlapping range. So
 * {@link com.chaconneai.spreader.protocol.MessageType#isResponse()} must be checked
 * before claiming an in-flight request, or the peer's requests get consumed as this
 * node's responses.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class NettyUdpTransport implements Transport {

    private final GossipConfig config;
    private final String bindHost;
    private final int maxMessageBytes;
    private final Logger log;
    private final DatagramFragmenter fragmenter;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong seqGen = new AtomicLong();

    private volatile MessageHandler handler;
    private volatile int boundPort = -1;

    private EventLoopGroup group;
    private Bootstrap serverBootstrap;

    /** Listening ports: the work port plus any claimed cluster port. */
    private final Map<Integer, Channel> listeners = new ConcurrentHashMap<>();

    /** Outbound only. It never listens for business requests, so everything it receives is a response. */
    private volatile Channel client;

    /** Requests awaiting a response, indexed by {@code seq}. */
    private final Map<Long, CompletableFuture<Message>> pending = new ConcurrentHashMap<>();

    /** The worker pool for inbound requests; it must not tie up Netty's IO threads. */
    private final ExecutorService workers;

    public NettyUdpTransport(GossipConfig config) {
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
        this.workers = ExecutorUtils.discarding("netty-gossip-worker", Math.max(2, n), 4096);
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
            group = new NioEventLoopGroup(
                    Math.max(2, config.workerThreads() / 2),
                    new NamedThreadFactory("netty-gossip-udp", true));

            serverBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    // MUST be false. Port exclusion is the leader election itself, and for
                    // UDP, SO_REUSEADDR on BSD and macOS behaves exactly like SO_REUSEPORT:
                    // leave it on and two nodes will both "claim" the same cluster port
                    // successfully -- two leaders, split brain, and not one error anywhere.
                    // See the matching comment in NioUdpTransport for the full reasoning
                    .option(ChannelOption.SO_REUSEADDR, false)
                    // Kernel send and receive queues. Do not estimate this from a datagram
                    // count on intuition; see Frames.SOCKET_BUFFER_BYTES for how it was chosen
                    .option(ChannelOption.SO_RCVBUF, Frames.SOCKET_BUFFER_BYTES)
                    .option(ChannelOption.SO_SNDBUF, Frames.SOCKET_BUFFER_BYTES)
                    // This line is mandatory. Netty's default UDP receive buffer is 2048
                    // bytes, and whatever UDP cannot read into it IS SIMPLY DROPPED -- so any
                    // datagram over 2KB is silently truncated, surfacing as "small messages
                    // are fine, large ones never arrive". Fragmentation itself was never the
                    // problem; every fragment was being cut short. SO_RCVBUF governs the
                    // kernel queue and does nothing about this
                    .option(ChannelOption.RCVBUF_ALLOCATOR,
                            new FixedRecvByteBufAllocator(Frames.MAX_DATAGRAM_BYTES))
                    // Heap or direct, stated EXPLICITLY.
                    //
                    // Without this line it would be ByteBufAllocator.DEFAULT, which prefers
                    // direct and is further influenced by -Dio.netty.noPreferDirect. That is
                    // to say, the behaviour would be decided by an external switch we never
                    // declared, changing with a Netty version or an added JVM flag, and
                    // giving no hint that it had.
                    //
                    // Heap is the default here: received data ends up as a byte[] handed
                    // upward anyway, and the copy direct buffers save is paid straight back
                    // at the direct-to-heap step
                    .option(ChannelOption.ALLOCATOR, config.directBuffers()
                            ? PooledByteBufAllocator.DEFAULT
                            : new PooledByteBufAllocator(false))
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(new InboundHandler());
                        }
                    });

            boundPort = WorkPortBinder.bind(config, this::bindListener, NettyUdpTransport::actualPort);
            client = openClient();
            log.info("Gossip Netty UDP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException | RuntimeException e) {
            running.set(false);
            stopQuietly();
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
        stopQuietly();
        workers.shutdownNow();
        log.info("Gossip Netty UDP transport stopped");
    }

    private void stopQuietly() {
        listeners.values().forEach(Channel::close);
        listeners.clear();
        Channel c = client;
        if (c != null) {
            c.close();
            client = null;
        }
        if (group != null) {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
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
        Channel existing = listeners.get(port);
        if (existing != null) {
            return (InetSocketAddress) existing.localAddress();
        }
        try {
            Channel ch = bindListener(port);
            log.info("Claimed the cluster port {}:{}", bindHost, port);
            return (InetSocketAddress) ch.localAddress();
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
        Channel ch = listeners.remove(port);
        if (ch == null) {
            return;
        }
        ch.close().awaitUninterruptibly(1_000L);
        log.info("Released the cluster port {}", port);
    }

    @Override
    public boolean holds(int port) {
        // Check the channel's real state, not merely whether the map holds the key --
        // with a dead socket and a stale map entry this node would go on believing it is
        // the leader while the port had long since been freed and taken by someone else.
        // See the matching comment in NioUdpTransport
        Channel ch = listeners.get(port);
        if (ch == null) {
            return false;
        }
        if (!ch.isActive()) {
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
        Channel ch = listeners.get(boundPort);
        if (ch != null && ch.isActive()) {
            return true;
        }
        // Rebind the same port; invisible from outside. See Transport#checkAndRepairWorkPort
        listeners.remove(boundPort);
        try {
            bindListener(boundPort);
            log.warn("The socket for work port {} had failed and has been rebound in place", boundPort);
            return true;
        } catch (IOException e) {
            log.error("Work port " + boundPort + " has failed and cannot be rebound; this node "
                    + "now receives nothing at all", e);
            return false;
        }
    }

    private Channel bindListener(int port) throws IOException {
        ChannelFuture f = serverBootstrap.bind(new InetSocketAddress(bindHost, port));
        f.awaitUninterruptibly();
        if (!f.isSuccess()) {
            throw new IOException("could not bind port " + port, f.cause());
        }
        Channel ch = f.channel();
        // Port 0 is assigned by the system, so read back the actual port; the argument
        // must not be used as the key
        listeners.put(actualPort(ch), ch);
        return ch;
    }

    /** The outbound socket; its port is assigned by the system. */
    private Channel openClient() throws IOException {
        ChannelFuture f = serverBootstrap.bind(new InetSocketAddress(bindHost, 0));
        f.awaitUninterruptibly();
        if (!f.isSuccess()) {
            throw new IOException("could not bind the outbound socket", f.cause());
        }
        return f.channel();
    }

    private static int actualPort(Channel ch) {
        return ((InetSocketAddress) ch.localAddress()).getPort();
    }

    // ------------------------------------------------------------------
    // Outbound
    // ------------------------------------------------------------------

    @Override
    public Message request(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        Channel c = client;
        if (c == null || !c.isActive()) {
            throw new IOException("the transport is not ready");
        }
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.put(request.seq(), future);
        try {
            emit(c, addr, request);
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

    /** UDP has no connection to reuse, so this is identical to {@link #request}. */
    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        return request(addr, request, timeoutMs);
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        Channel c = client;
        if (c == null || !c.isActive()) {
            throw new IOException("the transport is not ready");
        }
        emit(c, addr, message);
    }

    /** Encodes, fragments if needed, and sends from the given channel. */
    private void emit(Channel ch, InetSocketAddress addr, Message msg) throws IOException {
        List<ByteBuffer> frames = fragmenter.split(Frames.encode(msg));
        for (ByteBuffer buf : frames) {
            ch.write(new DatagramPacket(Unpooled.wrappedBuffer(buf), addr));
        }
        ch.flush();
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /**
     * Shared by the listening ports and the outbound socket.
     *
     * <p>Telling a request from a response comes down to one question: a {@code seq}
     * present in the waiting table is a response, and anything else is a request. That is
     * sturdier than deciding by channel -- the outbound socket could in principle receive
     * a request from someone too.
     */
    private final class InboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
            InetSocketAddress from = packet.sender();
            ByteBuffer datagram = packet.content().nioBuffer();

            ByteBuffer frame = fragmenter.reassemble(datagram, from);
            if (frame == null) {
                // Not all fragments have arrived yet
                return;
            }
            Message msg;
            try {
                msg = Frames.decode(frame, maxMessageBytes);
            } catch (IOException e) {
                // Stray traffic from a port scanner and the like; simply ignore it
                log.debug("Discarding a malformed message from {}: {}", from, e.toString());
                return;
            }

            if (msg.type().isResponse()) {
                CompletableFuture<Message> waiting = pending.remove(msg.seq());
                if (waiting != null) {
                    waiting.complete(msg);
                } else {
                    // A late response: the requester timed out and left long ago. Discard
                    // it -- handing it upward would only make it deal with an ACK that has
                    // no business being on the inbound path at all
                    log.debug("Discarding a late response seq={} from {}", msg.seq(), from);
                }
                return;
            }
            dispatch(ctx.channel(), msg, from);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("UDP channel threw: {}", cause.toString());
        }
    }

    /** Processing moves to the worker pool; the response goes back out on the very channel it arrived on. */
    private void dispatch(Channel ch, Message msg, InetSocketAddress remote) {
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
                emit(ch, remote, response);
            } catch (IOException e) {
                log.warn("Failed to send a response back to {}: {}", remote, e.toString());
            }
        });
    }
}
