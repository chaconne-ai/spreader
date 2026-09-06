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
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.slf4j.Logger;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Deque;
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
 * The TCP transport built on Netty.
 *
 * <p><b>Protocol-identical to the built-in NIO implementation</b>: same frame format,
 * same port-claiming rules, same request/response semantics, so nodes running either can
 * share one cluster. The only difference is who moves the bytes.
 *
 * <h2>Stream framing is Netty's own</h2>
 * {@code LengthFieldBasedFrameDecoder} exists for exactly this and there is no point
 * rewriting it. See {@link NettyFrames}.
 *
 * <h2>One request at a time per connection</h2>
 * The server hands processing to a worker pool, so <b>responses do not necessarily come
 * back in request order</b>. Rather than multiplex request ids over one connection, each
 * connection carries a single in-flight request -- the same semantics the built-in
 * implementation has, which is what keeps the two behaving alike. Concurrency comes from
 * having several connections in the pool.
 *
 * <p>The {@code seq} is still checked when a response arrives: a mismatch means the
 * previous request's response was left on this connection, and the connection is
 * discarded rather than the wrong result handed upward.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class NettyTcpTransport implements Transport {

    /** The response awaited on a connection. There is only ever one; see the class documentation. */
    private static final AttributeKey<CompletableFuture<Message>> PENDING =
            AttributeKey.valueOf("spreader.pending");

    /** Idle-connection reclaim time, kept the same as the built-in implementation. */
    private static final long IDLE_TIMEOUT_MS = 30_000L;

    private final GossipConfig config;
    private final String bindHost;
    private final int maxMessageBytes;
    private final int connectTimeoutMs;
    private final Logger log;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong seqGen = new AtomicLong();

    private volatile MessageHandler handler;
    private volatile int boundPort = -1;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventLoopGroup clientGroup;
    private ServerBootstrap serverBootstrap;
    private Bootstrap clientBootstrap;

    /** Listening ports currently bound: the work port plus any claimed cluster port. */
    private final Map<Integer, Channel> listeners = new ConcurrentHashMap<>();

    /** The outbound connection pool, grouped by target address. */
    private final Map<InetSocketAddress, Deque<Pooled>> pool = new ConcurrentHashMap<>();

    /** The worker pool for inbound requests; it must not tie up Netty's IO threads. */
    private final ExecutorService workers;

    public NettyTcpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        this.maxMessageBytes = config.maxMessageBytes();
        this.connectTimeoutMs = config.connectTimeoutMs();
        this.log = config.log();
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
            bossGroup = new NioEventLoopGroup(1, new NamedThreadFactory("netty-gossip-boss", true));
            workerGroup = new NioEventLoopGroup(
                    Math.max(2, config.workerThreads() / 2),
                    new NamedThreadFactory("netty-gossip-io", true));
            clientGroup = new NioEventLoopGroup(
                    Math.max(2, config.workerThreads() / 2),
                    new NamedThreadFactory("netty-gossip-client", true));

            serverBootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // Do not enable SO_REUSEPORT: port exclusion is the leader election
                    // itself. See the documentation on Transport
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(NettyFrames.frameSplitter(maxMessageBytes))
                                    .addLast(NettyFrames.decoder(maxMessageBytes))
                                    .addLast(new NettyFrames.Encoder())
                                    .addLast(new ServerHandler());
                        }
                    });

            clientBootstrap = new Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(NettyFrames.frameSplitter(maxMessageBytes))
                                    .addLast(NettyFrames.decoder(maxMessageBytes))
                                    .addLast(new NettyFrames.Encoder())
                                    .addLast(new ClientHandler());
                        }
                    });

            boundPort = WorkPortBinder.bind(config, this::bindListener, NettyTcpTransport::actualPort);
            log.info("Gossip Netty TCP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException | RuntimeException e) {
            running.set(false);
            shutdownGroups();
            throw e;
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        listeners.values().forEach(Channel::close);
        listeners.clear();
        pool.values().forEach(q -> {
            Pooled p;
            while ((p = q.poll()) != null) {
                p.channel.close();
            }
        });
        pool.clear();
        shutdownGroups();
        workers.shutdownNow();
        log.info("Gossip Netty TCP transport stopped");
    }

    private void shutdownGroups() {
        // quietPeriod 0: this is the shutdown path, and there is no reason to spend two
        // more seconds being graceful
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        if (clientGroup != null) {
            clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
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

    private static int actualPort(Channel ch) {
        return ((InetSocketAddress) ch.localAddress()).getPort();
    }

    // ------------------------------------------------------------------
    // Outbound
    // ------------------------------------------------------------------

    @Override
    public Message request(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        if (!config.connectionPoolEnabled()) {
            return requestDirect(addr, request, timeoutMs);
        }
        Channel pooled = borrow(addr);
        if (pooled != null) {
            try {
                Message response = exchange(pooled, request, timeoutMs);
                release(addr, pooled);
                return response;
            } catch (IOException e) {
                pooled.close();
                log.debug("A pooled connection failed; retrying on a fresh one: {} ({})", addr, e.toString());
            }
        }
        Channel fresh = connect(addr);
        try {
            Message response = exchange(fresh, request, timeoutMs);
            release(addr, fresh);
            return response;
        } catch (IOException e) {
            fresh.close();
            throw e;
        }
    }

    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        Channel ch = connect(addr);
        try {
            return exchange(ch, request, timeoutMs);
        } finally {
            ch.close();
        }
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        Channel ch = config.connectionPoolEnabled() ? borrow(addr) : null;
        boolean pooled = ch != null;
        if (ch == null) {
            ch = connect(addr);
        }
        try {
            writeAndFlush(ch, message, timeoutMs);
            if (config.connectionPoolEnabled()) {
                release(addr, ch);
            } else {
                ch.close();
            }
        } catch (IOException e) {
            ch.close();
            if (!pooled) {
                throw e;
            }
            // A pooled connection may have been closed by the peer; retry once on a fresh
            // one -- the same handling as the built-in implementation
            Channel fresh = connect(addr);
            try {
                writeAndFlush(fresh, message, timeoutMs);
                release(addr, fresh);
            } catch (IOException retry) {
                fresh.close();
                throw retry;
            }
        }
    }

    /** Writes a message and waits for it to be flushed for real. */
    private void writeAndFlush(Channel ch, Message msg, int timeoutMs) throws IOException {
        ChannelFuture f = ch.writeAndFlush(msg);
        if (!f.awaitUninterruptibly(timeoutMs) || !f.isSuccess()) {
            throw new IOException("send failed: " + ch.remoteAddress(), f.cause());
        }
    }

    /** Sends a request and waits for its response. */
    private Message exchange(Channel ch, Message request, int timeoutMs)
            throws IOException {
        CompletableFuture<Message> future = new CompletableFuture<>();
        ch.attr(PENDING).set(future);
        try {
            writeAndFlush(ch, request, timeoutMs);
            Message response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (response.seq() != request.seq()) {
                // The previous response was left on the connection, so this answer is wrong.
                // Better to fail loudly than hand it upward
                throw new IOException("response sequence mismatch: expected " + request.seq()
                        + ", got " + response.seq());
            }
            return response;
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out waiting for a response: " + ch.remoteAddress());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for a response", e);
        } catch (ExecutionException e) {
            throw new IOException("connection error: " + ch.remoteAddress(), e.getCause());
        } finally {
            ch.attr(PENDING).set(null);
        }
    }

    private Channel connect(InetSocketAddress addr) throws IOException {
        ChannelFuture f = clientBootstrap.connect(addr);
        if (!f.awaitUninterruptibly(connectTimeoutMs) || !f.isSuccess()) {
            f.cancel(true);
            throw new IOException("could not connect: " + addr, f.cause());
        }
        return f.channel();
    }

    // ------------------------------------------------------------------
    // Connection pool
    // ------------------------------------------------------------------

    private Channel borrow(InetSocketAddress addr) {
        Deque<Pooled> q = pool.get(addr);
        if (q == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        Pooled p;
        synchronized (q) {
            while ((p = q.pollLast()) != null) {
                if (!p.channel.isActive() || now - p.idleSinceMs > IDLE_TIMEOUT_MS) {
                    p.channel.close();
                    continue;
                }
                return p.channel;
            }
        }
        return null;
    }

    private void release(InetSocketAddress addr, Channel ch) {
        if (!config.connectionPoolEnabled() || !ch.isActive()) {
            ch.close();
            return;
        }
        Deque<Pooled> q = pool.computeIfAbsent(addr, k -> new ArrayDeque<>());
        synchronized (q) {
            if (q.size() >= config.poolMaxIdlePerHost()) {
                ch.close();
                return;
            }
            q.addLast(new Pooled(ch, System.currentTimeMillis()));
        }
    }

    private record Pooled(Channel channel, long idleSinceMs) {
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /** Server side: parse the message, hand it to the worker pool, and write back any response. */
    private final class ServerHandler extends SimpleChannelInboundHandler<Message> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            MessageHandler h = handler;
            if (h == null) {
                return;
            }
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            workers.execute(() -> {
                Message response;
                try {
                    response = h.handle(msg, remote);
                } catch (Throwable t) {
                    log.error("Failed to handle an inbound message: " + msg, t);
                    return;
                }
                if (response != null && ctx.channel().isActive()) {
                    ctx.writeAndFlush(response);
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Stray traffic from port scanners and the like lands here; not worth an error
            log.debug("Inbound connection {} threw: {}", ctx.channel().remoteAddress(), cause.toString());
            ctx.close();
        }
    }

    /** Client side: hand the response to whichever request is waiting for it. */
    private final class ClientHandler extends SimpleChannelInboundHandler<Message> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            CompletableFuture<Message> f = ctx.channel().attr(PENDING).getAndSet(null);
            if (f != null) {
                f.complete(msg);
            } else {
                // Nobody is waiting for it: almost certainly a response that arrived after
                // its request had already timed out
                log.debug("Discarding a response nobody is waiting for, seq={}", msg.seq());
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            CompletableFuture<Message> f = ctx.channel().attr(PENDING).getAndSet(null);
            if (f != null) {
                f.completeExceptionally(new IOException("the peer closed the connection"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            CompletableFuture<Message> f = ctx.channel().attr(PENDING).getAndSet(null);
            if (f != null) {
                f.completeExceptionally(cause);
            }
            ctx.close();
        }
    }
}
