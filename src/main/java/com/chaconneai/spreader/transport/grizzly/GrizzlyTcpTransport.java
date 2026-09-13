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
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.attributes.Attribute;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.NIOConnection;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.SelectableChannel;
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
 * The TCP transport built on Grizzly.
 *
 * <p>The protocol is identical to the other three implementations, so nodes running any
 * of them can share one cluster.
 *
 * <h2>Separate transports for the server and client sides</h2>
 * A Grizzly filter chain hangs off a transport, and the server side (take a request,
 * send a response) and the client side (send a request, await a response) do different
 * things. Rather than have one filter work out whether a connection was dialled out or
 * accepted in, there are two transports, each with its own chain.
 *
 * <h2>One request at a time per connection</h2>
 * As with the other three implementations: the server hands processing to a worker pool,
 * so responses do not necessarily come back in request order.
 * So each connection carries one in-flight request at a time, and concurrency comes from
 * having several connections in the pool.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class GrizzlyTcpTransport implements Transport {

    /** The response awaited on a connection. */
    private static final Attribute<CompletableFuture<Message>> PENDING =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute("spreader.pending");

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

    private TCPNIOTransport server;
    private TCPNIOTransport client;

    /** Listening port -> server-side connection. */
    private final Map<Integer, Connection<?>> listeners = new ConcurrentHashMap<>();

    /** The outbound connection pool, grouped by target address. */
    private final Map<InetSocketAddress, Deque<Pooled>> pool = new ConcurrentHashMap<>();

    private final ExecutorService workers;

    public GrizzlyTcpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        this.maxMessageBytes = config.maxMessageBytes();
        this.connectTimeoutMs = config.connectTimeoutMs();
        this.log = config.log();
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
            server = TCPNIOTransportBuilder.newInstance().build();
            server.setProcessor(FilterChainBuilder.stateless()
                    .add(new TransportFilter())
                    .add(GrizzlyFrames.tcpDecoder(maxMessageBytes))
                    .add(new ServerFilter())
                    .build());
            server.setReuseAddress(true);
            server.setTcpNoDelay(true);
            server.start();

            client = TCPNIOTransportBuilder.newInstance().build();
            client.setProcessor(FilterChainBuilder.stateless()
                    .add(new TransportFilter())
                    .add(GrizzlyFrames.tcpDecoder(maxMessageBytes))
                    .add(new ClientFilter())
                    .build());
            client.setTcpNoDelay(true);
            client.setConnectionTimeout(connectTimeoutMs);
            client.start();

            boundPort = WorkPortBinder.bind(config, this::bindListener,
                    GrizzlyTcpTransport::actualPort);
            log.info("Gossip Grizzly TCP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException | RuntimeException e) {
            running.set(false);
            shutdownQuietly();
            throw e;
        }
    }

    @Override
    public void stop() {
        synchronized (lifecycle) {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            pool.values().forEach(q -> {
                Pooled p;
                while ((p = q.poll()) != null) {
                    p.connection.closeSilently();
                }
            });
            pool.clear();
            shutdownQuietly();
            workers.shutdownNow();
            log.info("Gossip Grizzly TCP transport stopped");
        }
    }

    private void shutdownQuietly() {
        listeners.clear();
        for (TCPNIOTransport t : new TCPNIOTransport[]{server, client}) {
            if (t == null) {
                continue;
            }
            try {
                t.shutdownNow();
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


    /**
     * Serialises claiming a port against shutting the transport down.
     *
     * <p>Without it the two interleave: a takeover binds the cluster port at the moment
     * stop() is draining the listener map, and the socket ends up in nobody's hands. It
     * cannot be cleaned up afterwards either, since closing a channel needs the very event
     * loop that stop() has just shut down. So the two are kept apart instead: a bind either
     * completes before the shutdown, and the shutdown closes it, or it finds the transport
     * already stopped and never binds at all.
     *
     * <p>The cost is a lock on a path taken once per leadership change, which is nothing.
     */
    private final Object lifecycle = new Object();

    @Override
    public InetSocketAddress open(int port) {
        synchronized (lifecycle) {
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
        server.unbind(c);
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
     * <p>It asks the underlying {@code SelectableChannel} directly rather than using
     * {@link Connection#isOpen()}: in Grizzly 2.3 that flag is not always accurate for
     * server-side connections -- on the UDP side it was observed still reporting true
     * after {@code close().get()}. Both protocols use the same test, so the trap is not
     * walked into twice.
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
        // transport, the port is never really released, and the rebind is certain to hit
        // "Address already in use". It has to go through the same unbind as close(port)
        Connection<?> dead = listeners.remove(boundPort);
        if (dead != null) {
            try {
                server.unbind(dead);
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
        Connection<?> c = server.bind(bindHost, port);
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
        if (!config.connectionPoolEnabled()) {
            return requestDirect(addr, request, timeoutMs);
        }
        Connection<?> pooled = borrow(addr);
        if (pooled != null) {
            try {
                Message response = exchange(pooled, request, timeoutMs);
                release(addr, pooled);
                return response;
            } catch (IOException e) {
                pooled.closeSilently();
                log.debug("A pooled connection failed; retrying on a fresh one: {} ({})", addr, e.toString());
            }
        }
        Connection<?> fresh = connect(addr);
        try {
            Message response = exchange(fresh, request, timeoutMs);
            release(addr, fresh);
            return response;
        } catch (IOException e) {
            fresh.closeSilently();
            throw e;
        }
    }

    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        Connection<?> c = connect(addr);
        try {
            return exchange(c, request, timeoutMs);
        } finally {
            c.closeSilently();
        }
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        Connection<?> c = config.connectionPoolEnabled() ? borrow(addr) : null;
        boolean pooled = c != null;
        if (c == null) {
            c = connect(addr);
        }
        try {
            write(c, message, timeoutMs);
            if (config.connectionPoolEnabled()) {
                release(addr, c);
            } else {
                c.closeSilently();
            }
        } catch (IOException e) {
            c.closeSilently();
            if (!pooled) {
                throw e;
            }
            // A pooled connection may have been closed by the peer; retry once on a fresh one
            Connection<?> fresh = connect(addr);
            try {
                write(fresh, message, timeoutMs);
                release(addr, fresh);
            } catch (IOException retry) {
                fresh.closeSilently();
                throw retry;
            }
        }
    }

    private void write(Connection<?> c, Message msg, int timeoutMs) throws IOException {
        try {
            c.write(GrizzlyFrames.encode(msg)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while sending", e);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("send timed out: " + c.getPeerAddress());
        } catch (ExecutionException e) {
            throw new IOException("send failed: " + c.getPeerAddress(), e.getCause());
        }
    }

    private Message exchange(Connection<?> c, Message request, int timeoutMs) throws IOException {
        CompletableFuture<Message> future = new CompletableFuture<>();
        PENDING.set(c, future);
        try {
            write(c, request, timeoutMs);
            Message response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (response.seq() != request.seq()) {
                // The previous response was left on the connection, so this answer is wrong.
                // Better to fail loudly than hand it upward
                throw new IOException("response sequence mismatch: expected " + request.seq()
                        + ", got " + response.seq());
            }
            return response;
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out waiting for a response: " + c.getPeerAddress());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for a response", e);
        } catch (ExecutionException e) {
            throw new IOException("connection error: " + c.getPeerAddress(), e.getCause());
        } finally {
            PENDING.remove(c);
        }
    }

    private Connection<?> connect(InetSocketAddress addr) throws IOException {
        try {
            return client.connect(addr).get(connectTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while connecting: " + addr, e);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("connect timed out: " + addr);
        } catch (ExecutionException e) {
            throw new IOException("could not connect: " + addr, e.getCause());
        }
    }

    // ------------------------------------------------------------------
    // Connection pool
    // ------------------------------------------------------------------

    private Connection<?> borrow(InetSocketAddress addr) {
        Deque<Pooled> q = pool.get(addr);
        if (q == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        Pooled p;
        synchronized (q) {
            while ((p = q.pollLast()) != null) {
                if (!p.connection.isOpen() || now - p.idleSinceMs > IDLE_TIMEOUT_MS) {
                    p.connection.closeSilently();
                    continue;
                }
                return p.connection;
            }
        }
        return null;
    }

    private void release(InetSocketAddress addr, Connection<?> c) {
        if (!config.connectionPoolEnabled() || !c.isOpen()) {
            c.closeSilently();
            return;
        }
        Deque<Pooled> q = pool.computeIfAbsent(addr, k -> new ArrayDeque<>());
        synchronized (q) {
            if (q.size() >= config.poolMaxIdlePerHost()) {
                c.closeSilently();
                return;
            }
            q.addLast(new Pooled(c, System.currentTimeMillis()));
        }
    }

    private record Pooled(Connection<?> connection, long idleSinceMs) {
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /** Server side: parse the message, hand it to the worker pool, and write back any response. */
    private final class ServerFilter extends BaseFilter {

        @Override
        public NextAction handleRead(FilterChainContext ctx) {
            Message msg = ctx.getMessage();
            MessageHandler h = handler;
            if (msg == null || h == null) {
                return ctx.getStopAction();
            }
            Connection<?> c = ctx.getConnection();
            InetSocketAddress remote = (InetSocketAddress) c.getPeerAddress();
            workers.execute(() -> {
                Message response;
                try {
                    response = h.handle(msg, remote);
                } catch (Throwable t) {
                    log.error("Failed to handle an inbound message: " + msg, t);
                    return;
                }
                if (response == null || !c.isOpen()) {
                    return;
                }
                try {
                    c.write(GrizzlyFrames.encode(response));
                } catch (IOException e) {
                    log.warn("Failed to send a response back to {}: {}", remote, e.toString());
                }
            });
            return ctx.getStopAction();
        }
    }

    /** Client side: hand the response to whichever request is waiting for it. */
    private final class ClientFilter extends BaseFilter {

        @Override
        public NextAction handleRead(FilterChainContext ctx) {
            Message msg = ctx.getMessage();
            CompletableFuture<Message> f = PENDING.remove(ctx.getConnection());
            if (f != null && msg != null) {
                f.complete(msg);
            } else {
                // Nobody is waiting for it: almost certainly a response that arrived after
                // its request had already timed out
                log.debug("Discarding a response nobody is waiting for");
            }
            return ctx.getStopAction();
        }

        @Override
        public NextAction handleClose(FilterChainContext ctx) {
            CompletableFuture<Message> f = PENDING.remove(ctx.getConnection());
            if (f != null) {
                f.completeExceptionally(new IOException("the peer closed the connection"));
            }
            return ctx.getStopAction();
        }
    }
}
