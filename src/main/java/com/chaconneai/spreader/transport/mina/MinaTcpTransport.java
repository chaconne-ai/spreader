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
package com.chaconneai.spreader.transport.mina;

import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.HashSet;
import java.net.SocketAddress;
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
 * The TCP transport built on Apache MINA.
 *
 * <p>The protocol is identical to the other three implementations, so nodes running any
 * of them can share one cluster.
 *
 * <h2>One acceptor for every listening port</h2>
 * A single MINA acceptor is bound to all of them. The alternative -- an acceptor per
 * port -- was tried first and abandoned; see the documentation on {@link #acceptor} for
 * what it cost.
 *
 * <p>The price of sharing is that "which address did I just bind" has to be recovered by
 * diffing the address set before and after, since port 0 is assigned by the system and
 * the real port must be read back. That is a small, contained piece of bookkeeping.
 *
 * <h2>One request at a time per connection</h2>
 * As with the built-in and Netty implementations: the server hands processing to a
 * worker pool, so responses do not necessarily come back in request order.
 * So each connection carries one in-flight request at a time, and concurrency comes from
 * having several connections in the pool.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MinaTcpTransport implements Transport {

    /** The response awaited on a session. */
    private static final String PENDING = "spreader.pending";

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

    /**
     * The one acceptor; every listening port is bound to it.
     *
     * <h2>Why not one per port</h2>
     * That is how it was written at first, for the clean {@link #open}/{@link #close}
     * semantics. <b>The cost was misjudged</b>: a MINA acceptor is heavy -- each brings
     * its own IoProcessor thread pool -- and {@code dispose(false)} releases
     * asynchronously. Starting and stopping clusters repeatedly in one process piles those
     * acceptors up until resources run out, at which point bind starts failing
     * ({@code RuntimeIoException: Failed to bind to: []}) and nodes cannot join.
     *
     * <p>The full test matrix is what forced this out: the MINA/UDP cell failed five
     * cases, while running the transport tests alone was perfectly green -- there simply
     * was not enough volume.
     *
     * <p>MINA supports binding one acceptor to several addresses, and that is its standard
     * usage anyway.
     */
    private NioSocketAcceptor acceptor;

    /** Listening port -> the address bound to it. Needed when unbinding one port. */
    private final Map<Integer, InetSocketAddress> listeners = new ConcurrentHashMap<>();

    private NioSocketConnector connector;

    /** The outbound connection pool, grouped by target address. */
    private final Map<InetSocketAddress, Deque<Pooled>> pool = new ConcurrentHashMap<>();

    private final ExecutorService workers;

    public MinaTcpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        this.maxMessageBytes = config.maxMessageBytes();
        this.connectTimeoutMs = config.connectTimeoutMs();
        this.log = config.log();
        int n = config.workerThreads();
        // The producer is the network receive thread: a full queue can only discard,
        // never push back -- stalling it would stop gossip heartbeats going out and get
        // this node declared failed
        this.workers = ExecutorUtils.discarding("mina-gossip-worker", Math.max(2, n), 4096);
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
            connector = new NioSocketConnector();
            connector.setConnectTimeoutMillis(connectTimeoutMs);
            connector.getFilterChain().addLast("codec",
                    new ProtocolCodecFilter(MinaFrames.codecFactory(maxMessageBytes)));
            connector.setHandler(new ClientHandler());
            connector.getSessionConfig().setTcpNoDelay(true);

            acceptor = new NioSocketAcceptor();
            // This MUST be off, or stop() hangs FOREVER.
            //
            // With it on, unbind() ends up in IoServiceListenerSupport.disconnectSessions(),
            // which contains:
            //
            //     for (session : managedSessions) session.closeNow().addListener(...);
            //     while (!managedSessions.isEmpty()) lock.wait(500);
            //
            // The wait has a 500ms timeout, but THE OUTER WHILE HAS NONE -- so long as one
            // session remains in managedSessions, that loop spins forever. And a session is
            // removed only when IoProcessor fires the fireSessionDestroyed callback, so if
            // any session was already closed once (the peer disconnected first, or we called
            // closeNow at the top of stop()), the callback never comes a second time and
            // managedSessions is never empty.
            //
            // Measured: on Docker/Linux, TransportProviderTest froze here for 20 minutes
            // without exiting, with all 10 NioProcessors spinning in EPoll.wait and not one
            // of them processing the close. The same code will not reproduce it on macOS --
            // KQueue and EPoll have different timing, and this race only reproduces reliably
            // on Linux.
            //
            // With it off, sessions are reaped asynchronously by the dispose(false) that
            // follows. The sockets are released just the same; unbind() simply no longer
            // waits for it synchronously -- the same reasoning as dispose(false) below
            acceptor.setCloseOnDeactivation(false);
            acceptor.setReuseAddress(true);
            acceptor.setBacklog(128);
            acceptor.getSessionConfig().setTcpNoDelay(true);
            acceptor.getFilterChain().addLast("codec",
                    new ProtocolCodecFilter(MinaFrames.codecFactory(maxMessageBytes)));
            acceptor.setHandler(new ServerHandler());

            boundPort = WorkPortBinder.bind(config, this::bindListener, MinaTcpTransport::actualPort);
            log.info("Gossip MINA TCP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException | RuntimeException e) {
            running.set(false);
            disposeAll();
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
                    p.session.closeNow();
                }
            });
            pool.clear();
            disposeAll();
            workers.shutdownNow();
            log.info("Gossip MINA TCP transport stopped");
        }
    }

    private void disposeAll() {
        listeners.clear();
        if (acceptor != null) {
            acceptor.unbind();
            // dispose(false): do not wait for it to finish.
            //
            // dispose(true) was tried and IT HANGS: MINA waits indefinitely in
            // awaitTermination while its IoProcessor threads, under certain timings, never
            // exit. Measured: the main thread stuck in tearDown for 29 minutes, with the
            // entire test suite at a standstill.
            //
            // And waiting synchronously was never needed: the acceptors piled up because
            // there was one per port, which binding a single acceptor to several addresses
            // already solved. The whole transport now holds one acceptor, and releasing it
            // asynchronously cannot pile up into anything
            acceptor.dispose(false);
            acceptor = null;
        }
        if (connector != null) {
            connector.dispose(false);
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
            InetSocketAddress existing = listeners.get(port);
            if (existing != null) {
                return existing;
            }
            try {
                InetSocketAddress bound = bindListener(port);
                log.info("Claimed the cluster port {}:{}", bindHost, port);
                return bound;
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
        InetSocketAddress bound = listeners.remove(port);
        if (bound == null) {
            return;
        }
        // Unbind this address only; the acceptor still serves the other ports
        acceptor.unbind(bound);
        log.info("Released the cluster port {}", port);
    }

    @Override
    public boolean holds(int port) {
        // Check the acceptor's REAL binding state, not merely whether the map holds the key --
        // with a dead socket and a stale map entry this node would go on believing it is
        // the leader while the port had long since been freed and taken by someone else.
        // See the matching comment in NioUdpTransport
        InetSocketAddress bound = listeners.get(port);
        if (bound == null) {
            return false;
        }
        if (acceptor == null || !acceptor.getLocalAddresses().contains(bound)) {
            listeners.remove(port, bound);
            log.warn("The socket for cluster port {} has failed; this node no longer holds it", port);
            return false;
        }
        return true;
    }

    /**
     * Binds a port to the shared acceptor.
     *
     * <p>Port 0 is assigned by the system, so the real port is recovered by diffing the
     * address set before and after the bind: with several addresses bound,
     * {@code getLocalAddress()} makes no promise about which one it returns.
     */
    @Override
    public boolean checkAndRepairWorkPort() {
        if (!running.get()) {
            return false;
        }
        InetSocketAddress bound = listeners.get(boundPort);
        if (bound != null && acceptor != null
                && acceptor.getLocalAddresses().contains(bound)) {
            return true;
        }
        // Rebind the same port; invisible from outside. See Transport#checkAndRepairWorkPort
        listeners.remove(boundPort);
        try {
            bindListener(boundPort);
            log.warn("The socket for work port {} had failed and has been rebound in place", boundPort);
            return true;
        } catch (IOException | RuntimeException e) {
            log.error("Work port " + boundPort + " has failed and cannot be rebound; this node "
                    + "now receives nothing at all", e);
            return false;
        }
    }

    private InetSocketAddress bindListener(int port) throws IOException {
        Set<SocketAddress> before = new HashSet<>(acceptor.getLocalAddresses());
        acceptor.bind(new InetSocketAddress(bindHost, port));

        Set<SocketAddress> after = new HashSet<>(acceptor.getLocalAddresses());
        after.removeAll(before);
        InetSocketAddress bound = after.stream()
                .filter(InetSocketAddress.class::isInstance)
                .map(InetSocketAddress.class::cast)
                .findFirst()
                .orElseThrow(() -> new IOException("no new address appeared after binding: " + port));
        listeners.put(bound.getPort(), bound);
        return bound;
    }

    private static int actualPort(InetSocketAddress address) {
        return address.getPort();
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
        IoSession pooled = borrow(addr);
        if (pooled != null) {
            try {
                Message response = exchange(pooled, request, timeoutMs);
                release(addr, pooled);
                return response;
            } catch (IOException e) {
                pooled.closeNow();
                log.debug("A pooled connection failed; retrying on a fresh one: {} ({})", addr, e.toString());
            }
        }
        IoSession fresh = connect(addr);
        try {
            Message response = exchange(fresh, request, timeoutMs);
            release(addr, fresh);
            return response;
        } catch (IOException e) {
            fresh.closeNow();
            throw e;
        }
    }

    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        IoSession session = connect(addr);
        try {
            return exchange(session, request, timeoutMs);
        } finally {
            session.closeNow();
        }
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        IoSession session = config.connectionPoolEnabled() ? borrow(addr) : null;
        boolean pooled = session != null;
        if (session == null) {
            session = connect(addr);
        }
        try {
            write(session, message, timeoutMs);
            if (config.connectionPoolEnabled()) {
                release(addr, session);
            } else {
                session.closeNow();
            }
        } catch (IOException e) {
            session.closeNow();
            if (!pooled) {
                throw e;
            }
            // A pooled connection may have been closed by the peer; retry once on a fresh one
            IoSession fresh = connect(addr);
            try {
                write(fresh, message, timeoutMs);
                release(addr, fresh);
            } catch (IOException retry) {
                fresh.closeNow();
                throw retry;
            }
        }
    }

    private void write(IoSession session, Message msg, int timeoutMs) throws IOException {
        WriteFuture f = session.write(msg);
        if (!f.awaitUninterruptibly(timeoutMs, TimeUnit.MILLISECONDS) || !f.isWritten()) {
            throw new IOException("send failed: " + session.getRemoteAddress(), f.getException());
        }
    }

    @SuppressWarnings("unchecked")
    private Message exchange(IoSession session, Message request, int timeoutMs) throws IOException {
        CompletableFuture<Message> future = new CompletableFuture<>();
        session.setAttribute(PENDING, future);
        try {
            write(session, request, timeoutMs);
            Message response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (response.seq() != request.seq()) {
                // The previous response was left on the connection, so this answer is wrong.
                // Better to fail loudly than hand it upward
                throw new IOException("response sequence mismatch: expected " + request.seq()
                        + ", got " + response.seq());
            }
            return response;
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("timed out waiting for a response: " + session.getRemoteAddress());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for a response", e);
        } catch (ExecutionException e) {
            throw new IOException("connection error: " + session.getRemoteAddress(), e.getCause());
        } finally {
            session.removeAttribute(PENDING);
        }
    }

    private IoSession connect(InetSocketAddress addr) throws IOException {
        ConnectFuture f = connector.connect(addr);
        if (!f.awaitUninterruptibly(connectTimeoutMs, TimeUnit.MILLISECONDS) || !f.isConnected()) {
            f.cancel();
            throw new IOException("could not connect: " + addr, f.getException());
        }
        return f.getSession();
    }

    // ------------------------------------------------------------------
    // Connection pool
    // ------------------------------------------------------------------

    private IoSession borrow(InetSocketAddress addr) {
        Deque<Pooled> q = pool.get(addr);
        if (q == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        Pooled p;
        synchronized (q) {
            while ((p = q.pollLast()) != null) {
                if (!p.session.isConnected() || now - p.idleSinceMs > IDLE_TIMEOUT_MS) {
                    p.session.closeNow();
                    continue;
                }
                return p.session;
            }
        }
        return null;
    }

    private void release(InetSocketAddress addr, IoSession session) {
        if (!config.connectionPoolEnabled() || !session.isConnected()) {
            session.closeNow();
            return;
        }
        Deque<Pooled> q = pool.computeIfAbsent(addr, k -> new ArrayDeque<>());
        synchronized (q) {
            if (q.size() >= config.poolMaxIdlePerHost()) {
                session.closeNow();
                return;
            }
            q.addLast(new Pooled(session, System.currentTimeMillis()));
        }
    }

    private record Pooled(IoSession session, long idleSinceMs) {
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /** Server side: parse the message, hand it to the worker pool, and write back any response. */
    private final class ServerHandler extends IoHandlerAdapter {

        @Override
        public void messageReceived(IoSession session, Object message) {
            MessageHandler h = handler;
            if (h == null || !(message instanceof Message msg)) {
                return;
            }
            InetSocketAddress remote = (InetSocketAddress) session.getRemoteAddress();
            workers.execute(() -> {
                Message response;
                try {
                    response = h.handle(msg, remote);
                } catch (Throwable t) {
                    log.error("Failed to handle an inbound message: " + msg, t);
                    return;
                }
                if (response != null && session.isConnected()) {
                    session.write(response);
                }
            });
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) {
            // Stray traffic from port scanners and the like lands here; not worth an error
            log.debug("Inbound connection {} threw: {}", session.getRemoteAddress(), cause.toString());
            session.closeNow();
        }
    }

    /** Client side: hand the response to whichever request is waiting for it. */
    private final class ClientHandler extends IoHandlerAdapter {

        @SuppressWarnings("unchecked")
        @Override
        public void messageReceived(IoSession session, Object message) {
            CompletableFuture<Message> f =
                    (CompletableFuture<Message>) session.removeAttribute(PENDING);
            if (f != null && message instanceof Message msg) {
                f.complete(msg);
            } else {
                // Nobody is waiting for it: almost certainly a response that arrived after
                // its request had already timed out
                log.debug("Discarding a response nobody is waiting for: {}", message);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void sessionClosed(IoSession session) {
            CompletableFuture<Message> f =
                    (CompletableFuture<Message>) session.removeAttribute(PENDING);
            if (f != null) {
                f.completeExceptionally(new IOException("the peer closed the connection"));
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void exceptionCaught(IoSession session, Throwable cause) {
            CompletableFuture<Message> f =
                    (CompletableFuture<Message>) session.removeAttribute(PENDING);
            if (f != null) {
                f.completeExceptionally(cause);
            }
            session.closeNow();
        }
    }
}
