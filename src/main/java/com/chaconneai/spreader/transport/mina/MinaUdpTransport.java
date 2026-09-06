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
import com.chaconneai.spreader.transport.DatagramFragmenter;
import com.chaconneai.spreader.transport.Frames;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioDatagramAcceptor;
import org.apache.mina.util.ExceptionMonitor;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.HashSet;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
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
 * The UDP transport built on Apache MINA.
 *
 * <h2>No codec filter here</h2>
 * A UDP datagram is a complete boundary in itself, so there is no stream framing to do
 * and MINA's cumulative decoder has nothing to contribute. The real question is what
 * happens when a message exceeds 64KB, which is an application protocol decision, and it
 * goes to {@link DatagramFragmenter} -- the same code and the same wire format as the
 * other three implementations.
 *
 * <p>So the handler receives the raw {@link IoBuffer} and runs {@link Frames#decode}
 * after reassembly.
 *
 * <h2>Outbound sessions are cached per target address</h2>
 * Every connect on MINA's UDP connector is a new socket. Gossip probes frequently and
 * building one each time is not cheap, so sessions are cached and reused per target
 * address; responses find their way back through the waiting table by {@code seq}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MinaUdpTransport implements Transport {

    private final GossipConfig config;
    private final String bindHost;
    private final int maxMessageBytes;
    private final Logger log;
    private final DatagramFragmenter fragmenter;

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
    private NioDatagramAcceptor acceptor;

    /** Listening port -> the address bound to it. Needed when unbinding one port. */
    private final Map<Integer, InetSocketAddress> listeners = new ConcurrentHashMap<>();

    private NioDatagramConnector connector;

    /** Outbound sessions, cached by target address. */
    private final Map<InetSocketAddress, IoSession> outbound = new ConcurrentHashMap<>();

    /** Requests awaiting a response, indexed by {@code seq}. */
    private final Map<Long, CompletableFuture<Message>> pending = new ConcurrentHashMap<>();

    /** ExceptionMonitor is a global static in MINA, so installing it once is enough. */
    private static final AtomicBoolean QUIET_MONITOR_INSTALLED = new AtomicBoolean();

    private final ExecutorService workers;

    public MinaUdpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        this.maxMessageBytes = config.maxMessageBytes();
        this.log = config.log();
        this.fragmenter = new DatagramFragmenter(
                Frames.MAX_DATAGRAM_BYTES, maxMessageBytes, config.log());
        // Halved, to line up with Netty (NettyUdpTransport uses workerThreads / 2).
        //
        // This pool serves TWO purposes at once: business dispatch, and MINA's acceptor
        // threads (see new NioDatagramAcceptor(workers) in start()). Opening it at the full
        // workerThreads would make one transport instance 16 threads where Netty has 8 --
        // the same setting meaning twice as much under one implementation as the other,
        // which is unreasonable on its face.
        //
        // In CPU-constrained deployments the difference is decisive: start several clusters
        // at once in a 2-core container and the thread count far exceeds what the scheduler
        // can handle. Measured: load average up at 73, 42% of CPU time spent in system mode
        // (all of it context switching), and the tests an order of magnitude slower than on
        // a 12-core machine. A Kubernetes Pod with a cpu limit runs into exactly this
        int n = Math.max(2, config.workerThreads() / 2);
        // The producer is the network receive thread: a full queue can only discard,
        // never push back -- stalling it would stop gossip heartbeats going out and get
        // this node declared failed
        this.workers = ExecutorUtils.discarding("mina-gossip-worker", n, 4096);
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
            installQuietExceptionMonitor();

            // Heap or direct. MINA's allocator is a global static too, like ExceptionMonitor.
            // The default SimpleBufferAllocator is on-heap; this switch is what moves it off.
            // Same reasoning as on the Netty side, see GossipConfig.directBuffers()
            if (config.directBuffers()) {
                IoBuffer.setUseDirectBuffer(true);
            }

            // The processor thread count follows the acceptor; do not let it open another
            // set sized by CPU count. Two is plenty in a container, and more only worsens
            // context switching
            connector = new NioDatagramConnector(1);
            connector.setConnectTimeoutMillis(config.connectTimeoutMs());
            connector.getSessionConfig().setReadBufferSize(Frames.MAX_DATAGRAM_BYTES);
            // setReadBufferSize governs how much is read at a time; these two govern how
            // deep the kernel queues are. Different things, and both need setting.
            // See Frames.SOCKET_BUFFER_BYTES
            connector.getSessionConfig().setReceiveBufferSize(Frames.SOCKET_BUFFER_BYTES);
            connector.getSessionConfig().setSendBufferSize(Frames.SOCKET_BUFFER_BYTES);
            connector.setHandler(new InboundHandler());

            // Put the acceptor's threads IN OUR OWN POOL rather than letting MINA build its
            // own.
            //
            // The no-argument constructor starts a fresh set of threads every time, and
            // dispose(false) releases asynchronously -- so starting and stopping clusters in
            // one process makes the thread count climb and never fall. Measured: it reached
            // NioDatagramAcceptor-164, while the same tests under the built-in NIO piled up
            // nothing at all. The consequence is not an error but GETTING SLOWER AND SLOWER:
            // LatchBarrierClusterTest took 12 seconds under NIO and 511 under MINA, 42 times
            // slower. It shows worst on machines with few cores, since all those accumulated
            // threads are competing for the same handful.
            //
            // Handed a fixed-size pool, the thread count is bounded by that pool no matter
            // how many times the transport is started and stopped
            acceptor = new NioDatagramAcceptor(workers);
            // MUST be off, or stop() hangs forever. Exactly the same reasoning as at the
            // matching point in MinaTcpTransport: unbind() ends up in disconnectSessions()'s
            // while (!managedSessions.isEmpty()), that loop has no timeout, and one session
            // left unremoved means it never exits.
            //
            // UDP is even likelier to hit it: MINA fabricates a session per peer address,
            // and gossip's peers change constantly, so closed sessions are left behind more
            // often than over TCP
            acceptor.setCloseOnDeactivation(false);
            // MUST be false. Port exclusion is the leader election itself, and for UDP,
            // SO_REUSEADDR on BSD and macOS behaves exactly like SO_REUSEPORT: leave it on
            // and two nodes will both "claim" the same cluster port successfully -- two
            // leaders, split brain, and not one error anywhere.
            // See the matching comment in NioUdpTransport for the full reasoning
            acceptor.getSessionConfig().setReuseAddress(false);
            acceptor.getSessionConfig().setReadBufferSize(Frames.MAX_DATAGRAM_BYTES);
            acceptor.getSessionConfig().setReceiveBufferSize(Frames.SOCKET_BUFFER_BYTES);
            acceptor.getSessionConfig().setSendBufferSize(Frames.SOCKET_BUFFER_BYTES);
            acceptor.setHandler(new InboundHandler());

            boundPort = WorkPortBinder.bind(config, this::bindListener, MinaUdpTransport::actualPort);
            log.info("Gossip MINA UDP transport started on work port {}:{}", bindHost, boundPort);
        } catch (IOException | RuntimeException e) {
            running.set(false);
            disposeAll();
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
        outbound.values().forEach(IoSession::closeNow);
        outbound.clear();
        disposeAll();
        workers.shutdownNow();
        log.info("Gossip MINA UDP transport stopped");
    }

    /**
     * Silences the exception noise MINA makes while shutting down.
     *
     * <p>{@code dispose()} interrupts the acceptor thread blocked in
     * {@code Semaphore.acquire()} to tell it to exit -- MINA's <b>own</b> internal signal,
     * and entirely normal. But the default {@code DefaultExceptionMonitor} treats it as an
     * unexpected exception and prints it with a full stack trace. Measured: 196 of them in
     * one full test run, against none at all under the built-in NIO.
     *
     * <p>The noise is the lesser problem. What matters is that it buries the <b>real</b>
     * exceptions: with a screen full of the same InterruptedException, nobody checks
     * whether the 197th is different.
     *
     * <p>Only this one is silenced. Everything else still goes to the default
     * implementation -- swallowing all exceptions is more dangerous than printing too
     * many.
     */
    private static void installQuietExceptionMonitor() {
        if (!QUIET_MONITOR_INSTALLED.compareAndSet(false, true)) {
            return;
        }
        ExceptionMonitor previous = ExceptionMonitor.getInstance();
        ExceptionMonitor.setInstance(new ExceptionMonitor() {
            @Override
            public void exceptionCaught(Throwable cause) {
                if (cause instanceof InterruptedException) {
                    // Used to wake the acceptor thread during shutdown; not a failure
                    return;
                }
                previous.exceptionCaught(cause);
            }
        });
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

    @Override
    public InetSocketAddress open(int port) {
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
        // See the matching comment in NioUdpTransport.
        //
        // What is stored here is an address rather than a session (one acceptor is bound
        // to several ports), so the only way is to ask the acceptor which addresses it
        // currently holds
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
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.put(request.seq(), future);
        try {
            emit(outboundSession(addr), request);
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
     * <p>Range scanning takes this path, and it meets a great many addresses that
     * <b>do not exist at all</b>. Were sessions cached per address as in {@link #request},
     * one sweep of 254 addresses would leave 254 sessions behind -- and scanning is
     * periodic. Sockets run out quickly, surfacing as every subsequent JOIN timing out and
     * nodes being unable to join.
     *
     * <p>So one is built here per call and closed straight after. That is precisely why
     * {@code requestDirect} exists: its difference from {@code request} was never "does it
     * wait for an answer",
     * but <b>"is this address worth remembering"</b>.
     */
    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.put(request.seq(), future);
        IoSession session = null;
        try {
            session = connectOnce(addr);
            emit(session, request);
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
            if (session != null) {
                session.closeNow();
            }
        }
    }

    /** Builds a throwaway session that never enters the {@link #outbound} cache. */
    private IoSession connectOnce(InetSocketAddress addr) throws IOException {
        ConnectFuture f = connector.connect(addr);
        if (!f.awaitUninterruptibly(config.connectTimeoutMs(), TimeUnit.MILLISECONDS)
                || !f.isConnected()) {
            throw new IOException("could not establish an outbound session: " + addr, f.getException());
        }
        return f.getSession();
    }

    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        emit(outboundSession(addr), message);
    }

    /** Gets, or builds, an outbound session to a target address. */
    private IoSession outboundSession(InetSocketAddress addr) throws IOException {
        IoSession cached = outbound.get(addr);
        if (cached != null && cached.isConnected()) {
            return cached;
        }
        ConnectFuture f = connector.connect(addr);
        if (!f.awaitUninterruptibly(config.connectTimeoutMs(), TimeUnit.MILLISECONDS)
                || !f.isConnected()) {
            throw new IOException("could not establish an outbound session: " + addr, f.getException());
        }
        IoSession session = f.getSession();
        IoSession previous = outbound.put(addr, session);
        if (previous != null && previous != session) {
            previous.closeNow();
        }
        return session;
    }

    /** Encodes, fragments if needed, and writes out. */
    private void emit(IoSession session, Message msg) throws IOException {
        for (ByteBuffer buf : fragmenter.split(Frames.encode(msg))) {
            session.write(IoBuffer.wrap(buf));
        }
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /**
     * Shared by listening sessions and outbound sessions.
     *
     * <p>Telling a request from a response comes down to whether the {@code seq} is in the
     * waiting table, regardless of which session it arrived on.
     */
    private final class InboundHandler extends IoHandlerAdapter {

        @Override
        public void messageReceived(IoSession session, Object message) {
            if (!(message instanceof IoBuffer buf)) {
                return;
            }
            InetSocketAddress from = (InetSocketAddress) session.getRemoteAddress();
            ByteBuffer frame = fragmenter.reassemble(buf.buf(), from);
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
                return;
            }
            dispatch(session, msg, from);
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) {
            log.debug("UDP session threw: {}", cause.toString());
        }
    }

    /** Processing moves to the worker pool; the response goes back out on the very session it arrived on. */
    private void dispatch(IoSession session, Message msg, InetSocketAddress remote) {
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
                emit(session, response);
            } catch (IOException e) {
                log.warn("Failed to send a response back to {}: {}", remote, e.toString());
            }
        });
    }
}
