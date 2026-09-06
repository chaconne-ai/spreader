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
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The TCP transport built on plain NIO.
 *
 * <p>Inbound: a single-threaded Selector event loop handles accept, read and write. Once
 * a message is parsed it goes to a worker pool, and the result comes back through a
 * write queue that the event loop actually writes out. That keeps business processing
 * from blocking the event loop, and keeps several threads from writing to one channel at
 * once.
 *
 * <p>Outbound: a request/response model ({@link #request}) with connections reused
 * through {@link ConnectionPool}. The server already handles several messages in
 * sequence on one connection, so reuse needs cooperation from the sending side only.
 * Range scanning goes through {@link #requestDirect}, keeping those one-shot addresses
 * out of the pool.
 *
 * <p>Several ports can be listened on at once: the work port is bound by
 * {@link #start()} and the cluster port claimed by {@link #open(int)}, both sharing one
 * event loop and one handler.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class NioTcpTransport implements Transport {

    /**
     * How long a server-side connection may sit idle before it is reclaimed.
     *
     * <p>{@link ConnectionPool}'s idle ceiling must be lower, or the pool would hold
     * nothing but connections the peer has already closed.
     */
    static final long IDLE_TIMEOUT_MS = 60_000L;

    /** The event loop's select timeout, which also sets how often idle connections are checked. */
    private static final long SELECT_TIMEOUT_MS = 1_000L;

    private final GossipConfig config;
    private final String bindHost;
    private final int connectTimeoutMs;
    private final int maxMessageBytes;
    private final Logger log;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong seqGen = new AtomicLong();

    /** Tasks submitted from other threads for the event loop to run, such as registering interest in writability. */
    private final ConcurrentLinkedQueue<Runnable> pendingTasks = new ConcurrentLinkedQueue<>();

    private volatile MessageHandler handler;
    private volatile int boundPort = -1;

    /** Every port currently listened on: the work port plus any claimed cluster port. */
    private final ConcurrentHashMap<Integer, ServerSocketChannel> listeners = new ConcurrentHashMap<>();

    private Selector selector;
    private Thread eventLoop;
    private final ExecutorService workers;

    /** The outbound connection pool; null when reuse is switched off. */
    private final ConnectionPool pool;

    public NioTcpTransport(GossipConfig config) {
        this.config = config;
        this.bindHost = config.bindHost();
        this.connectTimeoutMs = config.connectTimeoutMs();
        this.maxMessageBytes = config.maxMessageBytes();
        this.log = config.log();
        this.pool = config.connectionPoolEnabled()
                ? new ConnectionPool(config.poolMaxIdlePerHost(), config.poolIdleTimeoutMs(), log)
                : null;
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

    /** The work port -- the port this node publishes to everyone else. */
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

    /**
     * Binds the work port and starts the event loop.
     *
     * @throws IOException when every candidate port is taken
     */
    @Override
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            selector = Selector.open();

            boundPort = bindWorkPort();

            eventLoop = new Thread(this::runEventLoop, "gossip-selector");
            eventLoop.setDaemon(true);
            eventLoop.start();

            log.info("Gossip transport started on work port {}:{}", bindHost, boundPort);
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
        if (pool != null) {
            pool.close();
        }
        workers.shutdownNow();
        log.info("Gossip transport stopped");
    }

    // ------------------------------------------------------------------
    // Claiming ports
    // ------------------------------------------------------------------

    /**
     * Claims the cluster port. A successful bind <i>is</i> this node taking leadership:
     * on one machine the operating system's port exclusion guarantees a single winner,
     * and across machines the guarantee comes from the caller looking before claiming.
     */
    @Override
    public InetSocketAddress open(int port) {
        if (!running.get()) {
            return null;
        }
        ServerSocketChannel existing = listeners.get(port);
        if (existing != null) {
            // Already held -- possibly it is the work port itself. Idempotent return
            return (InetSocketAddress) quietLocalAddress(existing);
        }
        try {
            ServerSocketChannel ch = bindListener(port);
            selector.wakeup();
            log.info("Claimed the cluster port {}:{}", bindHost, port);
            return (InetSocketAddress) quietLocalAddress(ch);
        } catch (IOException e) {
            // Someone else holding the port is the normal outcome of the race, not an error
            log.debug("Cluster port {} is already taken: {}", port, e.toString());
            return null;
        }
    }

    @Override
    public void close(int port) {
        if (port == boundPort) {
            // The work port is this node's own footing and must never be released as if
            // it were the cluster port
            return;
        }
        ServerSocketChannel ch = listeners.remove(port);
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
        // Check the socket's real state, not merely whether the map holds the key. With a
        // dead socket and a stale map entry, this node would go on believing it is the
        // leader while the port had long since been freed and taken by someone else.
        // See the matching comment in NioUdpTransport
        ServerSocketChannel ch = listeners.get(port);
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
        ServerSocketChannel ch = listeners.get(boundPort);
        if (ch != null && ch.isOpen()) {
            return true;
        }
        // Rebind the same port; invisible from outside. See Transport#checkAndRepairWorkPort
        //
        // The cleanup must follow the same order as close(port): cancel the key, close the
        // channel, wake the selector. Removing without cancelling the key leaves the JDK to
        // defer the channel's close until the selector deregisters it on its next pass, and
        // the port stays occupied until then -- so the rebind is certain to hit
        // "Address already in use"
        ServerSocketChannel dead = listeners.remove(boundPort);
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
        // Deregistration is asynchronous, so hitting "still occupied" on the first attempt
        // is entirely normal. Retry briefly; and failing that, no harm done -- the check is
        // periodic and will come round again
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

    private int bindWorkPort() throws IOException {
        return WorkPortBinder.bind(config, this::bindListener, NioTcpTransport::actualPort);
    }

    /** Binds a listening port and registers it with the event loop. */
    private ServerSocketChannel bindListener(int port) throws IOException {
        ServerSocketChannel ch = ServerSocketChannel.open();
        try {
            ch.configureBlocking(false);
            // Note: do not enable SO_REUSEPORT. It would let several processes bind the
            // same port at once, and port exclusion -- the natural lock this design rests
            // on -- would stop working
            ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            ch.bind(new InetSocketAddress(bindHost, port), 128);
            ch.register(selector, SelectionKey.OP_ACCEPT);
            // Port 0 is assigned by the system, so read back the actual port; the argument
            // must not be used as the key
            listeners.put(actualPort(ch), ch);
            return ch;
        } catch (IOException e) {
            NioChannelIO.closeQuietly(ch);
            throw e;
        }
    }

    private static int actualPort(ServerSocketChannel ch) throws IOException {
        return ((InetSocketAddress) ch.getLocalAddress()).getPort();
    }

    private void closeAllListeners() {
        for (ServerSocketChannel ch : listeners.values()) {
            NioChannelIO.closeQuietly(ch);
        }
        listeners.clear();
    }

    private static SocketAddress quietLocalAddress(ServerSocketChannel ch) {
        try {
            return ch.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Outbound
    // ------------------------------------------------------------------

    /**
     * Issues a request and waits for the response, preferring a pooled connection.
     *
     * <p>A borrowed connection may have been closed by the peer without anyone noticing:
     * the write lands in the kernel buffer and looks successful, and only the read finds
     * EOF. In that case the call is retried <b>once</b> on a fresh connection. Only
     * pooled connections are retried -- a freshly opened one that fails has genuinely
     * failed, and trying again would prove nothing.
     *
     * <p>Retrying means the peer may process the request twice. Gossip's own messages are
     * all idempotent, and business messages (PAYLOAD) are protected by de-duplication on
     * {@code sender + seq} at the receiving side -- see the deduplicator in
     * {@code DefaultGossipCluster}.
     *
     * @param addr      the target address
     * @param request   the request message
     * @param timeoutMs total read/write timeout
     * @return the response message
     * @throws IOException on connection failure, timeout, or an error at the peer
     */
    @Override
    public Message request(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        if (pool == null) {
            return requestDirect(addr, request, timeoutMs);
        }
        SocketChannel pooled = pool.borrow(addr);
        if (pooled != null) {
            try {
                Message response = exchange(pooled, request, timeoutMs);
                pool.release(addr, pooled);
                return response;
            } catch (IOException e) {
                pool.discard(pooled);
                log.debug("A pooled connection failed; retrying on a fresh one: {} ({})", addr, e.toString());
            }
        }

        SocketChannel fresh = NioChannelIO.connect(addr, connectTimeoutMs);
        try {
            Message response = exchange(fresh, request, timeoutMs);
            pool.release(addr, fresh);
            return response;
        } catch (IOException e) {
            pool.discard(fresh);
            throw e;
        }
    }

    /** A request that bypasses the pool, for range scanning: pooling those one-shot
     *  addresses would only poison it. */
    @Override
    public Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException {
        SocketChannel ch = null;
        try {
            ch = NioChannelIO.connect(addr, connectTimeoutMs);
            return exchange(ch, request, timeoutMs);
        } finally {
            NioChannelIO.closeQuietly(ch);
        }
    }

    private Message exchange(SocketChannel ch, Message request, int timeoutMs)
            throws IOException {
        NioChannelIO.writeFully(ch, Codec.encode(request), timeoutMs);
        return NioChannelIO.readMessage(ch, timeoutMs, maxMessageBytes);
    }

    /**
     * Sends one way. It does not wait for a response body, but does wait for the TCP
     * write to complete.
     *
     * <p>Only for messages the peer genuinely does not answer -- LEAVE, PAYLOAD_ONEWAY.
     * Were the peer to reply and this not to read it, the response would sit on the
     * connection and the next request would read it as its own.
     */
    @Override
    public void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException {
        if (pool == null) {
            SocketChannel ch = null;
            try {
                ch = NioChannelIO.connect(addr, connectTimeoutMs);
                NioChannelIO.writeFully(ch, Codec.encode(message), timeoutMs);
            } finally {
                NioChannelIO.closeQuietly(ch);
            }
            return;
        }

        SocketChannel pooled = pool.borrow(addr);
        if (pooled != null) {
            try {
                NioChannelIO.writeFully(pooled, Codec.encode(message), timeoutMs);
                pool.release(addr, pooled);
                return;
            } catch (IOException e) {
                pool.discard(pooled);
                log.debug("A pooled connection failed while sending; retrying on a fresh one: {} ({})",
                        addr, e.toString());
            }
        }

        SocketChannel fresh = NioChannelIO.connect(addr, connectTimeoutMs);
        try {
            NioChannelIO.writeFully(fresh, Codec.encode(message), timeoutMs);
            pool.release(addr, fresh);
        } catch (IOException e) {
            pool.discard(fresh);
            throw e;
        }
    }

    // ------------------------------------------------------------------
    // Inbound event loop
    // ------------------------------------------------------------------

    private void runEventLoop() {
        while (running.get()) {
            try {
                runPendingTasks();
                selector.select(SELECT_TIMEOUT_MS);
                if (!running.get()) {
                    break;
                }

                Set<SelectionKey> keys = selector.selectedKeys();
                for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext(); ) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    try {
                        if (key.isAcceptable()) {
                            doAccept((ServerSocketChannel) key.channel());
                        } else if (key.isReadable()) {
                            doRead(key);
                        } else if (key.isWritable()) {
                            doWrite(key);
                        }
                    } catch (CancelledKeyException e) {
                        closeConnection(key, null);
                    } catch (IOException e) {
                        closeConnection(key, e);
                    }
                }
                closeIdleConnections();
            } catch (ClosedChannelException e) {
                if (running.get()) {
                    log.error("The selector channel is closed", e);
                }
                break;
            } catch (Throwable t) {
                if (running.get()) {
                    log.error("The gossip event loop threw", t);
                }
            }
        }
        // Clean up every established connection before leaving; the listening ports are
        // closeAllListeners' job
        if (selector != null && selector.isOpen()) {
            for (SelectionKey key : selector.keys()) {
                if (!(key.channel() instanceof ServerSocketChannel)) {
                    NioChannelIO.closeQuietly(key.channel());
                }
            }
        }
    }

    private void runPendingTasks() {
        Runnable task;
        while ((task = pendingTasks.poll()) != null) {
            try {
                task.run();
            } catch (Throwable t) {
                log.error("An event loop task failed", t);
            }
        }
    }

    private void doAccept(ServerSocketChannel server) throws IOException {
        SocketChannel ch;
        while ((ch = server.accept()) != null) {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            SelectionKey key = ch.register(selector, SelectionKey.OP_READ);
            key.attach(new Connection(ch, key));
        }
    }

    private void doRead(SelectionKey key) throws IOException {
        Connection conn = (Connection) key.attachment();
        if (conn == null) {
            closeConnection(key, null);
            return;
        }
        conn.lastActiveMs = System.currentTimeMillis();

        conn.ensureReadCapacity(1024);
        int n = conn.readBuffer.read(conn.channel);
        if (n < 0) {
            closeConnection(key, null);
            return;
        }

        List<Message> messages = conn.drainMessages(maxMessageBytes);
        if (messages.isEmpty()) {
            return;
        }
        InetSocketAddress remote = conn.remoteAddress();
        for (Message msg : messages) {
            dispatch(conn, msg, remote);
        }
    }

    /** Moves the processing onto the worker pool so the event loop is never blocked. */
    private void dispatch(Connection conn, Message msg, InetSocketAddress remote) {
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
                enqueueWrite(conn, Codec.encode(response));
            } catch (IOException e) {
                log.warn("Failed to encode a response: {}", e.toString());
            }
        });
    }

    /** Called from a worker thread: queues the response and wakes the event loop. */
    private void enqueueWrite(Connection conn, ByteBuffer buf) {
        if (!conn.channel.isOpen()) {
            return;
        }
        synchronized (conn.writeQueue) {
            conn.writeQueue.addLast(buf);
        }
        pendingTasks.add(() -> {
            if (conn.key.isValid()) {
                conn.key.interestOps(conn.key.interestOps() | SelectionKey.OP_WRITE);
            }
        });
        selector.wakeup();
    }

    private void doWrite(SelectionKey key) throws IOException {
        Connection conn = (Connection) key.attachment();
        if (conn == null) {
            closeConnection(key, null);
            return;
        }
        conn.lastActiveMs = System.currentTimeMillis();

        synchronized (conn.writeQueue) {
            while (!conn.writeQueue.isEmpty()) {
                ByteBuffer buf = conn.writeQueue.peekFirst();
                conn.channel.write(buf);
                if (buf.hasRemaining()) {
                    // Kernel buffer is full; keep OP_WRITE and wait for the next writable event
                    return;
                }
                conn.writeQueue.pollFirst();
            }
        }
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    private void closeIdleConnections() {
        long now = System.currentTimeMillis();
        if (now - lastIdleCheckMs < SELECT_TIMEOUT_MS) {
            return;
        }
        lastIdleCheckMs = now;
        if (pool != null) {
            // Sweep the outbound pool's idle connections too. Otherwise the peer closes
            // first while this side keeps them, and the next borrow is bound to fail
            pool.evictIdle();
        }
        for (SelectionKey key : new ArrayList<>(selector.keys())) {
            if (key.attachment() instanceof Connection conn
                    && now - conn.lastActiveMs > IDLE_TIMEOUT_MS) {
                boolean pendingWrite;
                synchronized (conn.writeQueue) {
                    pendingWrite = !conn.writeQueue.isEmpty();
                }
                if (!pendingWrite) {
                    closeConnection(key, null);
                }
            }
        }
    }

    private long lastIdleCheckMs;

    private void closeConnection(SelectionKey key, IOException cause) {
        if (cause != null && log.isDebugEnabled()) {
            log.debug("Closing a connection: {}", cause.toString());
        }
        key.cancel();
        NioChannelIO.closeQuietly(key.channel());
    }

    // ------------------------------------------------------------------
    // Per-connection context
    // ------------------------------------------------------------------

    /** One server-side connection, holding its read buffer and pending write queue. */
    private static final class Connection {
        final SocketChannel channel;
        final SelectionKey key;
        final GrowableReadBuffer readBuffer = new GrowableReadBuffer(4096);
        final Deque<ByteBuffer> writeQueue = new ArrayDeque<>(4);
        volatile long lastActiveMs = System.currentTimeMillis();

        Connection(SocketChannel channel, SelectionKey key) {
            this.channel = channel;
            this.key = key;
        }

        void ensureReadCapacity(int minWritable) {
            readBuffer.ensureWritable(minWritable);
        }

        InetSocketAddress remoteAddress() {
            try {
                return (InetSocketAddress) channel.getRemoteAddress();
            } catch (IOException e) {
                return null;
            }
        }

        /** Parses every complete message out of the read buffer. */
        List<Message> drainMessages(int maxMessageBytes) throws IOException {
            List<Message> result = null;
            while (true) {
                Message msg = readBuffer.tryDecode(maxMessageBytes);
                if (msg == null) {
                    break;
                }
                if (result == null) {
                    result = new ArrayList<>(2);
                }
                result.add(msg);
            }
            readBuffer.compact();
            return result == null ? List.of() : result;
        }
    }

    /**
     * A growable read buffer. It stays in write mode throughout: {@code position} is the
     * number of bytes written, and parsing reads by absolute index. That avoids the state
     * confusion that constant flip/compact cycles bring.
     */
    private static final class GrowableReadBuffer {
        private ByteBuffer buf;
        private int readIndex;

        GrowableReadBuffer(int initialCapacity) {
            this.buf = ByteBuffer.allocate(initialCapacity);
        }

        void ensureWritable(int minWritable) {
            if (buf.remaining() >= minWritable) {
                return;
            }
            compact();
            if (buf.remaining() >= minWritable) {
                return;
            }
            int needed = buf.position() + minWritable;
            int newCap = Math.max(buf.capacity() * 2, needed);
            ByteBuffer bigger = ByteBuffer.allocate(newCap);
            buf.flip();
            bigger.put(buf);
            buf = bigger;
        }

        int read(SocketChannel ch) throws IOException {
            return ch.read(buf);
        }

        /** Bytes written but not yet consumed. */
        private int readableBytes() {
            return buf.position() - readIndex;
        }

        /** Tries to parse one complete message, returning null when there is not enough data. */
        Message tryDecode(int maxMessageBytes) throws IOException {
            if (readableBytes() < Codec.HEADER_LEN) {
                return null;
            }
            int magic = buf.getInt(readIndex);
            if (magic != Codec.MAGIC) {
                throw new IOException("bad message magic: 0x" + Integer.toHexString(magic));
            }
            byte version = buf.get(readIndex + 4);
            if (version != Codec.VERSION) {
                throw new IOException("unsupported protocol version: " + version);
            }
            byte type = buf.get(readIndex + 5);
            int len = buf.getInt(readIndex + 6);
            if (len < 0 || len > maxMessageBytes) {
                throw new IOException("bad message length: " + len);
            }
            if (readableBytes() < Codec.HEADER_LEN + len) {
                // Partial message: reserve room for the whole thing and wait for the rest
                ensureWritable(Codec.HEADER_LEN + len - readableBytes());
                return null;
            }
            byte[] payload = new byte[len];
            int start = readIndex + Codec.HEADER_LEN;
            for (int i = 0; i < len; i++) {
                payload[i] = buf.get(start + i);
            }
            readIndex = start + len;
            return Codec.decode(type, payload);
        }

        /** Drops consumed data and moves what remains to the front of the buffer. */
        void compact() {
            if (readIndex == 0) {
                return;
            }
            int remaining = readableBytes();
            if (remaining > 0) {
                buf.limit(buf.position());
                buf.position(readIndex);
                buf.compact();
            } else {
                buf.clear();
            }
            readIndex = 0;
        }
    }
}
