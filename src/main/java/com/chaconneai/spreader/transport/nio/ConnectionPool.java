package com.chaconneai.spreader.transport.nio;

import com.chaconneai.spreader.transport.DatagramFragmenter;
import com.chaconneai.spreader.transport.Frames;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of outbound TCP connections.
 *
 * <p>Without reuse, every message pays for a three-way handshake and a four-way close --
 * and since the sender is the one closing, every message leaves a TIME_WAIT behind for
 * 60 seconds, which exhausts the machine's ephemeral ports at a sustained few hundred
 * QPS. The server side already handles several messages in sequence on one connection
 * (see Connection in {@link NioTcpTransport}), so reuse needs cooperation from the
 * sending side only.
 *
 * <h2>Why connections are lent exclusively rather than multiplexed</h2>
 * An outbound request is synchronous: it writes and then reads the response on the same
 * thread. Were two threads to share one connection, A could read B's response. So a lent
 * connection <b>belongs to exactly one thread</b> until it is returned. The pool saves
 * the connection setup; it does not provide concurrent sharing.
 *
 * <h2>How a borrowed connection is kept clean</h2>
 * <ul>
 *   <li><b>The idle timeout must be shorter than the server's.</b> The server closes at
 *       {@link NioTcpTransport#IDLE_TIMEOUT_MS}, so holding connections longer than that
 *       means lending out dead ones -- slower than not pooling at all. The constructor
 *       validates this.</li>
 *   <li><b>Both borrow and return check for leftover data.</b> In a request/response
 *       protocol there should be nothing readable on an idle connection. Finding data
 *       means the exchange has gone out of step, and such a connection must be discarded
 *       -- keeping it would let the next request read the previous response.</li>
 *   <li>Even so, a half-open connection the peer just closed can still be borrowed: the
 *       write lands in the kernel buffer and looks successful, and only the read finds
 *       EOF. So a caller using a pooled connection has to be able to retry -- see
 *       {@link NioTcpTransport#request}.</li>
 * </ul>
 *
 * <p>Last in, first out: the most recently used connection is reused first, so cold ones
 * age out to the timeout and get swept away.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
class ConnectionPool {

    private final int maxIdlePerHost;
    private final long idleTimeoutMs;
    private final Logger log;

    private final Map<InetSocketAddress, Deque<Pooled>> idle = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed;

    /** Kept purely so the log can report how well the pool is doing. */
    private long hits;
    private long misses;

    ConnectionPool(int maxIdlePerHost, long idleTimeoutMs, Logger log) {
        if (idleTimeoutMs >= NioTcpTransport.IDLE_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "the pool idle timeout (" + idleTimeoutMs + "ms) must be shorter than the "
                            + "server's idle reclaim time (" + NioTcpTransport.IDLE_TIMEOUT_MS
                            + "ms); otherwise the pool holds nothing but connections the peer has closed");
        }
        this.maxIdlePerHost = maxIdlePerHost;
        this.idleTimeoutMs = idleTimeoutMs;
        this.log = log;
    }

    /**
     * Borrows a connection.
     *
     * @return a usable connection, or null when the pool has none -- in which case the
     *         caller opens a new one
     */
    SocketChannel borrow(InetSocketAddress addr) {
        long now = System.currentTimeMillis();
        lock.lock();
        try {
            if (closed) {
                return null;
            }
            Deque<Pooled> queue = idle.get(addr);
            if (queue == null) {
                misses++;
                return null;
            }
            Pooled p;
            while ((p = queue.pollLast()) != null) {
                if (now - p.lastUsedMs > idleTimeoutMs || !isClean(p.channel)) {
                    NioChannelIO.closeQuietly(p.channel);
                    continue;
                }
                hits++;
                return p.channel;
            }
            idle.remove(addr);
            misses++;
            return null;
        } finally {
            lock.unlock();
        }
    }

    /** Returns a used connection. Anything not reusable is simply closed, so the caller
     *  need not decide. */
    void release(InetSocketAddress addr, SocketChannel ch) {
        if (ch == null) {
            return;
        }
        lock.lock();
        try {
            if (closed || !ch.isOpen() || !ch.isConnected() || !isClean(ch)) {
                NioChannelIO.closeQuietly(ch);
                return;
            }
            Deque<Pooled> queue = idle.computeIfAbsent(addr, k -> new ArrayDeque<>());
            if (queue.size() >= maxIdlePerHost) {
                // Pool is full: no point holding a surplus connection and its file descriptor
                NioChannelIO.closeQuietly(ch);
                return;
            }
            queue.addLast(new Pooled(ch, System.currentTimeMillis()));
        } finally {
            lock.unlock();
        }
    }

    /** Retires a connection for good, after a failure while using it. It must not go back in the pool. */
    void discard(SocketChannel ch) {
        NioChannelIO.closeQuietly(ch);
    }

    /** Sweeps out idle connections past their timeout; called periodically by the transport's event loop. */
    void evictIdle() {
        long now = System.currentTimeMillis();
        lock.lock();
        try {
            for (Iterator<Map.Entry<InetSocketAddress, Deque<Pooled>>> it = idle.entrySet().iterator();
                 it.hasNext(); ) {
                Deque<Pooled> queue = it.next().getValue();
                // The head is the least recently used, so the first one within the timeout ends the sweep
                while (!queue.isEmpty() && now - queue.peekFirst().lastUsedMs > idleTimeoutMs) {
                    NioChannelIO.closeQuietly(queue.pollFirst().channel);
                }
                if (queue.isEmpty()) {
                    it.remove();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void close() {
        lock.lock();
        try {
            closed = true;
            for (Deque<Pooled> queue : idle.values()) {
                for (Pooled p : queue) {
                    NioChannelIO.closeQuietly(p.channel);
                }
            }
            idle.clear();
            if (log.isDebugEnabled()) {
                log.debug("Connection pool closed with {} hit(s) and {} miss(es)", hits, misses);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether the connection has no leftover data on it.
     *
     * <p>In a request/response protocol, reading nothing from an idle connection is the
     * only normal outcome: -1 means the peer has closed it, and anything &gt; 0 means the
     * previous exchange went out of step. Neither may be reused.
     *
     * <p>This does consume a byte, but only on the branch where the connection has to be
     * discarded anyway, so there is no side effect.
     */
    private static boolean isClean(SocketChannel ch) {
        try {
            ByteBuffer probe = ByteBuffer.allocate(1);
            int n = ch.read(probe);
            return n == 0;
        } catch (IOException e) {
            return false;
        }
    }

    private static final class Pooled {
        final SocketChannel channel;
        final long lastUsedMs;

        Pooled(SocketChannel channel, long lastUsedMs) {
            this.channel = channel;
            this.lastUsedMs = lastUsedMs;
        }
    }
}
