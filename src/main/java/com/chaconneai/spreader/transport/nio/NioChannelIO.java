package com.chaconneai.spreader.transport.nio;

import com.chaconneai.spreader.transport.DatagramFragmenter;
import com.chaconneai.spreader.transport.Frames;
import com.chaconneai.spreader.transport.MessageHandler;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.WorkPortBinder;

import com.chaconneai.spreader.protocol.Codec;
import com.chaconneai.spreader.protocol.Message;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Read/write helpers for outbound SocketChannels.
 *
 * <p>Frame parsing lives in {@link Frames}, shared by all four transport
 * implementations. What is here is the streaming read -- TCP reads and waits in turns,
 * so the "the data is already sitting complete in a buffer" path does not apply.
 *
 * <p>Everything uses a non-blocking SocketChannel plus a Selector, which is what makes
 * precise timeouts possible across all three phases of connect, write and read.
 * ({@code SocketChannel.socket().setSoTimeout()} has no effect on an NIO channel, so it
 * cannot be relied upon.) Creating a Selector is not cheap, so one is cached per thread
 * and reused -- a range scan opens thousands of short-lived connections, and reuse makes
 * that cost negligible.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class NioChannelIO {

    /** One Selector reused per calling thread. */
    private static final ThreadLocal<Selector> SELECTORS = new ThreadLocal<>();

    private NioChannelIO() {
    }

    private static Selector selector() throws IOException {
        Selector sel = SELECTORS.get();
        if (sel == null || !sel.isOpen()) {
            sel = Selector.open();
            SELECTORS.set(sel);
        }
        return sel;
    }

    /**
     * Lets the UDP transport in this package share the per-thread Selector.
     *
     * <p>{@code Selector.open()} creates an epoll/kqueue handle, which is far from free.
     * Opening one per message would cost more than the data itself at any real send rate.
     *
     * <p>Callers <b>must</b> call {@link #release} afterwards to cancel the key, or a
     * leftover key will interfere with the next use.
     */
    static Selector sharedSelector() throws IOException {
        return selector();
    }

    /** Cancels the key immediately, so nothing is left to interfere with the next use. */
    static void release(Selector sel, SelectionKey key) {
        if (key != null) {
            key.cancel();
        }
        if (sel != null) {
            try {
                sel.selectNow();
            } catch (IOException ignore) {
                // A failed cleanup does not affect the main path
            }
        }
    }

    /**
     * Opens a TCP connection, giving up on timeout.
     *
     * @return a connected channel, still in non-blocking mode. The caller closes it
     */
    public static SocketChannel connect(InetSocketAddress addr, int timeoutMs) throws IOException {
        SocketChannel ch = SocketChannel.open();
        try {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);

            if (!ch.connect(addr)) {
                Selector sel = selector();
                SelectionKey key = null;
                try {
                    key = ch.register(sel, SelectionKey.OP_CONNECT);
                    long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
                    while (true) {
                        long remainMs = (deadline - System.nanoTime()) / 1_000_000L;
                        if (remainMs <= 0) {
                            throw new SocketTimeoutException("connect timed out: " + addr);
                        }
                        sel.select(remainMs);
                        boolean ready = sel.selectedKeys().contains(key);
                        sel.selectedKeys().clear();
                        // finishConnect throws ConnectException when the connection was refused
                        if (ready && ch.finishConnect()) {
                            break;
                        }
                    }
                } finally {
                    release(sel, key);
                }
            }
            return ch;
        } catch (IOException | RuntimeException e) {
            closeQuietly(ch);
            throw e;
        }
    }

    /** Writes the whole buffer out, possibly waiting for writability several times. */
    public static void writeFully(SocketChannel ch, ByteBuffer buf, int timeoutMs) throws IOException {
        Selector sel = null;
        SelectionKey key = null;
        try {
            long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
            while (buf.hasRemaining()) {
                if (ch.write(buf) == 0) {
                    // Send buffer is full; register for writability and wait
                    if (key == null) {
                        sel = selector();
                        key = ch.register(sel, SelectionKey.OP_WRITE);
                    }
                    long remainMs = (deadline - System.nanoTime()) / 1_000_000L;
                    if (remainMs <= 0) {
                        throw new SocketTimeoutException("write timed out");
                    }
                    sel.select(remainMs);
                    sel.selectedKeys().clear();
                }
            }
        } finally {
            release(sel, key);
        }
    }

    /** Reads until the buffer is full. */
    private static void readFully(SocketChannel ch, ByteBuffer buf, long deadlineNanos) throws IOException {
        Selector sel = null;
        SelectionKey key = null;
        try {
            while (buf.hasRemaining()) {
                int n = ch.read(buf);
                if (n < 0) {
                    throw new EOFException("the peer closed the connection");
                }
                if (n == 0) {
                    if (key == null) {
                        sel = selector();
                        key = ch.register(sel, SelectionKey.OP_READ);
                    }
                    long remainMs = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                    if (remainMs <= 0) {
                        throw new SocketTimeoutException("read timed out");
                    }
                    sel.select(remainMs);
                    sel.selectedKeys().clear();
                }
            }
        } finally {
            release(sel, key);
        }
    }

    /** Reads and decodes one complete message. */
    public static Message readMessage(SocketChannel ch, int timeoutMs, int maxMessageBytes)
            throws IOException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;

        ByteBuffer header = ByteBuffer.allocate(Codec.HEADER_LEN);
        readFully(ch, header, deadline);
        header.flip();

        int magic = header.getInt();
        if (magic != Codec.MAGIC) {
            throw new IOException("bad message magic: 0x" + Integer.toHexString(magic));
        }
        byte version = header.get();
        if (version != Codec.VERSION) {
            throw new IOException("unsupported protocol version: " + version);
        }
        byte type = header.get();
        int len = header.getInt();
        if (len < 0 || len > maxMessageBytes) {
            throw new IOException("bad message length: " + len);
        }

        ByteBuffer payload = ByteBuffer.allocate(len);
        readFully(ch, payload, deadline);
        return Codec.decode(type, payload.array());
    }

    /** Closes any resource, ignoring failures. Typed on Closeable because a Selector is
     *  Closeable rather than a Channel. */
    public static void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException ignore) {
                // Nothing to do about a failed close
            }
        }
    }
}
