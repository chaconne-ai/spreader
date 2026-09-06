package com.chaconneai.spreader.transport;

import com.chaconneai.spreader.protocol.Codec;
import com.chaconneai.spreader.protocol.Message;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Frame encoding and validation, shared by every transport implementation.
 *
 * <p>{@link Codec} handles turning a message body into bytes; this handles what a frame
 * looks like and whether the bytes received form a valid one. It is a separate layer
 * because the four implementations (built-in NIO, Netty, MINA, Grizzly) each use their
 * own framework's mechanism to <b>split frames</b> -- but the validation and parsing
 * that follow must be one and the same, or the byte streams they produce would stop
 * being interoperable and they could no longer share a cluster for comparative
 * benchmarking.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class Frames {

    /**
     * The theoretical ceiling for a single UDP datagram: 65535 minus an 8-byte UDP
     * header and a 20-byte IP header.
     *
     * <p>Messages larger than this are fragmented by {@link DatagramFragmenter}, so this
     * is no longer a ceiling on message size -- only on how much fits in one datagram.
     */
    public static final int MAX_DATAGRAM_BYTES = 65_507;

    /**
     * Size of the UDP socket send and receive buffers, used identically by all four
     * implementations.
     *
     * <h2>Why it has to be set explicitly</h2>
     * UDP does not retransmit: once the kernel's receive queue fills, further datagrams
     * are <b>discarded outright, with no notification to either end</b>. What surfaces is
     * an inexplicable timeout higher up -- "waited 15 seconds for replication and it
     * never came" -- and a long investigation before anyone realises the packets simply
     * never arrived.
     *
     * <p>The defaults differ wildly between systems: macOS sets
     * {@code net.inet.udp.recvspace} to 768KB, while Linux's {@code rmem_default} is
     * commonly only 208KB. Left unset, the same code absorbs bursts more than three times
     * better on one platform than the other -- and <b>nobody would ever think to suspect
     * it</b>.
     *
     * <p>This trap was walked into for real. The Netty implementation once set
     * {@code 4 * MAX_DATAGRAM_BYTES} (256KB), intended as tuning, which was in fact
     * <b>three times smaller</b> than the macOS default. NETTY/UDP then dropped packets
     * intermittently in the matrix, while NIO/UDP -- which set no option and inherited
     * the system's 768KB -- stayed green throughout.
     *
     * <p>2MB was chosen: roughly 32 full datagrams, leaving ample burst headroom. Note
     * that anything above {@code kern.ipc.maxsockbuf} (macOS) or {@code rmem_max} (Linux)
     * is silently clamped by the kernel with no error, so genuinely saturating the link
     * means tuning the system parameters too.
     */
    public static final int SOCKET_BUFFER_BYTES = 2 * 1024 * 1024;

    private Frames() {
    }

    /** Encodes a message into one complete frame. */
    public static ByteBuffer encode(Message msg) throws IOException {
        return Codec.encode(msg);
    }

    /**
     * Parses one complete frame.
     *
     * @param buf a buffer flipped for reading that holds a <b>complete</b> frame
     * @throws IOException if the magic number, the version or the length is wrong
     */
    public static Message decode(ByteBuffer buf, int maxMessageBytes) throws IOException {
        if (buf.remaining() < Codec.HEADER_LEN) {
            throw new IOException("incomplete message: only " + buf.remaining() + " byte(s)");
        }
        int magic = buf.getInt();
        if (magic != Codec.MAGIC) {
            throw new IOException("bad message magic: 0x" + Integer.toHexString(magic));
        }
        byte version = buf.get();
        if (version != Codec.VERSION) {
            throw new IOException("unsupported protocol version: " + version);
        }
        byte type = buf.get();
        int len = buf.getInt();
        if (len < 0 || len > maxMessageBytes) {
            throw new IOException("bad message length: " + len);
        }
        if (buf.remaining() < len) {
            // Over UDP this usually means MTU truncation; over TCP it means the layer
            // above split the frame wrongly
            throw new IOException("incomplete message body: declared " + len
                    + " byte(s), got " + buf.remaining());
        }
        byte[] payload = new byte[len];
        buf.get(payload);
        return Codec.decode(type, payload);
    }
}
