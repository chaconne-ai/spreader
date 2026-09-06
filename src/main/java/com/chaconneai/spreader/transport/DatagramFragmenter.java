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
package com.chaconneai.spreader.transport;

import com.chaconneai.spreader.protocol.Codec;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UDP fragmentation and reassembly.
 *
 * <h2>Why it is needed</h2>
 * A UDP datagram holds at most 65507 bytes, and with a few hundred members the member
 * list alone can burst that -- cache snapshots and task dispatch arguments exceed it far
 * more easily. Without this layer, an oversized message simply <b>could not be sent</b>:
 * the old behaviour was to throw an IOException and tell the caller to use TCP.
 *
 * <p>With it, message size over UDP is bounded by {@code maxMessageBytes}, exactly as
 * over TCP.
 *
 * <h2>Frame format</h2>
 * Anything that fits in one datagram is <b>sent as is</b>, with not a single byte of
 * overhead -- which is the path the overwhelming majority of gossip messages take. Only
 * an oversized frame gets a fragment header:
 * <pre>
 * +--------+---------+-------+--------+---------+---------+----------+
 * | magic  | version | flags | msgId  | fragIdx | fragCnt | frag data |
 * | 4 byte | 1 byte  | 1 byte| 8 byte |  2 byte |  2 byte |          |
 * +--------+---------+-------+--------+---------+---------+----------+
 * </pre>
 * The magic number differs from {@link Codec#MAGIC}, so the receiving side knows which
 * path to take from the first four bytes, with no negotiation needed.
 *
 * <h2>Lose a fragment, lose the message</h2>
 * There is no retransmission and no NAK. The reasoning is the same as for the transport
 * not retransmitting at all: gossip beats packet loss by repeating periodically, and
 * business messages have acks and retries above them ({@code payloadAck}). All this
 * guarantees is that <b>no partial message is ever handed upward</b> -- a reassembly
 * that has not completed within {@link #REASSEMBLY_TIMEOUT_MS} is discarded whole.
 *
 * <p>The more fragments, the likelier the whole message is lost (any one of n going
 * missing loses all of it), so this is not a licence to send anything you like over UDP
 * now that large messages work. Several megabytes still belong on TCP. What it fixes is
 * the cliff-edge failure of "slightly over the limit, therefore cannot be sent at
 * all".
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class DatagramFragmenter {

    /** Magic number for fragment frames, "GSFG", kept distinct from {@link Codec#MAGIC}. */
    public static final int FRAG_MAGIC = 0x47534647;

    /** Fragmentation protocol version. */
    public static final byte FRAG_VERSION = 1;

    /** Length of the fragment header. */
    public static final int FRAG_HEADER_LEN = 18;

    /** Reassembly timeout in milliseconds. Past it, the whole message is discarded. */
    static final long REASSEMBLY_TIMEOUT_MS = 5_000L;

    /**
     * Ceiling on reassemblies running at once.
     *
     * <p>It guards against a malicious or malfunctioning peer that sends only first
     * fragments and never the rest, each one holding memory until it times out. At the
     * ceiling, new reassemblies are simply refused; those already under way are
     * unaffected.
     */
    static final int MAX_CONCURRENT_REASSEMBLIES = 512;

    /** The most fragments one message may be split into. fragCnt is an unsigned 16-bit
     *  field, and this leaves headroom below it. */
    static final int MAX_FRAGMENTS = 60_000;

    private final int maxDatagramBytes;
    private final int maxMessageBytes;
    private final Logger log;

    /**
     * msgId starts from a random value rather than 0.
     *
     * <p>Reassembly is keyed on source address plus msgId, and a node that restarts may
     * well keep the same port. Starting from 0, the new process's fragments would collide
     * with the half-finished reassemblies its predecessor left behind at the peer.
     */
    private final AtomicLong msgIdGen = new AtomicLong(ThreadLocalRandom.current().nextLong());

    private final Map<Key, Reassembly> inflight = new ConcurrentHashMap<>();

    /** When expired reassemblies were last swept. Sweeping is triggered lazily; there is
     *  no dedicated thread. */
    private volatile long lastSweepMs = System.currentTimeMillis();

    public DatagramFragmenter(int maxDatagramBytes, int maxMessageBytes, Logger log) {
        this.maxDatagramBytes = maxDatagramBytes;
        this.maxMessageBytes = maxMessageBytes;
        this.log = log;
    }

    /** How much fragment data fits in one datagram. */
    public int payloadPerFragment() {
        return maxDatagramBytes - FRAG_HEADER_LEN;
    }

    // ------------------------------------------------------------------
    // Sending side
    // ------------------------------------------------------------------

    /**
     * Splits a complete frame into datagrams ready to be sent.
     *
     * @param frame a complete frame, flipped for reading
     * @return for a frame within the limit, a single-element list holding the original
     *         buffer -- no copy and no overhead; otherwise the fragments
     * @throws IOException when the frame is too large even to fragment
     */
    public List<ByteBuffer> split(ByteBuffer frame) throws IOException {
        int total = frame.remaining();
        if (total <= maxDatagramBytes) {
            return List.of(frame);
        }
        if (total > maxMessageBytes) {
            throw new IOException("message is " + total + " bytes, over maxMessageBytes "
                    + maxMessageBytes);
        }

        int per = payloadPerFragment();
        int count = (total + per - 1) / per;
        if (count > MAX_FRAGMENTS) {
            throw new IOException("a " + total + "-byte message would need " + count
                    + " fragments, over the limit of " + MAX_FRAGMENTS
                    + "; use the TCP transport for volumes like this");
        }

        long msgId = msgIdGen.incrementAndGet();
        List<ByteBuffer> out = new ArrayList<>(count);
        int offset = frame.position();
        for (int i = 0; i < count; i++) {
            int len = Math.min(per, total - i * per);
            ByteBuffer buf = ByteBuffer.allocate(FRAG_HEADER_LEN + len);
            buf.putInt(FRAG_MAGIC);
            buf.put(FRAG_VERSION);
            buf.put((byte) 0);                     // flags, reserved for later use
            buf.putLong(msgId);
            buf.putShort((short) i);
            buf.putShort((short) count);
            // Slice off a duplicate, leaving the original buffer's position untouched
            ByteBuffer slice = frame.duplicate();
            slice.position(offset + i * per).limit(offset + i * per + len);
            buf.put(slice);
            buf.flip();
            out.add(buf);
        }
        log.debug("A {}-byte message exceeds the {}-byte datagram limit; sending it as {} fragments",
                total, maxDatagramBytes, count);
        return out;
    }

    // ------------------------------------------------------------------
    // Receiving side
    // ------------------------------------------------------------------

    /**
     * Handles one received datagram.
     *
     * @param datagram flipped for reading
     * @param from     the source address, which together with msgId keys the reassembly
     * @return a complete frame ready for {@link Frames#decode}, or null when the message
     *         is not complete yet -- or this fragment was discarded
     */
    public ByteBuffer reassemble(ByteBuffer datagram, InetSocketAddress from) {
        if (!isFragment(datagram)) {
            return datagram;
        }
        sweepIfDue();

        datagram.getInt();                          // magic; isFragment already checked it
        byte version = datagram.get();
        if (version != FRAG_VERSION) {
            log.debug("Discarding a fragment from {}: unrecognised protocol version {}", from, version);
            return null;
        }
        datagram.get();                             // flags
        long msgId = datagram.getLong();
        int index = datagram.getShort() & 0xFFFF;
        int count = datagram.getShort() & 0xFFFF;

        if (count <= 0 || index >= count) {
            log.debug("Discarding a fragment from {}: bad index {}/{}", from, index, count);
            return null;
        }
        int per = payloadPerFragment();
        if ((long) count * per > maxMessageBytes + (long) per) {
            log.debug("Discarding a fragment from {}: {} fragments declared, which would "
                    + "reassemble past maxMessageBytes", from, count);
            return null;
        }

        byte[] data = new byte[datagram.remaining()];
        datagram.get(data);

        Key key = new Key(from, msgId);
        Reassembly r = inflight.get(key);
        if (r == null) {
            if (inflight.size() >= MAX_CONCURRENT_REASSEMBLIES) {
                log.warn("At the ceiling of {} concurrent reassemblies; discarding a new fragment from {}",
                        MAX_CONCURRENT_REASSEMBLIES, from);
                return null;
            }
            Reassembly created = new Reassembly(count);
            r = inflight.putIfAbsent(key, created);
            if (r == null) {
                r = created;
            }
        }

        byte[] complete = r.accept(index, count, data);
        if (complete == null) {
            return null;
        }
        inflight.remove(key);
        return ByteBuffer.wrap(complete);
    }

    /** Whether the first four bytes are the fragment magic. Leaves the buffer's position alone. */
    public static boolean isFragment(ByteBuffer buf) {
        return buf.remaining() >= FRAG_HEADER_LEN
                && buf.getInt(buf.position()) == FRAG_MAGIC;
    }

    /** Clears out incomplete reassemblies. Triggered lazily, and at most once a second. */
    private void sweepIfDue() {
        long now = System.currentTimeMillis();
        if (now - lastSweepMs < 1_000L || inflight.isEmpty()) {
            return;
        }
        lastSweepMs = now;
        int dropped = 0;
        for (Iterator<Map.Entry<Key, Reassembly>> it = inflight.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Key, Reassembly> e = it.next();
            if (now - e.getValue().createdMs > REASSEMBLY_TIMEOUT_MS) {
                it.remove();
                dropped++;
            }
        }
        if (dropped > 0) {
            log.debug("Discarded {} incomplete fragmented message(s) -- fragments were lost in transit", dropped);
        }
    }

    /** How many messages are being reassembled right now. For diagnostics and tests. */
    public int inflightCount() {
        return inflight.size();
    }

    // ------------------------------------------------------------------

    /** The reassembly key: a msgId is unique only within one source address. */
    private record Key(InetSocketAddress from, long msgId) {
    }

    /** One message being reassembled. Fragments may arrive out of order, and may arrive twice. */
    private static final class Reassembly {

        private final long createdMs = System.currentTimeMillis();
        private final byte[][] parts;
        private int received;

        Reassembly(int count) {
            this.parts = new byte[count][];
        }

        /**
         * Accepts one fragment.
         *
         * @return the assembled frame once every fragment has arrived, otherwise null
         */
        synchronized byte[] accept(int index, int count, byte[] data) {
            if (count != parts.length || index >= parts.length) {
                // One msgId declaring two different fragment counts means either a msgId
                // collision or a tampered message; drop the whole thing
                return null;
            }
            if (parts[index] != null) {
                return null;                        // arrived twice; ignore
            }
            parts[index] = data;
            if (++received < parts.length) {
                return null;
            }
            int total = 0;
            for (byte[] p : parts) {
                total += p.length;
            }
            byte[] out = new byte[total];
            int offset = 0;
            for (byte[] p : parts) {
                System.arraycopy(p, 0, out, offset, p.length);
                offset += p.length;
            }
            return out;
        }
    }
}
