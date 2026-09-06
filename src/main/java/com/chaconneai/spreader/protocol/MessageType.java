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
package com.chaconneai.spreader.protocol;

/**
 * The kinds of gossip message.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum MessageType {

    /** Liveness probe, carrying the sender's membership view. */
    PING((byte) 1),

    /** Response to a probe, carrying the responder's membership view. */
    ACK((byte) 2),

    /** Indirect probe: asks the recipient to PING a target on the sender's behalf --
     *  SWIM's indirect probe. */
    PING_REQ((byte) 3),

    /** Request to join the cluster. */
    JOIN((byte) 4),

    /** Response to a join, carrying the full member list. */
    JOIN_ACK((byte) 5),

    /** Membership sync: an active push of a change event. */
    SYNC((byte) 6),

    /** Notice of a graceful departure. */
    LEAVE((byte) 7),

    /** The lightweight discovery handshake. It verifies cluster identity only and
     *  produces no membership change. */
    PROBE((byte) 8),

    /** Response to the discovery handshake. */
    PROBE_ACK((byte) 9),

    /** A business message: application-defined content spread through the gossip
     *  channel. The recipient replies with an ACK. */
    PAYLOAD((byte) 12),

    /**
     * A one-way business message: the recipient <b>sends nothing back</b>.
     *
     * <p>It needs its own type rather than reusing {@link #PAYLOAD} because connections
     * are pooled. Were the recipient still to reply with an ACK that the sender never
     * reads, that ACK would sit on the connection and the <i>next</i> request would read
     * it as its own response. "Do not wait for an answer" has to be agreed by both ends;
     * the sender cannot decide it unilaterally.
     */
    PAYLOAD_ONEWAY((byte) 13);

    private final byte code;

    MessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /**
     * Whether this is a <b>response</b>.
     *
     * <h2>Why the transport layer has to know</h2>
     * A {@code seq} comes from the <b>sender's own</b> counter and is unique only on that
     * side -- it was never globally unique. A response carries back the request's
     * {@code seq} unchanged, so matching responses by it works. But a message the peer
     * sends <b>on its own initiative</b> (PING, SYNC, PAYLOAD and friends) carries the
     * <i>peer's</i> {@code seq}, drawn from a range that overlaps this node's completely.
     *
     * <p>All three framework UDP implementations (Netty, MINA, Grizzly) share one receive
     * callback and one in-flight request table across every socket. So "on receipt, look
     * the seq up in the in-flight table" would let <b>a request from the peer be consumed
     * as one's own response</b>: the requester gets a message that makes no sense, the
     * real response goes unclaimed when it arrives, and the peer's request vanishes
     * without trace. Right after a cluster starts, every node's seq is climbing from near
     * zero in step, so collisions are anything but rare.
     *
     * <p>The symptoms are <b>intermittent</b> -- "timed out waiting for the leader",
     * "wrote a value and read back null" -- and appear only over UDP. The built-in NIO
     * implementation opens a temporary socket per request, which isolates responses
     * naturally, so it stayed green throughout; TCP separates by connection and never
     * hits it either. That false signal was misleading for a long time.
     *
     * <p>With this test in place, claiming an in-flight request requires the message to
     * be a response type, and request-type messages always go through normal inbound
     * dispatch.
     */
    public boolean isResponse() {
        return this == ACK || this == JOIN_ACK || this == PROBE_ACK;
    }

    public static MessageType fromCode(byte code) {
        for (MessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("unknown message type code: " + code);
    }
}
