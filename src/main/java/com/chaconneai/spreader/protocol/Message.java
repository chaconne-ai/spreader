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

import com.chaconneai.spreader.Node;

import java.util.List;

/**
 * A message travelling through the cluster.
 *
 * <p>The interface exists to separate "the transport moves bytes" from "this is what a
 * message looks like". Both {@link com.chaconneai.spreader.transport.Transport} and
 * {@link Codec} know only this interface, so the protocol can evolve without disturbing
 * the transport layer, and none of the four transport implementations (built-in NIO,
 * Netty, MINA, Grizzly) has to know about {@link GossipMessage}.
 *
 * <p>This is a <b>read-only view</b>: every field is immutable, and messages are built
 * through {@link GossipMessage#builder(MessageType, String)}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface Message {

    /** What kind of message this is. */
    MessageType type();

    /**
     * The cluster the sender belongs to.
     *
     * <p>Every message carries it. A node with a different cluster name is turned away
     * even when the network can reach it, and that is how several clusters stay isolated
     * on one network.
     */
    String clusterName();

    /** The sender's own latest node information. May be null -- an error response sent
     *  before this node is ready, for instance. */
    Node sender();

    /** The member list riding along with the message. This is how gossip propagates. */
    List<Node> members();

    /**
     * A monotonically increasing message sequence number.
     *
     * <p>The transport uses it to match responses to requests, and to discard stale
     * responses that arrive late.
     */
    long seq();

    /** Whether the peer handled it successfully. */
    boolean ok();

    /** Why {@link #ok()} is false; the empty string when all is well. */
    String errorMessage();

    /** Target node id for an indirect probe; the empty string on every other message. */
    String targetId();

    /** Target host for an indirect probe; the empty string on every other message. */
    String targetHost();

    /** Target port for an indirect probe; 0 on every other message. */
    int targetPort();

    /** Channel name for a business message; the empty string is the default channel.
     *  Only PAYLOAD-family messages use it. */
    String channel();

    /** The business payload. Carried only by {@link MessageType#PAYLOAD} messages;
     *  an empty array on every other type. */
    byte[] content();
}
