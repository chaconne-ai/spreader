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
package com.chaconneai.spreader.event;

/**
 * The kinds of cluster event.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum ClusterEventType {

    /** This node finished starting: it is listening, but discovery has not completed yet. */
    SELF_STARTED,

    /** This node has stopped. */
    SELF_STOPPED,

    /** Discovery is over and this node has joined -- either an existing cluster, or one
     *  consisting of itself. */
    CLUSTER_JOINED,

    /** A new node joined. */
    NODE_JOINED,

    /** A node left gracefully: it called stop() on itself. */
    NODE_LEFT,

    /** A node was declared failed. */
    NODE_DEAD,

    /** A node became suspect. */
    NODE_SUSPECT,

    /** A suspect node recovered. */
    NODE_RECOVERED,

    /** A node's metadata changed. */
    NODE_UPDATED,

    /** The leader changed. */
    LEADER_CHANGED,

    /** The leadership fell vacant; the cluster is temporarily without one. */
    LEADER_LEFT,

    /** Leadership is filled again -- not necessarily by the node that held it before. */
    LEADER_BACK,

    /** A business message arrived from another node. */
    PAYLOAD_RECEIVED,

    /**
     * A node started resting and will not send or receive business messages for now.
     *
     * <p>It remains a full member -- still gossiping, still answering probes, still in
     * the leader takeover order. It merely stops being chosen as a target for business
     * messages. <b>Do not treat this as a departure.</b>
     */
    NODE_BREAK,

    /** A resting node came back and resumed sending and receiving business messages. */
    NODE_RESUME
}
