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

import com.chaconneai.spreader.Node;

/**
 * Listener for cluster events. Every method is a default, so override only what you
 * need.
 *
 * <p>Callbacks run serially on a dedicated dispatch thread, so an implementation should
 * avoid blocking for long. Anything expensive belongs on a business thread pool of your
 * own.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface GossipListener {

    /** Called for every event, before the type-specific callback. */
    default void onEvent(ClusterEvent event) {
    }

    /** This node finished starting. */
    default void onSelfStarted(Node self) {
    }

    /** This node stopped. */
    default void onSelfStopped(Node self) {
    }

    /**
     * Discovery is over and this node knows where it belongs.
     *
     * @param alone true when no other node was found, so this one forms a cluster of its
     *              own and becomes the leader automatically
     */
    default void onClusterJoined(Node self, boolean alone) {
    }

    /** A new node joined. */
    default void onNodeJoined(Node node) {
    }

    /**
     * A node departed.
     *
     * @param graceful true when it left on its own terms; false when failure detection
     *                 declared it dead
     */
    default void onNodeLeft(Node node, boolean graceful) {
    }

    /** A node became suspect. */
    default void onNodeSuspect(Node node) {
    }

    /** A suspect node is healthy again. */
    default void onNodeRecovered(Node node) {
    }

    /** A node's metadata was updated. */
    default void onNodeUpdated(Node node, Node previous) {
    }

    /**
     * A node started resting.
     *
     * <p><b>This is not a departure.</b> A resting node remains a full member: still
     * gossiping, still answering probes, still in the leader takeover order, and its
     * {@code state} is still ALIVE. It has merely stepped out of business messaging --
     * others skip it when choosing a target, and it sends nothing outward itself.
     *
     * <p>Usually there is nothing for the application to do. Where there is, the typical
     * case is recomputing something like a per-instance sharding plan, since one usable
     * instance has gone.
     */
    default void onNodeBreak(Node node) {
    }

    /** A resting node came back and takes part in business messaging again. */
    default void onNodeResume(Node node) {
    }

    /**
     * The leader changed.
     *
     * @param previous     the outgoing leader; null the first time a leader is settled
     * @param current      the incoming leader; null while the cluster has none
     * @param selfIsLeader whether this node became the leader
     */
    default void onLeaderChanged(Node previous, Node current, boolean selfIsLeader) {
    }

    /**
     * Leadership fell vacant and the cluster is briefly without one.
     *
     * <p>The node first in the takeover order will now go for the cluster port, and
     * {@link #onLeaderBack} fires once it has it. Work that runs only on the leader
     * should be stopped <i>here</i>, not after a new leader appears.
     *
     * @param node the leader that just left
     */
    default void onLeaderLeft(Node node) {
    }

    /**
     * Leadership is filled again.
     *
     * <p><b>This does not mean the previous node came back.</b> Membership does not track
     * who once led; the one taking over is usually a different member -- whichever is
     * first in the takeover order -- though it may also be the old one, restarted. All
     * the application needs to know is that there is a leader again; the parameter says
     * who.
     *
     * @param node the member now serving as leader
     */
    default void onLeaderBack(Node node) {
    }

    /**
     * A business message arrived from another node.
     *
     * @param sender  who sent it
     * @param content the message body; its meaning is entirely up to the application
     */
    default void onPayload(Node sender, byte[] content) {
    }
}
