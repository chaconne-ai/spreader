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

import java.util.List;

/**
 * Something that happened in the cluster.
 *
 * @param type         what kind of event this is
 * @param node         the node the event is about; for
 *                     {@link ClusterEventType#LEADER_CHANGED} this is the incoming
 *                     leader, which may be null
 * @param previous     the node as it was before; for LEADER_CHANGED, the outgoing
 *                     leader, which may be null
 * @param members      snapshot of the full member list as of just after the event
 * @param leader       the leader as of just after the event
 * @param selfIsLeader whether this node is currently the leader
 * @param graceful     for departure events, whether the peer left on its own terms
 * @param content      the business payload; carried only by
 *                     {@link ClusterEventType#PAYLOAD_RECEIVED}
 * @param channel      channel the message arrived on; meaningful only for
 *                     PAYLOAD_RECEIVED. An empty string means the default channel
 * @param timestamp    when the event was produced
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record ClusterEvent(
        ClusterEventType type,
        Node node,
        Node previous,
        List<Node> members,
        Node leader,
        boolean selfIsLeader,
        boolean graceful,
        byte[] content,
        String channel,
        long timestamp) {

    private static final byte[] NO_CONTENT = new byte[0];

    public ClusterEvent {
        members = members == null ? List.of() : members;
        content = content == null ? NO_CONTENT : content;
        channel = channel == null ? "" : channel;
    }

    /** Builds an event that carries no business payload. */
    public static ClusterEvent of(ClusterEventType type, Node node, Node previous,
                                  List<Node> members, Node leader, boolean selfIsLeader,
                                  boolean graceful) {
        return new ClusterEvent(type, node, previous, members, leader, selfIsLeader,
                graceful, NO_CONTENT, "", System.currentTimeMillis());
    }

    /** Builds an event for one inbound business message. */
    public static ClusterEvent payload(Node sender, String channel, byte[] content,
                                       List<Node> members, Node leader, boolean selfIsLeader) {
        return new ClusterEvent(ClusterEventType.PAYLOAD_RECEIVED, sender, null, members,
                leader, selfIsLeader, false, content, channel, System.currentTimeMillis());
    }

    @Override
    public String toString() {
        return "ClusterEvent{" + type
                + (node == null ? "" : ", node=" + node.shortId() + "@" + node.address())
                + ", members=" + members.size()
                + ", leader=" + (leader == null ? "none" : leader.address())
                + ", selfIsLeader=" + selfIsLeader
                + (content.length == 0 ? "" : ", content=" + content.length + "B")
                + '}';
    }
}
