package com.chaconneai.spreader.protocol;

import com.chaconneai.spreader.Node;

import java.util.List;
import java.util.Objects;

/**
 * A gossip message. Every field is immutable, so an instance can be passed between
 * threads freely once constructed.
 *
 * <p>The class is {@code final} for that reason: a subclass could introduce mutable
 * state and break the guarantee the rest of the system relies on.
 *
 * <p>{@code clusterName} is checked on every message: a node with a different cluster
 * name is turned away even when the network can reach it, which is how several clusters
 * stay isolated on one network.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class GossipMessage implements Message {

    private static final byte[] NO_CONTENT = new byte[0];

    private final MessageType type;
    private final String clusterName;
    private final Node sender;
    private final List<Node> members;
    private final long seq;
    private final boolean ok;
    private final String errorMessage;
    private final String targetId;
    private final String targetHost;
    private final int targetPort;
    private final byte[] content;

    /** Channel name for a business message; the empty string is the default channel.
     *  Only PAYLOAD-family messages use it. */
    private final String channel;

    private GossipMessage(Builder b) {
        this.type = Objects.requireNonNull(b.type, "type");
        this.clusterName = Objects.requireNonNull(b.clusterName, "clusterName");
        this.sender = b.sender;
        this.members = b.members == null ? List.of() : List.copyOf(b.members);
        this.seq = b.seq;
        this.ok = b.ok;
        this.errorMessage = b.errorMessage == null ? "" : b.errorMessage;
        this.targetId = b.targetId == null ? "" : b.targetId;
        this.targetHost = b.targetHost == null ? "" : b.targetHost;
        this.targetPort = b.targetPort;
        this.content = b.content == null || b.content.length == 0
                ? NO_CONTENT : b.content.clone();
        this.channel = b.channel == null ? "" : b.channel;
    }

    public static Builder builder(MessageType type, String clusterName) {
        return new Builder(type, clusterName);
    }

    @Override
    public MessageType type() {
        return type;
    }

    /** The cluster the sender belongs to. */
    @Override
    public String clusterName() {
        return clusterName;
    }

    /** The sender's own latest node information. */
    @Override
    public Node sender() {
        return sender;
    }

    /**
     * The business payload. What comes back is a copy of the internal array, so the
     * caller may modify it freely. Only {@link MessageType#PAYLOAD} messages carry
     * content; every other type always yields an empty array.
     */
    @Override
    public byte[] content() {
        return content.length == 0 ? content : content.clone();
    }

    /** Channel name for a business message; the empty string is the default channel. */
    @Override
    public String channel() {
        return channel;
    }

    public int contentLength() {
        return content.length;
    }

    /** The membership view riding along -- possibly complete, possibly just the changes. */
    @Override
    public List<Node> members() {
        return members;
    }

    @Override
    public long seq() {
        return seq;
    }

    /** The outcome carried by a response message. */
    @Override
    public boolean ok() {
        return ok;
    }

    @Override
    public String errorMessage() {
        return errorMessage;
    }

    /** In a PING_REQ, the id of the node to be probed indirectly. */
    @Override
    public String targetId() {
        return targetId;
    }

    @Override
    public String targetHost() {
        return targetHost;
    }

    @Override
    public int targetPort() {
        return targetPort;
    }

    @Override
    public String toString() {
        return "GossipMessage{" + type
                + ", cluster=" + clusterName
                + ", from=" + (sender == null ? "?" : sender.shortId())
                + ", members=" + members.size()
                + ", ok=" + ok
                + '}';
    }

    /** Builder for messages. */
    public static final class Builder {
        private final MessageType type;
        private final String clusterName;
        private Node sender;
        private List<Node> members;
        private long seq;
        private boolean ok = true;
        private String errorMessage;
        private String targetId;
        private String targetHost;
        private int targetPort;
        private byte[] content;
        private String channel = "";

        private Builder(MessageType type, String clusterName) {
            this.type = type;
            this.clusterName = clusterName;
        }

        public Builder sender(Node sender) {
            this.sender = sender;
            return this;
        }

        public Builder members(List<Node> members) {
            this.members = members;
            return this;
        }

        public Builder seq(long seq) {
            this.seq = seq;
            return this;
        }

        public Builder ok(boolean ok) {
            this.ok = ok;
            return this;
        }

        public Builder error(String errorMessage) {
            this.ok = false;
            this.errorMessage = errorMessage;
            return this;
        }

        /** Sets the channel name. Meaningful only for PAYLOAD-family messages. */
        public Builder channel(String channel) {
            this.channel = channel == null ? "" : channel;
            return this;
        }

        public Builder content(byte[] content) {
            this.content = content;
            return this;
        }

        public Builder target(String id, String host, int port) {
            this.targetId = id;
            this.targetHost = host;
            this.targetPort = port;
            return this;
        }

        public GossipMessage build() {
            return new GossipMessage(this);
        }
    }
}
