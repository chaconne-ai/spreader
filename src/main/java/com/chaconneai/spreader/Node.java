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
package com.chaconneai.spreader;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One node of the cluster. Immutable: state changes derive a new instance through the
 * {@code withXxx} methods, so an instance can be shared and passed between threads
 * freely.
 *
 * <p>The class is {@code final} for that reason -- a subclass could introduce mutable
 * state and break the guarantee everything else relies on.
 *
 * <p>A node's identity is {@link #id()}, a UUID unique to the process, not its address.
 * That way a process restarting on the same machine takes on a wholly new identity and
 * cannot collide with the tombstone record of the instance before it.
 *
 * <p>{@link #name()} is the <b>application name</b> and not an identity -- one
 * application has several instances and they all share a name. Several applications can
 * share a cluster, and the name is what tells them apart, which is what makes "send only
 * to this kind of application" possible.
 *
 * <p>A node implements {@link Comparable}, and the comparison <i>is</i> the leader
 * takeover order (see {@link #compareTo}). So "who should lead" needs no comparator
 * passed in from outside: sort a set of nodes and take the first.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class Node implements Comparable<Node> {

    private final String id;
    private final String name;
    private final String host;
    private final int port;
    private final long startTime;
    private final long incarnation;
    private final NodeState state;
    private final int priority;
    private final boolean clusterPortHolder;
    private final boolean onBreak;
    private final Map<String, String> metadata;

    /** The default application name. Nodes with no name configured all fall into this group. */
    public static final String DEFAULT_NAME = "default";

    public Node(String id, String name, String host, int port, long startTime, long incarnation,
                NodeState state, int priority, Map<String, String> metadata) {
        this(id, name, host, port, startTime, incarnation, state, priority, false, metadata);
    }

    public Node(String id, String name, String host, int port, long startTime, long incarnation,
                NodeState state, int priority, boolean clusterPortHolder,
                Map<String, String> metadata) {
        this(id, name, host, port, startTime, incarnation, state, priority,
                clusterPortHolder, false, metadata);
    }

    public Node(String id, String name, String host, int port, long startTime, long incarnation,
                NodeState state, int priority, boolean clusterPortHolder, boolean onBreak,
                Map<String, String> metadata) {
        this.id = Objects.requireNonNull(id, "id");
        this.name = normalizeName(name);
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.startTime = startTime;
        this.incarnation = incarnation;
        this.state = Objects.requireNonNull(state, "state");
        this.priority = priority;
        this.clusterPortHolder = clusterPortHolder;
        this.onBreak = onBreak;
        this.metadata = metadata == null || metadata.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    /**
     * An identity unique to the process.
     */
    public String id() {
        return id;
    }

    /**
     * The application name; in a Spring Boot project this usually mirrors
     * {@code spring.application.name}.
     *
     * <p><b>Several nodes may share one</b> -- it is a group name saying "this is that
     * kind of application" and carries no uniqueness. To address one specific instance,
     * use {@link #id()}.
     *
     * <p>Unconfigured it is {@link #DEFAULT_NAME}; never null.
     */
    public String name() {
        return name;
    }

    /** Whether this node belongs to the {@code name} group. An empty {@code name} means
     *  no restriction and is always true. */
    public boolean matchesName(String name) {
        return name == null || name.isBlank() || this.name.equals(name.trim());
    }

    private static String normalizeName(String name) {
        return name == null || name.isBlank() ? DEFAULT_NAME : name.trim();
    }

    /**
     * The IP address published to everyone else.
     */
    public String host() {
        return host;
    }

    /**
     * The gossip port published to everyone else.
     */
    public int port() {
        return port;
    }

    /**
     * When the node started, as a millisecond timestamp. Used in leader election: the
     * earlier the start, the higher the precedence.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * The incarnation number. It increments each time a node refutes a misjudgement about
     * itself, and it is what decides which piece of state information is newer.
     */
    public long incarnation() {
        return incarnation;
    }

    public NodeState state() {
        return state;
    }

    /**
     * Election priority: the lower the number, the higher the precedence. Defaults to 0.
     * {@link #startTime()} is only compared when priorities are equal, which makes this
     * the knob for nominating a leader by hand.
     */
    public int priority() {
        return priority;
    }

    /**
     * Whether this node holds the cluster port.
     *
     * <p>The cluster port (22000 by default) can only be held by one node cluster-wide,
     * and whoever holds it is the leader. This flag travels with gossip, so every other
     * node learns who leads instead of working it out for itself.
     */
    public boolean clusterPortHolder() {
        return clusterPortHolder;
    }

    /**
     * Whether this node is <b>resting</b>.
     *
     * <h2>Resting is not leaving</h2>
     * A resting node <b>remains a full member</b>: still gossiping, still answering probes,
     * still in the leader takeover order. Failure detection therefore never declares it
     * failed and it never enters a tombstone period. {@link #state()} is still
     * {@link NodeState#ALIVE}.
     *
     * <p>Its sole effect is that <b>others skip it when choosing a send target</b>. So it
     * takes on no new business messages, while carrying on with its own work and its own
     * sending.
     *
     * <p>It is a flag orthogonal to {@link #state()} rather than a new node state because
     * SWIM's state machine answers "is this node alive" while resting answers "should it be
     * given work". Conflate the two and a resting node gets misjudged as failed.
     */
    public boolean onBreak() {
        return onBreak;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    public String metadata(String key) {
        return metadata.get(key);
    }

    /**
     * The address as a {@code host:port} string.
     */
    public String address() {
        return host + ":" + port;
    }

    public InetSocketAddress socketAddress() {
        return new InetSocketAddress(host, port);
    }

    /**
     * A display string of the form {@code name@host:port}. When several applications
     * share a cluster, an address alone does not say who it is in a log line; this does.
     */
    public String label() {
        return name + "@" + address();
    }

    public Node withState(NodeState newState) {
        return newState == this.state
                ? this
                : new Node(id, name, host, port, startTime, incarnation, newState, priority,
                clusterPortHolder, onBreak, metadata);
    }

    public Node withState(NodeState newState, long newIncarnation) {
        return new Node(id, name, host, port, startTime, newIncarnation, newState, priority,
                clusterPortHolder, onBreak, metadata);
    }

    public Node withIncarnation(long newIncarnation) {
        return new Node(id, name, host, port, startTime, newIncarnation, state, priority,
                clusterPortHolder, onBreak, metadata);
    }

    /**
     * Derives a new instance differing in whether it holds the cluster port.
     *
     * <p>Claiming or releasing the cluster port is a significant change and must raise the
     * incarnation, or this new information would be overwritten by older information still
     * propagating through the network.
     */
    public Node withClusterPort(boolean holding, long newIncarnation) {
        return new Node(id, name, host, port, startTime, newIncarnation, state, priority,
                holding, onBreak, metadata);
    }

    /**
     * Derives a new instance differing in whether it is resting.
     *
     * <p>As with {@link #withClusterPort}, the incarnation must be raised or this new
     * information would be overwritten by older information still propagating -- surfacing
     * as "it went to rest, and a moment later was given work again".
     */
    public Node withBreak(boolean resting, long newIncarnation) {
        return new Node(id, name, host, port, startTime, newIncarnation, state, priority,
                clusterPortHolder, resting, metadata);
    }

    /**
     * Whether this is the same physical endpoint. Equal addresses count as the same
     * endpoint, which may well be a new instance after a restart.
     */
    public boolean sameEndpoint(Node other) {
        return other != null && port == other.port && host.equals(other.host);
    }

    /**
     * The leader takeover order: lower priority first, then earlier start time, and
     * finally the ID as a tie-breaker so the ordering is total and deterministic.
     *
     * <p>Every node in the cluster sorts by these same rules, so all of them reach exactly
     * the same conclusion about who takes over when the leader fails -- with nothing to
     * negotiate.
     */
    @Override
    public int compareTo(Node other) {
        if (priority != other.priority) {
            return Integer.compare(priority, other.priority);
        }
        if (startTime != other.startTime) {
            return Long.compare(startTime, other.startTime);
        }
        return id.compareTo(other.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof Node other && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Node{" + shortId() + " " + label()
                + (onBreak ? ", resting" : "")
                + ", state=" + state
                + ", inc=" + incarnation
                + ", startTime=" + startTime
                + (clusterPortHolder ? ", holds the cluster port" : "")
                + '}';
    }

    /**
     * A shortened ID for log output.
     */
    public String shortId() {
        return id.length() > 8 ? id.substring(0, 8) : id;
    }
}
