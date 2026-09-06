package com.chaconneai.spreader;

import com.chaconneai.spreader.event.GossipListener;
import com.chaconneai.spreader.loadbalance.LoadBalancer;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.SplitBrainStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A cluster node.
 *
 * <h2>How it works</h2>
 * <ol>
 *   <li><b>Start</b>: bind the work port and begin listening.</li>
 *   <li><b>Find the door</b>: knock on the cluster port (22000 by default) of each
 *       configured host. One answer is enough -- the full member list comes back from it,
 *       and gossip converges the rest.</li>
 *   <li><b>Claim the door</b>: if nobody answers, the cluster has no nodes yet, so claim
 *       the cluster port and become the leader. Only one process can hold a port, and that
 *       exclusion <i>is</i> the election result.</li>
 *   <li><b>Exchange</b>: from then on, each gossip round picks a few members at random and
 *       exchanges membership views with them directly. <b>There is no central node</b> --
 *       the leader is merely "whoever holds that port", and communication stays
 *       point-to-point.</li>
 *   <li><b>Notice</b>: a failed probe leads to an indirect probe through others; failing
 *       again marks the node SUSPECT; timing out declares it departed. A node misjudged
 *       this way increments its incarnation and broadcasts a refutation, correcting itself
 *       automatically -- SWIM's standard mechanism.</li>
 *   <li><b>Take over</b>: when a leader departs the cluster port falls vacant, and the node
 *       first in the takeover order claims it. That order comes from
 *       {@link Node#compareTo}, every node computes the same answer, and nothing is
 *       negotiated.</li>
 * </ol>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * GossipCluster cluster = GossipCluster.create(GossipConfig.builder()
 *         .clusterName("order-cluster")
 *         .clusterPort(22000)
 *         .ipAddresses("192.168.0.111", "192.168.0.63")
 *         .build());
 *
 * cluster.addListener(new GossipListener() {
 *     public void onNodeJoined(Node node) { ... }
 *     public void onNodeLeft(Node node, boolean graceful) { ... }
 *     public void onLeaderChanged(Node previous, Node current, boolean selfIsLeader) { ... }
 * });
 *
 * cluster.start();
 * }</pre>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface GossipCluster extends AutoCloseable {

    /** Creates a cluster node from a configuration. It is not started yet. */
    static GossipCluster create(GossipConfig config) {
        return new DefaultGossipCluster(config);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /**
     * Starts the node. It returns quickly: binding the port happens synchronously, while
     * the slow part -- discovery -- runs in the background. To wait for discovery before
     * continuing, use {@link #awaitJoin}.
     *
     * @throws IOException when the work port cannot be bound
     */
    void start() throws IOException;

    /**
     * Runs a round of discovery: knock on the cluster port, join whoever answers, and
     * claim it if nobody does.
     *
     * <p>Called automatically at startup and triggered as needed by a scheduled task
     * thereafter. Applications rarely need to call it, unless they know for certain that
     * the topology has just changed and want to converge immediately.
     */
    void probe();

    /**
     * Shuts down gracefully: release the cluster port, broadcast a departure notice to
     * every known member, then close local resources. That way the others learn of it
     * without waiting out the failure detection timeout.
     */
    void stop();

    @Override
    default void close() {
        stop();
    }

    /** Waits for discovery to finish, whether it joined an existing cluster or formed one alone. */
    boolean awaitJoin(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Waits for a leader to be settled for the first time.
     *
     * <p>An application branching on "am I the leader" at startup should wait for this
     * signal rather than {@link #awaitJoin}, or it may read an intermediate state from the
     * startup race.
     */
    boolean awaitLeader(long timeout, TimeUnit unit) throws InterruptedException;

    boolean isRunning();

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    /** This node's information. */
    Node self();

    /** Every effective member right now, this node included, in takeover order. */
    List<Node> members();

    /**
     * The members under one application name, this node included, in takeover order.
     *
     * <p>Several applications can share a cluster, and this filters out "every instance of
     * the order service".
     *
     * @param name the application name; null or an empty string is equivalent to
     *             {@link #members()}
     */
    List<Node> membersOf(String name);

    /** Every entry right now, including departed nodes in their tombstone period. For diagnostics. */
    List<Node> allEntries();

    /**
     * The current leader -- the node holding the cluster port.
     * Returns null during the gap after a leader departs and before a new one has claimed
     * the port.
     */
    Node leader();

    /** Whether this node is the leader. */
    boolean isLeader();

    String clusterName();

    GossipConfig config();

    // ------------------------------------------------------------------
    // Operations
    // ------------------------------------------------------------------

    /** Updates this node's metadata; the change gossips out to the whole cluster. */
    void updateMetadata(Map<String, String> metadata);

    /**
     * Puts this node <b>to rest for a while</b>, or brings it back: it steps out of
     * business messaging without leaving the cluster.
     *
     * <h2>Resting is not leaving</h2>
     * That is the fundamental difference from {@link #stop()}. A resting node <b>remains a
     * full member</b>:
     *
     * <ul>
     *   <li>it gossips and answers probes as usual -- so failure detection <b>never</b>
     *       declares it dead and it never enters a tombstone period. {@link Node#state()}
     *       is still {@code ALIVE}</li>
     *   <li>it stays in the leader takeover order, and if it was the leader it <b>remains
     *       the leader</b> (leadership follows the cluster port, which has nothing to do
     *       with resting)</li>
     *   <li>cluster events still arrive; every {@link GossipListener} callback fires</li>
     *   <li>it is <b>still visible</b> in {@link #members()} and {@link #membersOf}, merely
     *       with {@link Node#onBreak()} true -- operators need to see who is resting</li>
     * </ul>
     *
     * <h2>What changes is <b>your own</b> business messaging, in both directions</h2>
     * <ul>
     *   <li><b>Others stop sending to it</b>: multicast, unicast and the load balancer all
     *       skip a resting node when choosing targets</li>
     *   <li><b>It stops sending outward</b>: while resting, {@code multicast} and
     *       {@code unicast} simply report that nothing went out, and no network is used</li>
     * </ul>
     *
     * <h2>Components built on top are unaffected, deliberately</h2>
     * The test is the <b>channel name</b>: anything beginning with {@code spreader.} is a
     * system channel and resting does not touch it.
     *
     * <table border="1">
     *   <caption>Who is affected</caption>
     *   <tr><th>Channel</th><th>Still sending and receiving while resting?</th></tr>
     *   <tr><td>the default channel -- your {@code multicast} and {@code unicast}</td>
     *       <td><b>Stopped</b>, both ways</td></tr>
     *   <tr><td>channels you named yourself, such as {@code "orders"}</td>
     *       <td><b>Stopped</b>, both ways</td></tr>
     *   <tr><td>{@code spreader.cache}, {@code spreader.mutex},
     *       {@code spreader.pool} and the rest</td><td><b>As usual</b></td></tr>
     *   <tr><td>gossip, probing, membership sync</td>
     *       <td><b>As usual</b>, or the node would be declared failed</td></tr>
     * </table>
     *
     * <p>The cache row especially must not change: replicas keep up by multicast, and
     * excluding a resting node would freeze its replica where it stands -- so it would
     * <b>come back holding stale data</b>. Locks and semaphores likewise: stop renewing the
     * leases and the locks it holds get released by mistake.
     *
     * <p><b>Whether a component declines to give work to a resting node is that component's
     * own decision.</b> Only RPC does so today ({@code RpcService} filters on
     * {@link Node#onBreak()} itself), because it knows which messages are new requests and
     * which are responses that must go back -- a distinction the transport layer cannot
     * make.
     *
     * <p>The state gossips out to the whole cluster, and other nodes are notified through
     * {@link GossipListener#onNodeBreak} and {@link GossipListener#onNodeResume}.
     *
     * <p><b>Idempotent</b> when the state does not change; nothing is broadcast twice.
     *
     * <p>The typical use: an instance about to do slow local maintenance -- warming a
     * cache, rebuilding an index, forcing a full GC -- that wants no new work meanwhile but
     * must not actually leave. Actually leaving would trigger a takeover and replica
     * migration, and it would have to rejoin afterwards: far more expensive.
     *
     * @param resting {@code true} to begin resting, {@code false} to finish
     */
    void takeBreak(boolean resting);

    /** Whether this node is currently resting. */
    boolean isOnBreak();

    // ------------------------------------------------------------------
    // Observability
    // ------------------------------------------------------------------

    /**
     * Observability data grouped by channel, keyed by channel name -- the empty string
     * being the user's default channel.
     *
     * <h2>Why it is split by channel</h2>
     * Several kinds of traffic run in a cluster at once: business messages on the default
     * channel, cache replication on {@code spreader.cache}, locks on
     * {@code spreader.mutex}, RPC on {@code spreader.rpc}. Their volumes and latency
     * profiles are nothing alike -- lumped together, the high-frequency small packets of
     * cache replication dilute RPC's P99 until the problem disappears, and a spike in one
     * channel's error rate drowns in the total.
     *
     * <p>Per channel you get: throughput (TPS, with peaks), concurrency (current and
     * peak), the latency distribution (min/avg/max/P50/P95/P99), error rate, retry rate,
     * and duplicate count. Outbound and inbound are separate: slow outbound may be the peer
     * or the network, slow inbound is definitely your own problem, and watching only one
     * sends the investigation in the wrong direction.
     *
     * <p>What comes back is an <b>immutable snapshot</b>; it will not change however long
     * you hold it.
     *
     * <p>Returns an empty map when observability is off
     * ({@code GossipConfig.metricsEnabled(false)}).
     *
     * @see ChannelMetrics
     */
    Map<String, ChannelMetrics> metrics();

    /** A snapshot of one channel. A channel with no traffic yields an all-zero snapshot
     *  rather than null. */
    ChannelMetrics metrics(String channel);

    /**
     * The split-brain self-check: how many nodes hold the cluster port right now, and how
     * many times it has happened since startup.
     *
     * <h2>Why it is still reported after healing</h2>
     * The yielding mechanism converges the cluster back to a single leader on its own, so a
     * split is usually <b>over in a flash</b> -- it happened, it healed, and no trace is
     * left. Yet during it <b>both leaders are doing leader work</b>, granting locks and
     * coordinating cache writes, and data consistency has no guarantee for that window.
     * Even at a few hundred milliseconds, someone ought to know that the data from that
     * moment may be suspect.
     *
     * <p>{@link SplitBrainStatus#occurrences()} only ever increases, and anything above
     * zero means this cluster has had two leaders at once. <b>Alert on it.</b>
     *
     * <h2>One self-check covers both single-machine and multi-machine</h2>
     * The test is a single question -- how many members claim to hold the cluster port --
     * and it has nothing to do with where those nodes run. What differs is <b>the
     * likelihood</b>: on one machine the operating system guarantees port exclusion, while
     * across machines every host can bind its own and exclusion rests entirely on "scan at
     * startup plus timing".
     *
     * <p>There is one case nobody can detect: during a network partition neither side can
     * see the other, and each believes there is exactly one leader. That is what CAP
     * dictates and no implementation escapes it; the moment the partition heals it becomes
     * visible and converges.
     */
    SplitBrainStatus splitBrainStatus();

    /**
     * The fill level of every inbound buffer (RingBuffer): queued, capacity, dropped.
     *
     * <h2>Why this deserves more attention than latency</h2>
     * Latency and error rate describe problems that have <b>already happened</b>; buffer
     * level describes one that is <b>on its way</b>. A level sitting high means consumption
     * is not keeping up with production, and it is one traffic spike away from dropping.
     *
     * <p>And when a buffer fills, new messages are <b>discarded outright</b> -- backpressure
     * is not an option, since stalling the receive thread would stop gossip heartbeats and
     * get this node declared failed instead. So the dropping is by design, but it happens
     * <b>silently</b>: neither the sender nor the receiving application learns of it. A
     * non-zero {@link BufferMetrics#dropped()} means real, lost work.
     *
     * <p>Covers the event dispatcher's own queue and every buffered listener -- cache,
     * mutex, semaphore, latch, barrier, task dispatch and RPC each have one.
     */
    List<BufferMetrics> bufferMetrics();

    /**
     * Resets every channel's statistics to zero.
     *
     * <p>For collection styles that report "since the last read". <b>Do not call it</b> with
     * Prometheus: its model is a monotonically increasing counter whose rate the query side
     * computes, and a reset partway through makes {@code rate()} spike negative.
     */
    void resetMetrics();

    /**
     * <b>Multicast</b>: sends a business message to every other member, with content the
     * application defines. It arrives through {@link GossipListener#onPayload}.
     *
     * <p>Sent in parallel, so the total time depends on the slowest member rather than the
     * member count.
     *
     * <h2>Delivery guarantees follow {@link GossipConfig#payloadAck()}</h2>
     * <ul>
     *   <li><b>Acknowledged, the default</b> -- wait for the peer's ACK and retransmit when
     *       it does not come, at most {@link GossipConfig#payloadRetries()} times, then
     *       discard. The receiver de-duplicates on sender plus sequence number, so a
     *       retransmission never reaches the application twice.</li>
     *   <li><b>Unacknowledged</b> -- sent is done: no waiting and no retransmission. Fast,
     *       but whether the peer received it is unknowable.</li>
     * </ul>
     *
     * <h2>This node is excluded by default</h2>
     * The overloads without an {@code includeSelf} parameter send <b>only to other
     * members</b>. At the moment of sending, this node already has the message in hand --
     * whatever it needs to do it can simply do, without the message coming back round to
     * it. "I have handled it; the others need telling" is the shape of the overwhelming
     * majority of cases.
     *
     * <p>To include this node, use the {@code includeSelf = true} overload. Requirements
     * like "every instance runs this once" -- refreshing a local cache, reloading
     * configuration -- read most directly that way, with no separate handling of this
     * node's own share at the call site. That share is exactly what gets forgotten.
     *
     * <p>Sending to this node <b>does not use the network</b>: it dispatches straight to
     * the local listeners, which saves a loopback trip and is not turned away by
     * business-message de-duplication.
     *
     * @return how many members it reached. Unacknowledged, the number means only that it
     *         was written to the network successfully
     */
    int multicast(byte[] content);

    /**
     * <b>Multicast</b> to every member, optionally including this node.
     *
     * @param includeSelf whether to deliver to this node as well. Doing so <b>does not use
     *                    the network</b>: it dispatches straight to the local listeners,
     *                    which saves a loopback trip and is not turned away by
     *                    business-message de-duplication
     */
    int multicast(byte[] content, boolean includeSelf);

    /**
     * <b>Multicast</b> to every instance of one application name.
     *
     * <p>It is named {@code multicastTo} rather than being another {@code multicast}
     * overload so that it cannot be confused with the routing-key parameters, and the wrong
     * overload cannot be picked by accident.
     *
     * @param name the application name; null or an empty string degenerates to
     *             {@link #multicast(byte[])}
     * @return how many members it reached; 0 when that application has no instances
     */
    int multicastTo(String name, byte[] content);

    /** <b>Multicast</b> to the instances of one application name, optionally including this node. */
    int multicastTo(String name, byte[] content, boolean includeSelf);

    /**
     * <b>Unicast</b> to one specific member.
     *
     * @return whether it was delivered
     */
    boolean unicast(Node target, byte[] content);

    /**
     * <b>Unicast</b> to one of the other members, chosen by the {@link LoadBalancer}.
     * Round robin by default; configurable.
     *
     * @return who it actually went to, or null when there are no other members or the send
     *         failed
     */
    Node unicast(byte[] content);

    /** <b>Unicast</b> to a load-balancer's choice, optionally with this node among the candidates. */
    Node unicast(byte[] content, boolean includeSelf);

    /**
     * <b>Unicast</b> with a routing key. Together with
     * {@link LoadBalancer#hash()}
     * it makes one key land on the same member every time.
     *
     * @return who it actually went to, or null when there are no other members or the send
     *         failed
     */
    Node unicast(Object key, byte[] content);

    /** <b>Unicast</b> with a routing key, optionally with this node among the candidates. */
    Node unicast(Object key, byte[] content, boolean includeSelf);

    /**
     * <b>Unicast</b> to one instance of a given application name.
     *
     * <p>The typical case is "give this task to <b>one</b> order service" -- there are
     * several instances under that name, and the {@link LoadBalancer} decides which.
     *
     * @param name the application name; null or an empty string picks among all members
     * @return who it actually went to, or null when that application has no usable instance
     *         or the send failed
     */
    Node unicastTo(String name, byte[] content);

    /** <b>Unicast</b> to one instance of an application name, optionally with this node
     *  among the candidates. */
    Node unicastTo(String name, byte[] content, boolean includeSelf);

    /**
     * <b>Unicast</b> to one instance of a given application name, chosen by routing key.
     *
     * <p>With {@link LoadBalancer#hash()}, one key lands on the same instance every time,
     * and redistribution happens only when that application's instances come or go.
     *
     * @param name the application name; null or an empty string picks among all members
     * @param key  the routing key; may be null
     * @return who it actually went to, or null when that application has no usable instance
     *         or the send failed
     */
    Node unicastTo(String name, Object key, byte[] content);

    /** <b>Unicast</b> by application name and routing key, optionally with this node among
     *  the candidates. */
    Node unicastTo(String name, Object key, byte[] content, boolean includeSelf);

    /** Subscribes to business messages on the default channel, and to every cluster event. */
    void addListener(GossipListener listener);

    /**
     * Subscribes to business messages on <b>one channel</b>, and to every cluster event.
     *
     * <p>Channels keep business messages of different purposes apart: a listener subscribed
     * to {@code "mutex"} receives nothing from the default channel, and vice versa. A
     * toolkit built on spreader should use a channel of its own, so its internal traffic
     * does not pour into the application's {@code onPayload}.
     *
     * <p>Cluster events -- joins, departures, leader changes -- have nothing to do with
     * channels and reach every listener.
     */
    void addListener(String channel, GossipListener listener);

    /** Multicasts on one channel. Only listeners subscribed to it receive the message. */
    int multicastOn(String channel, String name, byte[] content);

    /** Multicasts on one channel, optionally including this node. */
    int multicastOn(String channel, String name, byte[] content, boolean includeSelf);

    /** Unicasts on one channel, with the load balancer choosing the target. */
    Node unicastOn(String channel, String name, Object key, byte[] content);

    /** Unicasts on one channel, optionally with this node among the candidates. */
    Node unicastOn(String channel, String name, Object key, byte[] content, boolean includeSelf);

    /**
     * <b>Unicast</b> to a member chosen by a load-balancing strategy <b>given for this
     * call</b>.
     *
     * <h2>This is the base of every unicast</h2>
     * The dozen or so other {@code unicast} / {@code unicastTo} / {@code unicastOn}
     * overloads are shorthands for it and all end up here, simply passing {@code null} for
     * {@code balancer}. So <b>the overloads without a balancer behave exactly as they
     * always did</b>.
     *
     * <h2>Why a per-call strategy is needed</h2>
     * {@link GossipConfig#loadBalancer()} is <b>one instance shared by the whole node</b>,
     * while different stages of one piece of work may want opposite strategies:
     *
     * <table border="1">
     *   <caption>Two stages within one component</caption>
     *   <tr><th>Stage</th><th>What it needs</th><th>Which to use</th></tr>
     *   <tr><td>spreading task shards out</td><td>every node gets a share</td>
     *       <td>{@link LoadBalancer#roundRobin()}</td></tr>
     *   <tr><td>gathering intermediate results by key</td>
     *       <td>one key must land on one node</td>
     *       <td>{@link LoadBalancer#consistentHash()}</td></tr>
     * </table>
     *
     * <p>Use consistent hashing for the sharding stage and five shards across three nodes
     * may well pile onto one machine with the other two idle; use round robin for the
     * gathering stage and one key scatters everywhere, so nothing gathers at all. One
     * global setting cannot serve both, which is why it can be passed per call.
     *
     * <h2>Hold the strategy instance yourself; do not construct one at the call site</h2>
     * {@link LoadBalancer#roundRobin()}、{@link LoadBalancer#weighted()}、
     * {@link LoadBalancer#consistentHash()} -- these three <b>carry state</b>, and every
     * call to the factory method produces a wholly new instance:
     *
     * <pre>{@code
     * // Wrong: a new one each time, so the round-robin cursor never leaves 0 --
     * // every message goes to the same machine
     * for (byte[] shard : shards) {
     *     cluster.unicastOn(ch, null, i, shard, true, LoadBalancer.roundRobin());
     * }
     *
     * // Right: one instance shared across the loop
     * LoadBalancer lb = LoadBalancer.roundRobin();
     * for (byte[] shard : shards) {
     *     cluster.unicastOn(ch, null, i, shard, true, lb);
     * }
     * }</pre>
     *
     * <p>The overloads without a {@code balancer} do not have this problem -- they use the
     * one from {@code GossipConfig}, built once at configuration time. The trap belongs
     * specifically to <b>passing a strategy per call</b>; see
     * {@link LoadBalancer#consistentHash()} for the details.
     *
     * @param channel     the channel name
     * @param name        the application name; null or an empty string picks among all members
     * @param key         the routing key; may be null, and only hash-style strategies use it
     * @param content     the message body
     * @param includeSelf whether this node is among the candidates. <b>Choosing this node
     *                    dispatches locally</b> -- no network, no serialisation, and no
     *                    space taken in the inbound buffer
     * @param balancer    the strategy for this call; <b>null uses
     *                    {@link GossipConfig#loadBalancer()}</b>
     * @return who it actually went to. <b>Null has four possible causes</b> and the caller
     *         cannot tell them apart: this node is resting, there were no candidates, the
     *         strategy declined, or one was chosen and the send failed. To distinguish
     *         "could not choose" from "could not send", pick with {@link #membersOf}
     *         yourself and then call {@link #unicastOn(String, Node, byte[])}
     */
    Node unicastOn(String channel, String name, Object key, byte[] content,
                   boolean includeSelf, LoadBalancer balancer);

    /** Unicasts on one channel to one specific member. */
    boolean unicastOn(String channel, Node target, byte[] content);

    /**
     * <b>Sends to the leader.</b> When this node is the leader it dispatches locally,
     * without touching the network.
     *
     * <p>For cases where something must be handled in one place: reporting in, registering,
     * requesting quota. There is no need to write
     * {@code isLeader() ? local : unicast(leader(), ...)} yourself -- that has two traps,
     * both below.
     *
     * <h2>Trap one: "am I the leader" is {@link #isLeader()}, not {@link #leader()}</h2>
     * The two answer from <b>different evidence</b>:
     *
     * <table border="1">
     *   <caption>How the two differ</caption>
     *   <tr><th>Method</th><th>Evidence</th><th>When it lags</th></tr>
     *   <tr><td>{@link #isLeader()}</td><td><b>actual possession of the cluster port</b></td>
     *       <td>Never -- holding the port is a fact</td></tr>
     *   <tr><td>{@link #leader()}</td><td>the local membership view</td>
     *       <td>Yes -- the view travels by gossip, and after a handover it takes a round or
     *           two to update</td></tr>
     * </table>
     *
     * <p>So a node that has just claimed the port already returns true from
     * {@code isLeader()}, while {@code leader()} in its own view may still point at the
     * outgoing one. Deciding with {@code leader().equals(self())} therefore <b>sends the
     * message to a node that has already stepped down</b>.
     *
     * <h2>Trap two: {@code unicastOn(channel, self, ...)} silently discards</h2>
     * Should the target work out to be this node, calling unicast directly <b>fails without
     * a sound</b> -- no exception, no log line, merely false. This method handles that
     * internally.
     *
     * <h2>The window with no leader</h2>
     * For the few seconds after a leader departs and before a new one has claimed the port,
     * {@link #leader()} returns null and this method <b>returns false</b>. That window was
     * measured at roughly 6 seconds in containers.
     *
     * <p>Important messages should be retried -- but do not sit waiting here. The leader
     * <b>changes</b>, so every retry should call this method afresh rather than cache the
     * target from last time.
     *
     * <h2>What it cannot guarantee</h2>
     * Between establishing who leads and the message actually arriving, <b>the leader may
     * already have changed</b>. That is an inherent race in distributed systems and no API
     * eliminates it -- this method only guarantees sending on the most reliable judgement
     * available <b>at this instant</b>. For a stronger guarantee the receiver has to check
     * for itself, for instance by carrying a term number and refusing anything not from the
     * current leader.
     *
     * @return whether it was delivered. False has three possible causes: there is no
     *         leader, this node is resting, or the send failed
     */
    boolean sendToLeader(byte[] content);

    /** <b>Sends to the leader</b> on one channel. Semantics as in {@link #sendToLeader(byte[])}. */
    boolean sendToLeaderOn(String channel, byte[] content);

    void removeListener(GossipListener listener);
}
