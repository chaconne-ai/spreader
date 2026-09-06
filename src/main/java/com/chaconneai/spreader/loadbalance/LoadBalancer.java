package com.chaconneai.spreader.loadbalance;

import com.chaconneai.spreader.Node;

import java.util.List;

/**
 * The strategy for choosing which member a unicast goes to.
 *
 * <p>Members are peers, so a business message could go to any of them. That leaves a
 * real choice to make, and this interface hands it to the caller.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@FunctionalInterface
public interface LoadBalancer {

    /**
     * Picks one of the candidates.
     *
     * @param candidates the candidates, with this node already excluded. The caller
     *                   guarantees it is non-empty
     * @param key        routing key, may be null. Only relevant when one key has to land
     *                   on the same node every time
     * @return the chosen member, or null to abandon this send
     */
    Node choose(List<Node> candidates, Object key);

    /**
     * Round robin, the default. Spreads requests evenly across members.
     *
     * <p><b>Build it once and keep it; do not new one up per call.</b> The cursor is
     * <b>instance state</b>. A fresh instance starts its cursor at 0 every time, so
     * {@code choose} keeps returning the first candidate and the whole thing degenerates
     * into "always send to the same machine". Nothing throws and nothing warns -- the
     * traffic simply stops being balanced. See the table under
     * {@link #consistentHash()}.
     */
    static LoadBalancer roundRobin() {
        return new RoundRobinLoadBalancer();
    }

    /**
     * Random. Less to think about than round robin under concurrency, and comparable in
     * distribution.
     *
     * <p><b>Stateless</b>, so constructing one wherever you like has no side effects --
     * unlike {@link #roundRobin()}, {@link #weighted()} and
     * {@link #consistentHash()}.
     */
    static LoadBalancer random() {
        return new RandomLoadBalancer();
    }

    /**
     * Hashes the routing key with plain modulo. One key always lands on one member.
     *
     * <p><b>Change the member count and most keys move</b>: going from 5 nodes to 6
     * redistributes roughly 83% of them. For cache affinity use
     * {@link #consistentHash()}. This one only suits cases where stability is wanted
     * but migration costs nothing.
     *
     * <p>A null key degenerates to random.
     *
     * <p><b>Stateless</b>, so constructing one wherever you like has no side effects.
     */
    static LoadBalancer hash() {
        return new HashLoadBalancer();
    }

    /**
     * Consistent hashing: one key lands on the same member every time, <b>and only about
     * 1/N of the keys move when membership changes</b>.
     *
     * <p>This is the one to reach for when you want cache affinity. With plain modulo
     * ({@link #hash()}), adding a single machine invalidates nearly every local cache in
     * the cluster at once and the backend takes the full load; consistent hashing
     * disturbs only a small slice.
     *
     * <p>Weights come along for free: the virtual-node count scales with
     * {@link WeightedLoadBalancer#WEIGHT_KEY} from the node's metadata, defaulting to 1.
     *
     * <h2>These three must be held as instances, never constructed at the call site</h2>
     * {@link #roundRobin()}, {@link #weighted()} and this one all <b>carry state</b>, and
     * each breaks differently when that state is thrown away:
     *
     * <table border="1">
     *   <caption>What a fresh instance per call costs you</caption>
     *   <tr><th>Strategy</th><th>What the state is</th><th>Consequence</th></tr>
     *   <tr><td>{@link #roundRobin()}</td><td>the cursor</td>
     *       <td><b>Always returns the first candidate</b>; no rotation at all</td></tr>
     *   <tr><td>{@link #weighted()}</td><td>each node's running weight</td>
     *       <td>Smooth weighting stops working; degenerates to a fixed choice</td></tr>
     *   <tr><td>Consistent hash</td><td>the virtual-node ring plus a candidate-set
     *       signature</td>
     *       <td>Still correct, but <b>rebuilds the entire ring on every call</b>:
     *           {@value ConsistentHashLoadBalancer#DEFAULT_VIRTUAL_NODES}
     *           x node count MD5 computations</td></tr>
     * </table>
     *
     * <p>The first two are <b>correctness</b> problems and the third is a
     * <b>performance</b> one. What they share is that <b>none of them throws</b>.
     *
     * <pre>{@code
     * // Wrong: a new one per shard, so the cursor never leaves 0
     * for (Shard s : shards) {
     *     cluster.unicastOn(ch, null, s.index(), s.bytes(), true, LoadBalancer.roundRobin());
     * }
     *
     * // Right: one instance shared across the loop
     * LoadBalancer lb = LoadBalancer.roundRobin();
     * for (Shard s : shards) {
     *     cluster.unicastOn(ch, null, s.index(), s.bytes(), true, lb);
     * }
     * }</pre>
     *
     * <p>The one handed to {@code GossipConfig} is immune by construction -- it is built
     * once, at configuration time. The call sites to watch are the ones that <b>pass a
     * strategy per invocation</b>.
     */
    static LoadBalancer consistentHash() {
        return new ConsistentHashLoadBalancer();
    }

    /**
     * Weighted distribution: the stronger machines take more.
     *
     * <p>The weight is an <b>optional</b> entry in a node's metadata, defaulting to
     * {@value WeightedLoadBalancer#DEFAULT_WEIGHT} -- which means that with nothing
     * configured anywhere, this is equivalent to round robin:
     * <pre>{@code
     * GossipConfig.builder()
     *         .loadBalancer(LoadBalancer.weighted())
     *         .metadata(WeightedLoadBalancer.WEIGHT_KEY, "3")   // only on the nodes that need it
     *         .build();
     * }</pre>
     *
     * <p>Metadata travels with the member list, so every node already knows everyone
     * else's weight and no separate configuration distribution is needed. Changing a
     * weight means editing that node's configuration and restarting it; the others pick
     * up the new value on the next gossip round.
     *
     * <p>Typical uses: a fleet of mixed machine sizes, canary rollouts (give the new
     * version a low weight first), and graceful scale-down (set the weight to 0 and no
     * new work arrives once the in-flight work drains).
     *
     * <p><b>Stateful -- build it once and keep it.</b> Smooth weighting depends on the
     * per-node running weights held in the instance, and a fresh one per call
     * degenerates to a fixed choice. See the table under {@link #consistentHash()}.
     */
    static LoadBalancer weighted() {
        return new WeightedLoadBalancer();
    }
}
