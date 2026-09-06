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
package com.chaconneai.spreader.loadbalance;

import com.chaconneai.spreader.Node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Weighted request distribution: the stronger machines take more.
 *
 * <h2>Where the weight comes from</h2>
 * From {@value #WEIGHT_KEY} in the node's metadata, defaulting to
 * {@value #DEFAULT_WEIGHT}. Metadata travels with the member list, so <b>every node
 * already knows everyone else's weight</b> and no separate configuration distribution
 * is required:
 * <pre>{@code
 * GossipConfig.builder()
 *         .metadata(WeightedLoadBalancer.WEIGHT_KEY, "3")   // a strong box; give it more
 *         .build();
 * }</pre>
 *
 * <p>Typical uses: a fleet of mixed machine sizes (8-core and 32-core side by side),
 * canary rollouts (give the new version a weight of 1 and watch), and graceful
 * scale-down (set the weight to 0 and no new requests arrive once the in-flight ones
 * finish).
 *
 * <h2>Smooth weighted round robin, not weighted random</h2>
 * With two nodes at 5:1, weighted random also reaches 5:1 <b>overall</b> -- but it can
 * hit the same node several times in a row. The short-term distribution is lumpy, and
 * in a latency-sensitive service you can feel it.
 *
 * <p>Smooth weighted round robin (the algorithm Nginx uses) produces an interleaved
 * sequence like {@code A A B A A}: the same overall ratio, but any small window is
 * already close to it. The price is a little state -- see {@link #current}.
 *
 * <h2>What happens when every weight is 0</h2>
 * It degenerates to plain round robin. "Everyone weighs 0" is almost always a
 * misconfiguration, and in that case <b>carrying on dispatching</b> beats dispatching
 * to nobody -- the latter would silently stop the entire cluster from working.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class WeightedLoadBalancer implements LoadBalancer {

    /** The metadata key that carries the weight. */
    public static final String WEIGHT_KEY = "spreader.weight";

    /** Assumed when no weight is configured. */
    public static final int DEFAULT_WEIGHT = 1;

    /**
     * Each node's running weight -- the state behind smooth weighted round robin.
     *
     * <p>Keyed by node id. An entry outlives the node that left, which is what
     * {@link #evictStale} is for: without it, a long-lived cluster slowly accumulates
     * nodes that no longer exist.
     */
    private final Map<String, Integer> current = new ConcurrentHashMap<>();

    /** Anything above this is treated as a typo and clamped, so one slipped 999999 cannot starve everyone else. */
    private static final int MAX_WEIGHT = 10_000;

    @Override
    public synchronized Node choose(List<Node> candidates, Object key) {
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        evictStale(candidates);

        int total = 0;
        Node best = null;
        int bestCurrent = Integer.MIN_VALUE;

        for (Node node : candidates) {
            int weight = weightOf(node);
            total += weight;
            // Every round, each node first adds its own weight to its running total
            int now = current.merge(node.id(), weight, Integer::sum);
            if (now > bestCurrent) {
                bestCurrent = now;
                best = node;
            }
        }

        if (total <= 0) {
            // All zero: fall back to round robin, which beats choosing nobody
            return candidates.get(Math.floorMod(tick++, candidates.size()));
        }
        // The winner gives the total back, so the others get their turn next round
        current.merge(best.id(), -total, Integer::sum);
        return best;
    }

    /** Cursor used by the round-robin fallback when every weight is 0. */
    private int tick;

    /**
     * Reads one node's weight.
     *
     * @return 0 or greater. Missing, non-numeric and negative values all fall back to
     *         the default or to 0
     */
    public static int weightOf(Node node) {
        String raw = node.metadata().get(WEIGHT_KEY);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_WEIGHT;
        }
        try {
            int weight = Integer.parseInt(raw.trim());
            // A negative weight is meaningless, so it means 0 (take no new requests).
            // Anything oversized is clamped to the ceiling
            return Math.min(Math.max(weight, 0), MAX_WEIGHT);
        } catch (NumberFormatException e) {
            // A misconfiguration should not halt dispatch; carry on with the default
            return DEFAULT_WEIGHT;
        }
    }

    /** Drops nodes no longer among the candidates, so the state cannot grow without bound. */
    private void evictStale(List<Node> candidates) {
        if (current.size() <= candidates.size()) {
            return;
        }
        current.keySet().removeIf(id -> candidates.stream().noneMatch(n -> n.id().equals(id)));
    }

    @Override
    public String toString() {
        return "WeightedLoadBalancer";
    }
}
