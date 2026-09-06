package com.chaconneai.spreader.loadbalance;

import com.chaconneai.spreader.Node;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Consistent hashing: one key lands on the same member every time, and <b>only a small
 * fraction of keys move when membership changes</b>.
 *
 * <h2>How it differs from {@link HashLoadBalancer}</h2>
 * That one is plain modulo ({@code hash(key) % memberCount}). It also gets one key onto
 * one member consistently -- but the moment the member count changes, <b>most keys
 * change owner</b>: going from 5 nodes to 6 moves roughly 83% of them.
 *
 * <p>For cache affinity that is the worst possible behaviour: add one machine and
 * nearly every local cache in the cluster is invalidated at once, putting the backend
 * under full load instantly. Consistent hashing holds the disturbance to about
 * {@code 1/N} -- 5 to 6 moves roughly one key in six and leaves the rest alone.
 *
 * <p>So: <b>use this one for cache affinity</b>. {@link HashLoadBalancer} only suits
 * cases where stability is wanted but migration costs nothing.
 *
 * <h2>Virtual nodes</h2>
 * Putting each node at a single point on the ring distributes terribly -- with three
 * nodes, one of them may well own half the ring. Each physical node therefore gets
 * {@value #DEFAULT_VIRTUAL_NODES} virtual nodes, which smooths it out. The number is
 * empirical: too few is uneven, too many only spends memory and lookup time.
 *
 * <h2>Weights come along for free</h2>
 * Virtual node count = base x weight, the weight coming from node metadata (see
 * {@link WeightedLoadBalancer#WEIGHT_KEY}). A node weighted 3 occupies three times the
 * arc and naturally receives three times the keys. Unconfigured means 1, which behaves
 * exactly as if weights were not involved.
 *
 * <h2>The ring is rebuilt only when it has to be</h2>
 * Rebuilding on every call would put that cost on every request. Instead the previous
 * membership is remembered and the ring is rebuilt only when <b>membership actually
 * changed</b> -- a rare event in a stable cluster.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ConsistentHashLoadBalancer implements LoadBalancer {

    /** How many virtual nodes each physical node gets on the ring, at weight 1. */
    public static final int DEFAULT_VIRTUAL_NODES = 160;

    private final int virtualNodes;

    /** The hash ring: key is the position on it, value is the physical node sitting there. */
    private volatile TreeMap<Long, Node> ring = new TreeMap<>();

    /** Membership as of the last rebuild. Used to decide whether a rebuild is needed at all. */
    private volatile String signature = "";

    public ConsistentHashLoadBalancer() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashLoadBalancer(int virtualNodes) {
        this.virtualNodes = Math.max(1, virtualNodes);
    }

    @Override
    public Node choose(List<Node> candidates, Object key) {
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            // The biggest saving here: with one candidate the answer is a foregone
            // conclusion, while the path below builds the whole ring --
            // DEFAULT_VIRTUAL_NODES virtual nodes, one MD5 each.
            // Single-node clusters (local development, the first canary instance,
            // the last one left after scale-down) are a very common shape
            return candidates.get(0);
        }
        if (key == null) {
            // With no routing key there is no such thing as a stable landing spot, so
            // this degenerates to random. Forcing a fixed answer (always the first, say)
            // would be worse: every keyless call would pile onto one node
            return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
        }

        TreeMap<Long, Node> currentRing = ringFor(candidates);
        if (currentRing.isEmpty()) {
            // Every node weighs 0. Almost certainly a misconfiguration, and dispatching
            // anyway beats choosing nobody
            return candidates.get(Math.floorMod(key.hashCode(), candidates.size()));
        }

        long hash = hash(String.valueOf(key));
        // Walk clockwise to the first position at or past the hash; wrap to the start if there is none
        SortedMap<Long, Node> tail = currentRing.tailMap(hash);
        return tail.isEmpty() ? currentRing.get(currentRing.firstKey()) : tail.get(tail.firstKey());
    }

    /** Reuses the existing ring when membership has not changed. */
    private TreeMap<Long, Node> ringFor(List<Node> candidates) {
        String current = signatureOf(candidates);
        if (current.equals(signature)) {
            return ring;
        }
        synchronized (this) {
            // Double-check: a second thread arriving concurrently should not rebuild it again
            if (current.equals(signature)) {
                return ring;
            }
            TreeMap<Long, Node> rebuilt = build(candidates);
            ring = rebuilt;
            signature = current;
            return rebuilt;
        }
    }

    /**
     * A fingerprint of the membership.
     *
     * <p>It <b>sorts first</b>, and that matters: the same set of members may be ordered
     * differently on different nodes. Without the sort, each node would compute a
     * different fingerprint, build a different ring, and route the same key to different
     * members -- the stickiness would be gone outright.
     */
    private String signatureOf(List<Node> candidates) {
        List<String> ids = new ArrayList<>(candidates.size());
        for (Node n : candidates) {
            ids.add(n.id() + ':' + WeightedLoadBalancer.weightOf(n));
        }
        ids.sort(null);
        return String.join(",", ids);
    }

    private TreeMap<Long, Node> build(List<Node> candidates) {
        TreeMap<Long, Node> rebuilt = new TreeMap<>();
        for (Node node : candidates) {
            int weight = WeightedLoadBalancer.weightOf(node);
            if (weight <= 0) {
                // A node weighing 0 never goes on the ring and so receives no keys.
                // This is what makes graceful scale-down work
                continue;
            }
            int points = virtualNodes * weight;
            for (int i = 0; i < points; i++) {
                // Node id rather than host:port. A restart may change the port, and while
                // a changed id should redistribute, a changed address alone should not
                rebuilt.put(hash(node.id() + '#' + i), node);
            }
        }
        return rebuilt;
    }

    /**
     * The first 8 bytes of an MD5 digest.
     *
     * <p>Not {@code String.hashCode()}: its low bits are distributed poorly, so adjacent
     * strings produce adjacent values, the virtual nodes bunch together and the ring
     * comes out lopsided. Nothing here is security-sensitive -- MD5 is used purely as a
     * well-distributed hash function.
     */
    private static long hash(String value) {
        try {
            // MessageDigest is not thread-safe, so a fresh one each time. It is cheap,
            // and used only when building the ring or doing a lookup -- never in the
            // inner loop of a hot path
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(value.getBytes(StandardCharsets.UTF_8));
            long result = 0;
            for (int i = 0; i < 8; i++) {
                result = (result << 8) | (digest[i] & 0xFFL);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            // Every JDK is required to provide MD5, so this is unreachable
            throw new IllegalStateException("this JVM does not provide MD5", e);
        }
    }

    /** How many virtual nodes are on the ring. For diagnosing distribution problems. */
    public int ringSize() {
        return ring.size();
    }

    /** How many virtual nodes each physical node ended up with. For diagnosing distribution problems. */
    public Map<String, Integer> distribution() {
        Map<String, Integer> counts = new TreeMap<>();
        for (Node node : ring.values()) {
            counts.merge(node.id(), 1, Integer::sum);
        }
        return counts;
    }

    @Override
    public String toString() {
        return "ConsistentHashLoadBalancer{virtualNodes=" + virtualNodes + '}';
    }
}
