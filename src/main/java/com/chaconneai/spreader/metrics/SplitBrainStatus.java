package com.chaconneai.spreader.metrics;

import java.util.List;

/**
 * The result of the split-brain self-check.
 *
 * <h2>Why it is still reported after healing</h2>
 * The yielding mechanism converges the cluster back to a single leader on its own, so
 * a split is usually <b>over in a flash</b>: it happened, it healed, and all that is
 * left is one log line saying this node gave up leadership. Nobody knows it ever
 * occurred, let alone how often.
 *
 * <p>Yet during a split <b>both leaders are doing leader work</b> -- both granting
 * locks, both coordinating cache writes. Data consistency has no guarantee for that
 * window. Even if it lasted only a few hundred milliseconds, someone ought to know
 * that the data from that moment may be suspect.
 *
 * <p>So it is turned into a number a monitoring system can <b>see and alert on</b>:
 * {@link #occurrences()} only ever increases, and anything above zero means this
 * cluster has had two leaders at once.
 *
 * <h2>One self-check covers both single-machine and multi-machine</h2>
 * The test is a single question -- how many members claim to hold the cluster port --
 * and it has nothing to do with where those nodes run.
 *
 * <p>What differs is <b>the likelihood</b>. On one machine the OS guarantees port
 * exclusion, so two leaders can only come from a bug. Across machines every host can
 * bind its own port, exclusion rests entirely on "scan at startup plus timing", and
 * simultaneous starts are an ordinary race. <b>But detection and healing must be one
 * mechanism</b>: split them in two and the single-machine one never gets exercised.
 *
 * @param healthy     whether things are healthy right now, i.e. at most one holder
 * @param holders     the cluster-port holders <b>as this node sees them</b>. More than
 *                    one means a split is in progress
 * @param leader      the leader address according to this node; null when there is none
 * @param occurrences how many splits have been detected since startup. Monotonic --
 *                    non-zero means it happened, even if all is well now
 * @param lastDetectedAt timestamp of the most recent detection; 0 if it never happened
 * @param splittingSince when <b>the current</b> split began; 0 when things are healthy.
 *                       Reset on healing, so the next split is timed afresh
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public record SplitBrainStatus(
        boolean healthy,
        List<String> holders,
        String leader,
        long occurrences,
        long lastDetectedAt,
        long splittingSince) {

    /** The state of a cluster that has never had a problem. */
    public static SplitBrainStatus healthy(String leader, long occurrences, long lastDetectedAt) {
        return new SplitBrainStatus(true,
                leader == null ? List.of() : List.of(leader),
                leader, occurrences, lastDetectedAt, 0L);
    }

    /**
     * How long the current split has lasted, in milliseconds; 0 when healthy.
     *
     * <h2>Why the duration matters more than the yes/no</h2>
     * A split <b>happening</b> is not necessarily a problem: two nodes starting at
     * almost the same moment will collide, and the yielding mechanism converges within
     * a gossip round -- a few hundred milliseconds, start to finish. Declaring the whole
     * cluster unhealthy for that would have the orchestrator kill a pile of nodes that
     * were working perfectly well.
     *
     * <p>What matters is <b>failing to heal</b>. Minutes without convergence means the
     * yielding mechanism is not working -- one-way network failure, a wedged node, a
     * misconfiguration -- and that is when human intervention is worth something.
     *
     * <p>So this reports the fact and leaves "how long is too long" to the layer above.
     * That is an operational policy question and does not belong hard-coded in the
     * protocol.
     */
    public long splittingDurationMillis() {
        return splittingSince == 0L ? 0L : System.currentTimeMillis() - splittingSince;
    }

    /** A split is in progress right now. */
    public boolean splitting() {
        return !healthy;
    }

    /** A split has occurred at some point, healed or not. <b>This is the one to alert on.</b> */
    public boolean everSplit() {
        return occurrences > 0;
    }

    @Override
    public String toString() {
        if (splitting()) {
            return "SPLIT BRAIN: the cluster port is held by " + holders.size()
                    + " nodes at once: " + holders + " (" + occurrences + " occurrence(s) so far)";
        }
        return everSplit()
                ? "healthy now, leader=" + leader + ", but " + occurrences
                  + " split-brain occurrence(s) since startup"
                : "healthy, leader=" + leader;
    }
}
