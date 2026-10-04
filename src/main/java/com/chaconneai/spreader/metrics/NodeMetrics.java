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
package com.chaconneai.spreader.metrics;

import java.util.Comparator;
import java.util.List;

/**
 * A complete monitoring snapshot of one node, <b>ready to serialise straight to JSON</b>.
 *
 * <h2>Why this layer exists rather than handing out spreader's Map directly</h2>
 * An external system rendering a dashboard needs more than the per-channel message counts
 * -- it needs to know <b>whose data this is</b>: which node, in what role, how large the
 * cluster is, whether it is resting. Without those, data gathered from several nodes cannot
 * be told apart, and "why does this node have no traffic" has no answer (it may simply be
 * resting rather than dead).
 *
 * <p>So node identity, a cluster summary and the channel metrics are packed here into
 * <b>one self-contained bundle</b>: fetch it once and it can be rendered, with nothing to
 * piece together from other endpoints.
 *
 * <p>Records and primitives throughout, so any JSON library serialises it directly, with no
 * dependence on Jackson annotations.
 *
 * <h2>The two top-level metric objects, and why they are separate</h2>
 * This and {@link ChannelMetrics} are the whole of what spreader exposes: this one is the
 * <b>macroscopic</b> view of an instance, that one the <b>microscopic</b> view of a single
 * channel. They are measured <b>independently</b>, and this record deliberately holds no
 * channel map at all.
 *
 * <p>The reason is that an instance-level figure summed from per-channel figures is not an
 * instance-level figure. Adding up the channels mixes the application's own traffic with the
 * framework's (the {@code spreader.} prefixed channels that the cache, the locks and RPC use
 * for themselves), and the total then answers neither question. Whoever wants the breakdown
 * asks {@link com.chaconneai.spreader.GossipCluster#metrics()}; whoever wants the instance
 * asks here.
 *
 * <p>What does hang off this record are the things that genuinely belong to the node rather
 * than to any one channel: {@link BufferMetrics} and {@link SplitBrainStatus}.
 *
 * @param clusterName  the cluster name
 * @param nodeId       the node id
 * @param nodeName     the application name
 * @param address      {@code host:port}
 * @param leader       whether this node is the leader
 * @param leaderAddress the current leader's address; null when there is none
 * @param onBreak      whether this node is resting. No business traffic while resting is
 *                     normal, not a fault
 * @param memberCount  how many members the cluster has right now
 * @param uptimeMillis how long this node has been running
 * @param timestamp    when this snapshot was taken, as a millisecond timestamp
 * @param buffers      the fill level of each inbound buffer. A non-zero {@code dropped}
 *                     means messages really were lost, and the dropping is <b>silent</b> --
 *                     without looking here there is no sound at all
 * @param splitBrain   whether more than one node currently holds the cluster port, and how
 *                     often that has happened
 * @param traffic      this instance's own three-stage traffic figures, measured
 *                     independently of any channel. Shaped like {@link ChannelMetrics}
 *                     because the questions are the same ones, asked of the whole node
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public record NodeMetrics(
        String clusterName,
        String nodeId,
        String nodeName,
        String address,
        boolean leader,
        String leaderAddress,
        boolean onBreak,
        int memberCount,
        long uptimeMillis,
        long timestamp,
        List<BufferMetrics> buffers,
        SplitBrainStatus splitBrain,
        ChannelMetrics traffic) {

    /**
     * Without traffic figures, for a caller that only wants the node's identity and its
     * buffers.
     *
     * <p>Kept so that assembling the record without reaching into the metrics registry
     * stays a one-liner; {@code traffic} then reads as all zeroes rather than null.
     */
    public NodeMetrics(String clusterName, String nodeId, String nodeName, String address,
                       boolean leader, String leaderAddress, boolean onBreak, int memberCount,
                       long uptimeMillis, long timestamp, List<BufferMetrics> buffers,
                       SplitBrainStatus splitBrain) {
        this(clusterName, nodeId, nodeName, address, leader, leaderAddress, onBreak,
                memberCount, uptimeMillis, timestamp, buffers, splitBrain,
                ChannelMetrics.empty(MetricsRegistry.DEFAULT_CHANNEL));
    }

    /**
     * This instance's overall tps, sends and receives together.
     *
     * <p>No longer summed from the channels: it reads the node's own counters, fed by their
     * own calls at the same points. The key and the meaning are what they always were; what
     * changed is that the figure is now measured rather than added up, so it also covers
     * what no channel can account for.
     */
    public double totalTps() {
        return traffic.tps();
    }

    /**
     * This instance's overall error rate, 0 to 1.
     *
     * <p>Weighted by message count, like its per-channel counterpart, and for the same
     * reason: the arithmetic mean of per-channel rates would let a quiet channel that sent
     * two messages and failed one weigh as heavily as a main channel carrying a million.
     */
    public double errorRate() {
        return traffic.errorRate();
    }

    /** Messages arriving per second across this instance. */
    public double arrivalRate() {
        return traffic.arrivalRate();
    }

    /** Messages finished per second across this instance. */
    public double completionRate() {
        return traffic.completionRate();
    }

    /** Whether any buffer has ever dropped a message. The red light that most deserves the
     *  top of a dashboard. */
    public boolean hasDroppedMessages() {
        return buffers.stream().anyMatch(BufferMetrics::hasDropped);
    }

    /** The fullest buffer -- the one that will fill first. */
    public BufferMetrics hottestBuffer() {
        return buffers.stream().max(Comparator.comparingDouble(BufferMetrics::usage)).orElse(null);
    }

}
