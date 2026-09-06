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

/**
 * A snapshot of the observability data for one channel.
 *
 * <h2>Why it is split by channel</h2>
 * Several kinds of traffic share a cluster: the user's own business messages on the
 * default channel, cache replication on {@code spreader.cache}, locks on
 * {@code spreader.mutex}, RPC on {@code spreader.rpc}, and so on. Their volumes,
 * latency profiles and tolerable error rates are <b>nothing alike</b>.
 *
 * <p>Lumped together, the high-frequency small packets of cache replication dilute
 * RPC's P99 until the problem disappears, and a spike in one channel's error rate
 * drowns in the total. <b>Split by channel, an anomaly points at someone.</b>
 *
 * <h2>Outbound and inbound are different questions</h2>
 * <ul>
 *   <li>{@link #outboundLatency()} -- messages this node <b>sent</b>, measured from the
 *       send to the peer's acknowledgement. It includes the network round trip and the
 *       peer's processing, so it is "how slow this felt to the caller"</li>
 *   <li>{@link #inboundProcessing()} -- messages this node <b>received</b>, measured as
 *       how long the handler ran. No network involved, so it is "how slow this node
 *       itself is"</li>
 * </ul>
 *
 * <p>Both are needed because they point at different parties: slow outbound may be the
 * peer or the network, whereas slow inbound is definitely your own problem. Watching
 * only one of them sends the investigation in the wrong direction.
 *
 * @param channel           channel name; the empty string is the user's default channel
 * @param sent              messages sent successfully -- meaning acknowledged, when acks
 *                          are in use
 * @param sendFailures      failed sends, including those given up on after retries ran out
 * @param retries           retry count. Only meaningful read alongside
 *                          {@code sendFailures}: many retries with few failures means the
 *                          network is unsteady but being absorbed
 * @param received          messages received and processed successfully
 * @param receiveFailures   messages received whose handler threw
 * @param duplicates        duplicates turned away by de-duplication. A high count means a
 *                          peer is retransmitting heavily
 * @param inflight          <b>current concurrency</b>: messages being handled at this
 *                          instant. {@code +1} before handling and {@code -1} after,
 *                          counting both outbound sends awaiting an answer and inbound
 *                          messages being processed
 * @param peakInflight      all-time peak concurrency. Use it to judge whether the
 *                          concurrency ceiling is set sensibly
 * @param sentTps           outbound TPS: messages sent in the last complete second
 * @param receivedTps       inbound TPS: messages received in the last complete second
 * @param peakSentTps       all-time peak outbound TPS
 * @param peakReceivedTps   all-time peak inbound TPS
 * @param outboundLatency   distribution of outbound latency
 * @param inboundProcessing distribution of inbound handler time
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public record ChannelMetrics(
        String channel,
        long sent,
        long sendFailures,
        long retries,
        long received,
        long receiveFailures,
        long duplicates,
        int inflight,
        int peakInflight,
        double sentTps,
        double receivedTps,
        double peakSentTps,
        double peakReceivedTps,
        LatencySnapshot outboundLatency,
        LatencySnapshot inboundProcessing) {

    /** Total TPS: sends plus receives in the last complete second. */
    public double tps() {
        return sentTps + receivedTps;
    }

    /** All-time peak of the combined throughput. */
    public double peakTps() {
        return peakSentTps + peakReceivedTps;
    }

    /**
     * Outbound error rate, 0 to 1.
     *
     * <p>The denominator is total send <i>attempts</i>, not successes. With successes on
     * the bottom, a total failure would yield either 100% or a division by zero, and
     * neither number is readable.
     */
    public double sendErrorRate() {
        long total = sent + sendFailures;
        return total == 0 ? 0d : (double) sendFailures / total;
    }

    /** Inbound error rate, 0 to 1: what fraction of received messages failed to process. */
    public double receiveErrorRate() {
        long total = received + receiveFailures;
        return total == 0 ? 0d : (double) receiveFailures / total;
    }

    /**
     * Overall error rate, 0 to 1, sends and receives combined.
     *
     * <p>This is usually the one to alert on; the per-direction rates then say which way
     * to look.
     */
    public double errorRate() {
        long errors = sendFailures + receiveFailures;
        long total = sent + received + errors;
        return total == 0 ? 0d : (double) errors / total;
    }

    /** Retry rate, 0 to 1: retries per send on average. Persistently above 0 means the network is not clean. */
    public double retryRate() {
        long total = sent + sendFailures;
        return total == 0 ? 0d : (double) retries / total;
    }

    /** Not a single message has passed through. Used to skip output so that all-zero channels do not flood the display. */
    public boolean isIdle() {
        return sent == 0 && sendFailures == 0 && received == 0 && receiveFailures == 0;
    }

    /** Whether this channel belongs to the framework ({@code spreader.} prefix) rather than to business messages. */
    public boolean isSystemChannel() {
        return channel != null && channel.startsWith("spreader.");
    }

    @Override
    public String toString() {
        return "channel[" + (channel == null || channel.isEmpty() ? "default" : channel) + "] "
                + "sent=" + sent + "(failed " + sendFailures + ", retried " + retries + ")"
                + ", received=" + received + "(failed " + receiveFailures
                + ", duplicate " + duplicates + ")"
                + ", inflight=" + inflight + "(peak " + peakInflight + ")"
                + ", TPS=" + String.format("%.1f", tps())
                + "(peak " + String.format("%.1f", peakTps()) + ")"
                + ", errorRate=" + String.format("%.2f%%", errorRate() * 100)
                + "\n  outbound latency: " + outboundLatency
                + "\n  inbound processing: " + inboundProcessing;
    }
}
