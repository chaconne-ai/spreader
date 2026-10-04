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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Collects observability data per channel.
 *
 * <h2>This sits on the hot path</h2>
 * Every message sent or received passes through here, so the implementation uses only
 * {@link LongAdder}, {@link AtomicInteger} and a lock-free histogram -- <b>not one
 * lock, not one allocation</b> (a recorder is built the first time a channel appears
 * and reused from then on).
 *
 * <p>Observability becoming the bottleneck is an absurd outcome, and an easier one to
 * arrive at than it sounds: a single {@code synchronized} is enough to make sends and
 * receives queue up under concurrency.
 *
 * <h2>Turned off, it costs nothing</h2>
 * {@link #DISABLED} is an instance whose every method does nothing. Use it where
 * observability is unwanted -- embedded deployments, or extreme latency sensitivity --
 * and the JIT inlines the empty methods away entirely.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @see ChannelMetrics
 * @since 20/08/2026
 */
public class MetricsRegistry {

    /**
     * An instance that records nothing, for turning observability off.
     */
    /** What the default channel is called, at both the metrics and the listener level. */
    public static final String DEFAULT_CHANNEL = "default";

    /**
     * The channel gossip's own traffic is filed under: heartbeat, probe, membership sync,
     * join, leave.
     *
     * <p>It carries the {@code spreader.} prefix so that {@link ChannelMetrics#isSystemChannel()}
     * recognises it alongside the other framework channels. Called {@code system} without the
     * prefix, it would be counted as one of the application's own, and the business versus
     * framework split would quietly be wrong.
     *
     * <p>Having it as a channel rather than only an instance-level figure is what makes
     * "how much of this is just gossip keeping the cluster together" answerable on its own,
     * instead of as an unexplained gap between the two levels.
     */
    public static final String SYSTEM_CHANNEL = "spreader.system";

    public static final MetricsRegistry DISABLED = new MetricsRegistry(false);

    /**
     * How a channel is written for a human: in a URL path, a Prometheus label, a report.
     *
     * <p>Two keys read differently from the way they are stored. The default channel is
     * stored as {@code "default"} and shows as such. Gossip's own traffic is stored as
     * {@code "spreader.system"}, because the prefix is what marks a channel as the
     * framework's rather than the application's, but it shows as plain {@code "system"}:
     * the prefix is an internal convention and carries no meaning for whoever reads the
     * output.
     */
    public static String displayName(String channel) {
        if (channel == null || channel.isEmpty()) {
            return DEFAULT_CHANNEL;
        }
        return SYSTEM_CHANNEL.equals(channel) ? "system" : channel;
    }

    /**
     * The reverse of {@link #displayName}: what a reader typed, turned back into the key.
     *
     * <p>So that {@code /actuator/spreader/system} finds the same data that
     * {@code /actuator/spreader/spreader.system} does, and neither quietly returns an
     * all-zero record.
     */
    public static String channelKey(String display) {
        if (display == null || display.isEmpty() || DEFAULT_CHANNEL.equals(display)) {
            return DEFAULT_CHANNEL;
        }
        return "system".equals(display) ? SYSTEM_CHANNEL : display;
    }

    private final boolean enabled;
    private final Map<String, MetricRecorder> channels = new ConcurrentHashMap<>();
    private final MetricRecorder nodeRecorder = new MetricRecorder();

    public MetricsRegistry() {
        this(true);
    }

    private MetricsRegistry(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    // ------------------------------------------------------------------
    // Instrumentation: outbound
    // ------------------------------------------------------------------

    /**
     * An outbound request begins. Returns the start time, which the matching end method
     * expects back.
     *
     * <p>Handing the timestamp to the caller rather than keeping it here avoids holding
     * any "whose request is this" state. Under concurrency that would mean a table keyed
     * by thread or request id, costing far more than this method itself.
     *
     * @return the start time in nanoseconds, or 0 when observability is off
     */
    public long onSendStart(String channel) {
        return onSendStart(recorder(channel));
    }

    /** The instance-wide counterpart: no channel, so it lands on the node recorder. */
    public long onSendStart() {
        return onSendStart(nodeRecorder);
    }

    private long onSendStart(MetricRecorder r) {
        if (!enabled) {
            return 0L;
        }
        // Outbound in flight +1: sent and not yet acknowledged. This is NOT the processing
        // concurrency -- that one is bracketed by onHandleStart/onHandleEnd
        r.inflight.incrementAndGet();
        return System.nanoTime();
    }

    /**
     * An outbound request ends.
     *
     * @param startNanos whatever {@link #onSendStart} returned
     * @param success    whether it succeeded
     * @param retries    how many retries this send took; pass 0 for none
     */
    public void onSendEnd(String channel, long startNanos, boolean success, int retries) {
        onSendEnd(recorder(channel), startNanos, success, retries);
    }

    /** The instance-wide counterpart. */
    public void onSendEnd(long startNanos, boolean success, int retries) {
        onSendEnd(nodeRecorder, startNanos, success, retries);
    }

    private void onSendEnd(MetricRecorder r, long startNanos, boolean success, int retries) {
        if (!enabled) {
            return;
        }
        int now = r.inflight.decrementAndGet();
        // The peak is updated here rather than at start: the value read at start could
        // be changed by someone else a moment later, whereas this one is a concurrency
        // level that genuinely held while this request existed
        r.updatePeakInflight(now + 1);

        if (retries > 0) {
            r.retries.add(retries);
        }
        if (success) {
            r.sent.increment();
            if (startNanos > 0L) {
                r.outbound.record(System.nanoTime() - startNanos);
            }
        } else {
            r.sendFailures.increment();
            // Failed calls are kept out of the latency distribution: one timeout would
            // drag P99 up to the timeout value, and that number describes the timeout
            // setting rather than how fast the service actually is
        }
    }

    // ------------------------------------------------------------------
    // Instrumentation: arrival, the second stage
    // ------------------------------------------------------------------

    /**
     * A message has arrived and is being taken in.
     *
     * <p>This stage ends the moment the message is <b>accepted</b> (queued for a listener),
     * not when a listener has dealt with it. That is the third stage,
     * {@link #onHandleStart}. Keeping them apart is the point: the gap between the two
     * rates is the backlog, and the gap between arrivals and the two of them together is
     * what was dropped.
     */
    public long onReceiveStart(String channel) {
        return onReceiveStart(recorder(channel));
    }

    /** The instance-wide counterpart. */
    public long onReceiveStart() {
        return onReceiveStart(nodeRecorder);
    }

    private long onReceiveStart(MetricRecorder r) {
        if (!enabled) {
            return 0L;
        }
        r.arriving.incrementAndGet();
        return System.nanoTime();
    }

    /**
     * Taking the message in has finished.
     *
     * @param startNanos whatever {@link #onReceiveStart} returned
     * @param success    whether it was accepted; false means it was turned away, which is
     *                   what {@code dropRate} counts
     */
    public void onReceiveEnd(String channel, long startNanos, boolean success) {
        onReceiveEnd(recorder(channel), startNanos, success);
    }

    /** The instance-wide counterpart. */
    public void onReceiveEnd(long startNanos, boolean success) {
        onReceiveEnd(nodeRecorder, startNanos, success);
    }

    private void onReceiveEnd(MetricRecorder r, long startNanos, boolean success) {
        if (!enabled) {
            return;
        }
        int now = r.arriving.decrementAndGet();
        r.updatePeakArriving(now + 1);

        if (success) {
            r.received.increment();
        } else {
            r.receiveFailures.increment();
        }
        if (startNanos > 0L) {
            r.inbound.record(System.nanoTime() - startNanos);
        }
    }

    // ------------------------------------------------------------------
    // Instrumentation: handling, the third stage
    // ------------------------------------------------------------------

    /**
     * A listener has begun handling a message.
     *
     * <p>Concurrency {@code +1}. This is the figure to read as "how much work is in hand
     * right now", and it is a different quantity from {@code inflight}, which counts
     * outbound requests awaiting their answer.
     *
     * <p>How far "handling" reaches depends on the listener, and that is deliberate.
     * A listener that does the work in {@code onPayload} has it measured end to end; one
     * that hands the work to a pool of its own has the <b>handover</b> measured here, and
     * what happens inside its pool is its own business to report.
     *
     * @return the start time in nanoseconds, or 0 when observability is off
     */
    public long onHandleStart(String channel) {
        return onHandleStart(recorder(channel));
    }

    /** The instance-wide counterpart. */
    public long onHandleStart() {
        return onHandleStart(nodeRecorder);
    }

    private long onHandleStart(MetricRecorder r) {
        if (!enabled) {
            return 0L;
        }
        r.handling.incrementAndGet();
        return System.nanoTime();
    }

    /**
     * Handling has finished; concurrency {@code -1}.
     *
     * @param startNanos whatever {@link #onHandleStart} returned
     * @param success    whether the handler ran to completion. A failure still counts
     *                   towards {@code completionRate}: the work was done, the outcome was
     *                   a failure, and {@code handleErrorRate} is the share of those
     */
    public void onHandleEnd(String channel, long startNanos, boolean success) {
        onHandleEnd(recorder(channel), startNanos, success);
    }

    /** The instance-wide counterpart. */
    public void onHandleEnd(long startNanos, boolean success) {
        onHandleEnd(nodeRecorder, startNanos, success);
    }

    private void onHandleEnd(MetricRecorder r, long startNanos, boolean success) {
        if (!enabled) {
            return;
        }
        int now = r.handling.decrementAndGet();
        r.updatePeakHandling(now + 1);

        if (success) {
            r.handled.increment();
        } else {
            r.handledFailures.increment();
        }
        // Unlike a send, a failed handling stays in the distribution: it was real work that
        // really took that long, and leaving it out would flatter the numbers
        if (startNanos > 0L) {
            r.handle.record(System.nanoTime() - startNanos);
        }
    }

    /**
     * A duplicate message arrived and was turned away by de-duplication.
     */
    public void onDuplicate(String channel) {
        if (enabled) {
            recorder(channel).duplicates.increment();
        }
    }

    /** The instance-wide counterpart. */
    public void onDuplicate() {
        if (enabled) {
            nodeRecorder.duplicates.increment();
        }
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    /**
     * A snapshot of every channel, ordered by channel name.
     *
     * <p>What comes back is an <b>immutable snapshot</b>: it will not change no matter
     * how long you hold it, so serialising or rendering it at leisure is safe -- there
     * is no risk of the data shifting halfway through.
     */
    public Map<String, ChannelMetrics> snapshot() {
        List<String> names = new ArrayList<>(channels.keySet());
        names.sort(Comparator.naturalOrder());
        Map<String, ChannelMetrics> out = new LinkedHashMap<>(names.size());
        for (String name : names) {
            out.put(name, channels.get(name).snapshot(name));
        }
        return out;
    }

    /**
     * A snapshot of one channel. A channel with no traffic yields an all-zero snapshot rather than null.
     */
    public ChannelMetrics snapshot(String channel) {
        String key = key(channel);
        MetricRecorder r = channels.get(key);
        return r == null ? ChannelMetrics.empty(key) : r.snapshot(key);
    }

    /**
     * Resets every channel to zero.
     */
    public void reset() {
        channels.values().forEach(MetricRecorder::reset);
        nodeRecorder.reset();
    }

    private MetricRecorder recorder(String channel) {
        return channels.computeIfAbsent(key(channel), k -> new MetricRecorder());
    }

    /**
     * The key a channel is recorded under.
     *
     * <p>The default channel is keyed <b>{@code "default"}</b> rather than the empty string.
     * An empty string cannot be written in a Prometheus label or a URL path, so every display
     * layer had to rename it on the way out, and {@code channel("default")} then silently
     * returned an all-zero record because that was not the key. One name throughout removes
     * the trap, and {@link GossipListener#getChannel()} returns the same word by default.
     */
    private static String key(String channel) {
        return channel == null || channel.isEmpty() ? DEFAULT_CHANNEL : channel;
    }

    /** What this instance as a whole has handled, with no channel attribution. */
    public ChannelMetrics nodeSnapshot() {
        return nodeRecorder.snapshot(DEFAULT_CHANNEL);
    }


    // ------------------------------------------------------------------

    /**
     * One set of counters. The <b>same</b> class serves both levels: {@link #channels} holds
     * one per channel, and {@link #nodeRecorder} is the instance-wide one.
     *
     * <p>They are fed by separate calls rather than one being summed from the other. The
     * node's figures therefore include what no channel can account for: control-plane
     * traffic, a frame that would not decode, a frame from another cluster.
     */
    private static final class MetricRecorder {

        private final LongAdder sendFailures = new LongAdder();
        private final LongAdder retries = new LongAdder();
        private final LongAdder receiveFailures = new LongAdder();
        private final LongAdder duplicates = new LongAdder();
        private final LongAdder handledFailures = new LongAdder();

        /**
         * Successful sends and receives use RateCounter, since they need both a total and a TPS.
         */
        private final RateCounter sent = new RateCounter();
        private final RateCounter received = new RateCounter();
        private final RateCounter handled = new RateCounter();

        /** Outbound: sent and awaiting an answer. Not the processing concurrency. */
        private final AtomicInteger inflight = new AtomicInteger();
        private final AtomicInteger peakInflight = new AtomicInteger();

        /** Arrived and not yet accepted. Normally near zero; it climbs when intake stalls. */
        private final AtomicInteger arriving = new AtomicInteger();
        private final AtomicInteger peakArriving = new AtomicInteger();

        /** In a listener's hands right now. This is the one to read as "work in hand". */
        private final AtomicInteger handling = new AtomicInteger();
        private final AtomicInteger peakHandling = new AtomicInteger();

        private final LatencyHistogram outbound = new LatencyHistogram();
        private final LatencyHistogram inbound = new LatencyHistogram();
        private final LatencyHistogram handle = new LatencyHistogram();

        void updatePeakInflight(int observed) {
            raise(peakInflight, observed);
        }

        void updatePeakArriving(int observed) {
            raise(peakArriving, observed);
        }

        void updatePeakHandling(int observed) {
            raise(peakHandling, observed);
        }

        private static void raise(AtomicInteger peak, int observed) {
            int cur;
            while (observed > (cur = peak.get())) {
                if (peak.compareAndSet(cur, observed)) {
                    return;
                }
            }
        }

        ChannelMetrics snapshot(String channel) {
            return new ChannelMetrics(
                    channel,
                    sent.total(),
                    sendFailures.sum(),
                    retries.sum(),
                    received.total(),
                    receiveFailures.sum(),
                    duplicates.sum(),
                    handled.total(),
                    handledFailures.sum(),
                    inflight.get(),
                    peakInflight.get(),
                    arriving.get(),
                    peakArriving.get(),
                    handling.get(),
                    peakHandling.get(),
                    sent.ratePerSecond(),
                    received.ratePerSecond(),
                    handled.ratePerSecond(),
                    sent.peakRatePerSecond(),
                    received.peakRatePerSecond(),
                    handled.peakRatePerSecond(),
                    outbound.snapshot(),
                    inbound.snapshot(),
                    handle.snapshot());
        }

        void reset() {
            sent.reset();
            received.reset();
            handled.reset();
            sendFailures.reset();
            retries.reset();
            receiveFailures.reset();
            handledFailures.reset();
            duplicates.reset();
            // The peak is set to whatever is in hand now rather than to zero: a reset in the
            // middle of a busy moment would otherwise report a peak lower than the current
            // value, which reads as impossible
            peakInflight.set(inflight.get());
            peakArriving.set(arriving.get());
            peakHandling.set(handling.get());
            outbound.reset();
            inbound.reset();
            handle.reset();
        }
    }
}
