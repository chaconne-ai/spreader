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
 * @see ChannelMetrics
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public class MetricsRegistry {

    /** An instance that records nothing, for turning observability off. */
    public static final MetricsRegistry DISABLED = new MetricsRegistry(false);

    private final boolean enabled;
    private final Map<String, ChannelRecorder> channels = new ConcurrentHashMap<>();

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
        if (!enabled) {
            return 0L;
        }
        // Concurrency +1, on a counter shared with the inbound side. "How many messages
        // are in flight right now" ought to count both the ones sent and not yet
        // answered and the ones received and still being processed
        recorder(channel).inflight.incrementAndGet();
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
        if (!enabled) {
            return;
        }
        ChannelRecorder r = recorder(channel);
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
    // Instrumentation: inbound
    // ------------------------------------------------------------------

    /**
     * Inbound processing begins.
     *
     * <p>Concurrency goes {@code +1} here. It counts how many messages are being handled
     * at this instant, and that includes <b>both</b> outbound requests in flight and
     * inbound messages being processed. Counting only one side would leave a
     * receive-only node reporting a concurrency of 0 while it was in fact saturated.
     */
    public long onReceiveStart(String channel) {
        if (!enabled) {
            return 0L;
        }
        recorder(channel).inflight.incrementAndGet();
        return System.nanoTime();
    }

    /**
     * Inbound processing ends; concurrency goes {@code -1}.
     *
     * @param startNanos whatever {@link #onReceiveStart} returned
     * @param success    whether the handler ran to completion
     */
    public void onReceiveEnd(String channel, long startNanos, boolean success) {
        if (!enabled) {
            return;
        }
        ChannelRecorder r = recorder(channel);
        int now = r.inflight.decrementAndGet();
        r.updatePeakInflight(now + 1);

        if (success) {
            r.received.increment();
        } else {
            r.receiveFailures.increment();
        }
        if (startNanos > 0L) {
            r.inbound.record(System.nanoTime() - startNanos);
        }
    }

    /** A duplicate message arrived and was turned away by de-duplication. */
    public void onDuplicate(String channel) {
        if (enabled) {
            recorder(channel).duplicates.increment();
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

    /** A snapshot of one channel. A channel with no traffic yields an all-zero snapshot rather than null. */
    public ChannelMetrics snapshot(String channel) {
        String key = key(channel);
        ChannelRecorder r = channels.get(key);
        return r == null ? emptyMetrics(key) : r.snapshot(key);
    }

    /** Resets every channel to zero. */
    public void reset() {
        channels.values().forEach(ChannelRecorder::reset);
    }

    private ChannelRecorder recorder(String channel) {
        return channels.computeIfAbsent(key(channel), k -> new ChannelRecorder());
    }

    private static String key(String channel) {
        return channel == null ? "" : channel;
    }

    private static ChannelMetrics emptyMetrics(String channel) {
        return new ChannelMetrics(channel, 0, 0, 0, 0, 0, 0, 0, 0,
                0d, 0d, 0d, 0d, LatencySnapshot.EMPTY, LatencySnapshot.EMPTY);
    }

    // ------------------------------------------------------------------

    /** All the counters for one channel. */
    private static final class ChannelRecorder {

        private final LongAdder sendFailures = new LongAdder();
        private final LongAdder retries = new LongAdder();
        private final LongAdder receiveFailures = new LongAdder();
        private final LongAdder duplicates = new LongAdder();

        /** Successful sends and receives use RateCounter, since they need both a total and a TPS. */
        private final RateCounter sent = new RateCounter();
        private final RateCounter received = new RateCounter();

        private final AtomicInteger inflight = new AtomicInteger();
        private final AtomicInteger peakInflight = new AtomicInteger();

        private final LatencyHistogram outbound = new LatencyHistogram();
        private final LatencyHistogram inbound = new LatencyHistogram();

        void updatePeakInflight(int observed) {
            int cur;
            while (observed > (cur = peakInflight.get())) {
                if (peakInflight.compareAndSet(cur, observed)) {
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
                    inflight.get(),
                    peakInflight.get(),
                    sent.ratePerSecond(),
                    received.ratePerSecond(),
                    sent.peakRatePerSecond(),
                    received.peakRatePerSecond(),
                    outbound.snapshot(),
                    inbound.snapshot());
        }

        void reset() {
            sent.reset();
            received.reset();
            sendFailures.reset();
            retries.reset();
            receiveFailures.reset();
            duplicates.reset();
            peakInflight.set(inflight.get());
            outbound.reset();
            inbound.reset();
        }
    }
}
