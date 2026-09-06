package com.chaconneai.spreader.metrics;

import java.util.concurrent.TimeUnit;

/**
 * A snapshot of latency statistics, in <b>nanoseconds</b>.
 *
 * <p>Nanoseconds so that no precision is lost at this layer. For anything a human
 * reads, use the converting accessors such as {@link #maxMillis()}, or just
 * {@link #toString()}.
 *
 * @param count      number of samples
 * @param minNanos   the fastest one, <b>exact</b>
 * @param maxNanos   the slowest one, <b>exact</b>
 * @param avgNanos   the mean
 * @param p50Nanos   median, carrying roughly 3% bucketing error
 * @param p95Nanos   P95, same caveat
 * @param p99Nanos   P99, same caveat
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public record LatencySnapshot(
        long count,
        long minNanos,
        long maxNanos,
        double avgNanos,
        long p50Nanos,
        long p95Nanos,
        long p99Nanos) {

    public static final LatencySnapshot EMPTY =
            new LatencySnapshot(0L, 0L, 0L, 0d, 0L, 0L, 0L);

    public boolean isEmpty() {
        return count == 0;
    }

    public double minMillis() {
        return toMillis(minNanos);
    }

    public double maxMillis() {
        return toMillis(maxNanos);
    }

    public double avgMillis() {
        return avgNanos / 1_000_000d;
    }

    public double p50Millis() {
        return toMillis(p50Nanos);
    }

    public double p95Millis() {
        return toMillis(p95Nanos);
    }

    public double p99Millis() {
        return toMillis(p99Nanos);
    }

    private static double toMillis(long nanos) {
        return nanos / 1_000_000d;
    }

    public long minMicros() {
        return TimeUnit.NANOSECONDS.toMicros(minNanos);
    }

    public long maxMicros() {
        return TimeUnit.NANOSECONDS.toMicros(maxNanos);
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "no samples";
        }
        return String.format(
                "n=%d, min=%.3fms, avg=%.3fms, p50=%.3fms, p95=%.3fms, p99=%.3fms, max=%.3fms",
                count, minMillis(), avgMillis(), p50Millis(), p95Millis(), p99Millis(),
                maxMillis());
    }
}
