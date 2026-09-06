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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A latency histogram, used to compute P50 / P95 / P99.
 *
 * <h2>Why write one rather than use a library</h2>
 * HdrHistogram and Micrometer both do this, but this project <b>pulls in no
 * third-party implementation</b> -- observability should not be something you only get
 * by adding a library. The single exception is {@code slf4j-api}, which is a pure
 * facade and carries no implementation; HdrHistogram and its kind are implementations,
 * which is a different thing. Beyond that, this sits on the send/receive hot path, so
 * the cost of one recording has to be negligible.
 *
 * <h2>How the buckets work</h2>
 * Keeping every sample exactly means either unbounded memory or a sort, and neither is
 * acceptable. So the buckets are <b>logarithmic</b>: one magnitude per power of two,
 * each split into {@value #SUB_BUCKETS} even sub-buckets.
 *
 * <ul>
 *   <li>Recording is {@code O(1)}: compute an index, {@link LongAdder#increment()}, no
 *       lock</li>
 *   <li>Memory is fixed at {@value #BUCKETS} buckets, independent of sample count</li>
 *   <li>Relative error stays under {@code 1/SUB_BUCKETS}, about 3% -- ample for
 *       "is P99 8ms or 8.2ms", and magnitude and trend are what observability is
 *       actually about</li>
 * </ul>
 *
 * <p>This is the same approach Prometheus and Micrometer take; theirs merely allow the
 * bucket boundaries to be configured.
 *
 * <h2>min and max are exact</h2>
 * The percentiles carry bucketing error, but min and max are tracked precisely in their
 * own {@link AtomicLong}s. When investigating a problem, "how slow was the slowest one,
 * really" is often more useful than P99 -- and it is precisely the value that bucket
 * precision should not be allowed to blur.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public class LatencyHistogram {

    /** Sub-buckets per power-of-two magnitude. 32 corresponds to roughly 3% relative error. */
    private static final int SUB_BUCKETS = 32;

    private static final int SUB_BITS = 5;   // log2(SUB_BUCKETS)

    /** Number of magnitudes. In microseconds, 2^40 is about 12 days, which is plenty. */
    private static final int MAGNITUDES = 40;

    private static final int BUCKETS = MAGNITUDES * SUB_BUCKETS;

    private final LongAdder[] buckets = new LongAdder[BUCKETS];
    private final LongAdder count = new LongAdder();
    private final LongAdder totalMicros = new LongAdder();
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(0L);

    public LatencyHistogram() {
        for (int i = 0; i < BUCKETS; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /**
     * Records one latency.
     *
     * @param nanos nanoseconds. Bucketing happens in microseconds: nanosecond-level
     *              differences are meaningless for a distributed call, and indexing by
     *              nanoseconds would waste a dozen magnitudes for nothing
     */
    public void record(long nanos) {
        long micros = Math.max(0L, nanos) / 1_000L;
        count.increment();
        totalMicros.add(micros);
        buckets[bucketOf(micros)].increment();
        updateMin(micros);
        updateMax(micros);
    }

    /**
     * Which bucket a microsecond value falls into.
     *
     * <p>The idea: write the value as {@code 1.xxx x 2^k}. Then {@code k} picks the
     * magnitude and the top {@value #SUB_BITS} bits of the fraction pick the sub-bucket.
     * Values below {@value #SUB_BUCKETS} have no fractional part at all and get one
     * bucket each -- the sub-millisecond range is where resolution matters most anyway.
     */
    static int bucketOf(long micros) {
        if (micros < SUB_BUCKETS) {
            return (int) micros;
        }
        int magnitude = 63 - Long.numberOfLeadingZeros(micros);
        if (magnitude >= MAGNITUDES) {
            return BUCKETS - 1;
        }
        int sub = (int) ((micros >>> (magnitude - SUB_BITS)) & (SUB_BUCKETS - 1));
        return magnitude * SUB_BUCKETS + sub;
    }

    /** Turns a bucket index back into microseconds, taking the bucket's lower bound. */
    static long microsOf(int bucket) {
        if (bucket < SUB_BUCKETS) {
            return bucket;
        }
        int magnitude = bucket / SUB_BUCKETS;
        int sub = bucket % SUB_BUCKETS;
        return ((long) sub | SUB_BUCKETS) << (magnitude - SUB_BITS);
    }

    private void updateMin(long micros) {
        long cur;
        while (micros < (cur = min.get())) {
            if (min.compareAndSet(cur, micros)) {
                return;
            }
        }
    }

    private void updateMax(long micros) {
        long cur;
        while (micros > (cur = max.get())) {
            if (max.compareAndSet(cur, micros)) {
                return;
            }
        }
    }

    /**
     * Takes a snapshot.
     *
     * <p><b>Not atomic.</b> New samples keep arriving while it reads, so the parts may be
     * slightly inconsistent with one another -- count can come out a little larger than
     * the sum of the buckets, for instance. Observability data is not worth a lock for
     * this: paying with the hot path to buy a consistent sampling instant is wildly out
     * of proportion.
     */
    public LatencySnapshot snapshot() {
        long n = count.sum();
        if (n == 0) {
            return LatencySnapshot.EMPTY;
        }
        long[] counts = new long[BUCKETS];
        long total = 0;
        for (int i = 0; i < BUCKETS; i++) {
            counts[i] = buckets[i].sum();
            total += counts[i];
        }
        if (total == 0) {
            return LatencySnapshot.EMPTY;
        }
        long minMicros = min.get();
        return new LatencySnapshot(
                n,
                minMicros == Long.MAX_VALUE ? 0L : minMicros * 1_000L,
                max.get() * 1_000L,
                (double) totalMicros.sum() / n * 1_000d,
                percentile(counts, total, 0.50d) * 1_000L,
                percentile(counts, total, 0.95d) * 1_000L,
                percentile(counts, total, 0.99d) * 1_000L);
    }

    /** Finds a percentile among the buckets, in microseconds. Takes the bucket's upper
     *  bound -- better too high than too low, since understating hides the problem. */
    private static long percentile(long[] counts, long total, double quantile) {
        long target = (long) Math.ceil(total * quantile);
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= target) {
                return microsOf(i + 1);
            }
        }
        return microsOf(counts.length - 1);
    }

    /** Resets to zero. For collection styles that only care about the window just past. */
    public void reset() {
        for (LongAdder b : buckets) {
            b.reset();
        }
        count.reset();
        totalMicros.reset();
        min.set(Long.MAX_VALUE);
        max.set(0L);
    }
}
