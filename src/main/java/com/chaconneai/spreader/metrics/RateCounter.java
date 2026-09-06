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
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Throughput accounting: a running total plus the number handled <b>per second</b>.
 *
 * <h2>TPS here means "how many in one second"</h2>
 * Not "the average over N seconds". A multi-second average flattens spikes: a second
 * carrying ten times the traffic shows up as a small ripple in a ten-second average --
 * and that second is exactly the one worth seeing.
 *
 * <p>So {@link #ratePerSecond()} reports the actual count for the <b>last complete
 * second</b>. It uses the previous second rather than the current one because the
 * current one is not over: read at the 200ms mark you would get a fifth of it, and TPS
 * would be understated fivefold, systematically.
 *
 * <h2>How it works</h2>
 * A ring of per-second slots. Recording costs an index computation and one CAS
 * increment -- no locks, no allocation, fixed memory, and no cleanup thread: each slot
 * remembers which second it belongs to, so stale slots are simply overwritten.
 *
 * <p>Keeping several slots ({@value #SLOTS} of them) is not about averaging. It is so
 * that {@link #ratePerSecond()} still finds the previous second when the collection
 * interval drifts slightly, instead of landing exactly on a slot rollover and reading
 * zero.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public class RateCounter {

    /** Slots in the ring. Only the two most recent are read; the rest absorb jitter in collection timing. */
    static final int SLOTS = 8;

    private final LongAdder total = new LongAdder();

    /** One slot per second, holding that second's count. */
    private final AtomicLongArray slots = new AtomicLongArray(SLOTS);

    /** Parallel to {@link #slots}: which second each slot belongs to, so stale data from an earlier lap is recognised. */
    private final AtomicLongArray stamps = new AtomicLongArray(SLOTS);

    /** The highest complete-second count ever seen. */
    private final AtomicLong peak = new AtomicLong();

    public RateCounter() {
        for (int i = 0; i < SLOTS; i++) {
            stamps.set(i, Long.MIN_VALUE);
        }
    }

    public void increment() {
        add(1L);
    }

    public void add(long n) {
        total.add(n);
        long second = nowSecond();
        int idx = (int) Math.floorMod(second, SLOTS);
        // Slot belongs to a different second -> leftovers from an earlier lap, so reset
        // before recording. Same second just accumulates
        if (stamps.get(idx) != second) {
            stamps.set(idx, second);
            slots.set(idx, n);
        } else {
            slots.addAndGet(idx, n);
        }
    }

    /** Running total. */
    public long total() {
        return total.sum();
    }

    /**
     * TPS: how many were handled in the <b>last complete second</b>.
     *
     * <p>Returns 0 when that second carried no traffic -- which is the truth, not a gap
     * in the data.
     *
     * <p>One exception: within the first second of the process's life it falls back to
     * the current second's running count. Otherwise, checking TPS right after sending a
     * batch would show 0 and look like nothing was being collected.
     */
    public double ratePerSecond() {
        long second = nowSecond();
        long previous = countOf(second - 1);
        if (previous > 0) {
            recordPeak(previous);
            return previous;
        }
        // Less than a second old: the current second's tally is not a complete second,
        // but it carries more information than reporting 0
        if (elapsedSeconds() < 1) {
            long current = countOf(second);
            recordPeak(current);
            return current;
        }
        return 0d;
    }

    /** The highest TPS ever seen. Only updated when {@link #ratePerSecond()} is called,
     *  so it is only as accurate as the collection is frequent. */
    public double peakRatePerSecond() {
        return peak.get();
    }

    /** The count for a given second, or 0 when that slot has been taken over by another
     *  second -- which means that second saw no traffic. */
    private long countOf(long second) {
        int idx = (int) Math.floorMod(second, SLOTS);
        return stamps.get(idx) == second ? slots.get(idx) : 0L;
    }

    private void recordPeak(long value) {
        long cur;
        while (value > (cur = peak.get())) {
            if (peak.compareAndSet(cur, value)) {
                return;
            }
        }
    }

    private long elapsedSeconds() {
        return (System.nanoTime() - startNanos) / 1_000_000_000L;
    }

    private final long startNanos = System.nanoTime();

    private static long nowSecond() {
        return System.nanoTime() / 1_000_000_000L;
    }

    public void reset() {
        total.reset();
        for (int i = 0; i < SLOTS; i++) {
            slots.set(i, 0L);
            stamps.set(i, Long.MIN_VALUE);
        }
        peak.set(0L);
    }
}
