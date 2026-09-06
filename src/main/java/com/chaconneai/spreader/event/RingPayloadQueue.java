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
package com.chaconneai.spreader.event;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A bounded multi-producer, single-consumer ring buffer -- the <b>built-in</b>
 * implementation.
 *
 * <p>Used when JCTools is absent. Three key design points match what the mainstream
 * implementations (Disruptor, JCTools) do:
 *
 * <h2>1. Each cursor owns a whole cache line</h2>
 * Producers hammer the write cursor and the consumer hammers the read cursor; placed
 * next to each other, they fight over one cache line (false sharing) and throughput can
 * drop by more than half. See {@link PaddedCursor}.
 *
 * <h2>2. Publication uses per-slot markers, so producers never wait on each other</h2>
 * The naive approach has producers advance a single "published" cursor in sequence
 * order -- which means slot 6 must wait for slot 5 to publish, and if the OS has just
 * descheduled the thread holding 5, the one holding 6 spins for nothing.
 *
 * <p>Instead every slot carries a marker ({@link #available}) recording which lap it
 * was written on. A producer writes its slot and then marks its own cell, so
 * <b>nobody waits for anybody</b>; the consumer looks forward from its own position and
 * treats a matching marker as ready.
 *
 * <h2>3. Producers cache the read cursor</h2>
 * Deciding whether the queue is full means reading the read cursor -- which another
 * core is modifying, so reading it every time is a cache miss every time. The last
 * value seen is cached ({@link #cachedRead}) and the real one is consulted only when
 * the cached value says "full", so the overwhelming majority of offers involve no
 * cross-core read at all.
 *
 * <p>For more speed, add {@code org.jctools:jctools-core} and {@link PayloadQueues}
 * will pick it up automatically -- on top of all this, it tunes its memory barriers
 * more finely still.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RingPayloadQueue<T> implements PayloadQueue<T> {

    /** Element access on the slot array, with VarHandle controlling the memory semantics. */
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(Object[].class);

    /** The slot marker array, likewise. */
    private static final VarHandle FLAGS = MethodHandles.arrayElementVarHandle(long[].class);

    /** The next writable sequence number. Producers CAS for it. */
    private final PaddedCursor writeCursor = new PaddedCursor(0L);

    /** How far the consumer has read. Only the consumer thread modifies it. */
    private final PaddedCursor readCursor = new PaddedCursor(0L);

    /**
     * The producers' cached copy of the read cursor.
     *
     * <p>Used only to <b>decide quickly that the queue is not full</b>, and it may lag
     * the real value. Lagging can only make a queue that is in fact not full look full,
     * at which point the real value is read and the decision remade. So it is never
     * wrong -- occasionally just one read more.
     */
    private final PaddedCursor cachedRead = new PaddedCursor(0L);

    private final Object[] slots;

    /**
     * Which lap each slot was last written on.
     *
     * <p>The initial -1 means "never written". A write on lap n marks the slot n, and the
     * consumer reads readiness off that. A lap number rather than a boolean, because a
     * boolean cannot tell "written this lap" from "left over from the last one".
     */
    private final long[] available;

    private final int mask;
    private final int capacity;
    private final int shift;

    private final AtomicLong dropped = new AtomicLong();

    public RingPayloadQueue(int requestedCapacity) {
        this.capacity = nextPowerOfTwo(Math.max(2, requestedCapacity));
        this.slots = new Object[capacity];
        this.available = new long[capacity];
        Arrays.fill(this.available, -1L);
        this.mask = capacity - 1;
        this.shift = Integer.numberOfTrailingZeros(capacity);
    }

    @Override
    public boolean offer(T item) {
        while (true) {
            long write = writeCursor.get();
            long wrapPoint = write - capacity;

            // Check against the cached read cursor first. In the vast majority of cases
            // this settles it, with no cross-core read at all
            if (wrapPoint >= cachedRead.getPlain()) {
                long actualRead = readCursor.get();
                cachedRead.setRelease(actualRead);
                if (wrapPoint >= actualRead) {
                    dropped.incrementAndGet();
                    return false;
                }
            }

            if (writeCursor.compareAndSet(write, write + 1)) {
                int slot = (int) (write & mask);
                // Data first, then the ready marker. setRelease guarantees the order is
                // not reordered -- the other way round, the consumer would find a slot
                // marked ready whose contents were still empty
                SLOTS.set(slots, slot, item);
                FLAGS.setRelease(available, slot, write >>> shift);
                return true;
            }
        }
    }

    @Override
    public int drain(Consumer<T> consumer, int limit) {
        long read = readCursor.getPlain();
        int count = 0;
        long i = read;
        for (; count < limit; i++, count++) {
            int slot = (int) (i & mask);
            // Only a matching marker means this cell was written on this lap. A mismatch
            // means it is not finished yet, so stop here
            if ((long) FLAGS.getAcquire(available, slot) != (i >>> shift)) {
                break;
            }
            @SuppressWarnings("unchecked")
            T item = (T) SLOTS.get(slots, slot);
            // Clear the reference: otherwise this lap's object stays held by the array
            // until the next lap overwrites it
            SLOTS.set(slots, slot, null);
            consumer.accept(item);
        }
        if (count > 0) {
            // Advance the read cursor last: any earlier and a producer could overwrite
            // a slot that has not been handed over yet
            readCursor.setRelease(i);
        }
        return count;
    }

    @Override
    public int size() {
        long size = writeCursor.get() - readCursor.get();
        return (int) Math.max(0L, Math.min(size, capacity));
    }

    @Override
    public long dropped() {
        return dropped.get();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    private static int nextPowerOfTwo(int value) {
        int result = Integer.highestOneBit(value);
        return result == value ? value : result << 1;
    }

    @Override
    public String toString() {
        return "RingPayloadQueue{size=" + size() + "/" + capacity
                + ", dropped=" + dropped.get() + '}';
    }
}
