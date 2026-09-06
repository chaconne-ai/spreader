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

import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * An implementation backed by JCTools' {@code MpscArrayQueue}.
 *
 * <h2>Why JCTools rather than hand-rolling it</h2>
 * It follows the same ideas (monotonic sequence, CAS to claim a slot, batched
 * consumption) but works them out in far more detail:
 * <ul>
 *   <li><b>Cache-line padding</b> -- the producer and consumer cursors are forced onto
 *       separate cache lines. Adjacent, a write by one invalidates the other's cache
 *       line (false sharing), and under high concurrency that costs more than the queue
 *       itself</li>
 *   <li><b>Memory barriers pared to the minimum</b> -- every volatile read and write has
 *       been weighed, and lazySet is used wherever a volatile write is not required</li>
 *   <li>It has been run by a great many high-frequency systems. The edge cases in
 *       lock-free structures like this are extremely hard to test exhaustively on
 *       your own</li>
 * </ul>
 *
 * <h2>Why not Disruptor</h2>
 * Disruptor is more powerful, but it asks you to take on the whole
 * {@code EventFactory} / {@code EventHandler} / {@code RingBuffer} API, which is far
 * more invasive -- and its strengths (multi-consumer dependency graphs, pre-allocated
 * objects for zero GC) are of no use here. JCTools' API is simply a {@code Queue}, so
 * adopting it changed only a few lines.
 *
 * <p>{@link PayloadQueues} loads this class reflectively, so when JCTools is absent from
 * the classpath the class is never loaded at all and there is no
 * {@code NoClassDefFoundError}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
class JcToolsPayloadQueue<T> implements PayloadQueue<T> {

    private final MpscArrayQueue<T> queue;
    private final AtomicLong dropped = new AtomicLong();

    JcToolsPayloadQueue(int capacity) {
        this.queue = new MpscArrayQueue<>(Math.max(2, capacity));
    }

    @Override
    public boolean offer(T item) {
        if (queue.offer(item)) {
            return true;
        }
        dropped.incrementAndGet();
        return false;
    }

    @Override
    public int drain(Consumer<T> consumer, int limit) {
        // JCTools has its own Consumer interface with the same shape; one adapter lambda covers it
        return queue.drain(consumer::accept, limit);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public int capacity() {
        return queue.capacity();
    }

    @Override
    public long dropped() {
        return dropped.get();
    }

    @Override
    public String toString() {
        return "JcToolsPayloadQueue{size=" + size() + "/" + capacity()
                + ", dropped=" + dropped.get() + '}';
    }
}
