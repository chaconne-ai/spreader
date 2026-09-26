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
 * <p>It follows the same ideas as a hand-rolled ring buffer (monotonic sequence, CAS to claim a
 * slot, batched consumption) but pares the memory barriers to the minimum and forces the
 * producer and consumer cursors onto separate cache lines. The edge cases in a lock-free
 * structure like this are very hard to test exhaustively alone, which is the actual reason to
 * take one that a great many high-frequency systems have already run.
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
