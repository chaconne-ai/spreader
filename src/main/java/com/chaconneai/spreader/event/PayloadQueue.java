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

import java.util.function.Consumer;

/**
 * A bounded multi-producer, single-consumer queue that separates receiving
 * {@code onPayload} from processing it.
 *
 * <h2>Two implementations, chosen by what is on the classpath</h2>
 * <ul>
 *   <li><b>JCTools</b> -- with {@code org.jctools:jctools-core} present, its
 *       {@code MpscArrayQueue} is used. An industrial-grade implementation: cache-line
 *       padding against false sharing, and memory barriers tuned with care. Noticeably
 *       faster than the built-in one at high frequency</li>
 *   <li><b>Built-in</b> -- without it, a ring buffer written here. Same ideas
 *       (monotonic sequence, CAS to claim a slot, batched consumption), just without
 *       those micro-optimisations</li>
 * </ul>
 *
 * <p>Behaviour is identical either way, so <b>having JCTools or not affects speed, never
 * correctness</b>. That is spreader's standing posture: it runs with no dependencies,
 * and runs faster when a library is available.
 *
 * <h2>Why multi-producer, single-consumer</h2>
 * There may be several producers (the event dispatch pool) but exactly one consumer
 * (the listener's own processing thread). Multiple consumers would want a different
 * design -- a cursor each -- and the use here, one consumption chain per listener, does
 * not need it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface PayloadQueue<T> {

    /**
     * Offers one item.
     *
     * <p><b>Never blocks</b>: a full queue returns false immediately. The producer is the
     * event dispatch thread, and stalling it stops every message behind this one.
     *
     * @return true if it went in, false if the queue was full
     */
    boolean offer(T item);

    /**
     * Drains what is ready, handing each item to {@code consumer}.
     *
     * <p><b>Must always be called from the same thread.</b>
     *
     * @param limit the most to take in this batch. The cap exists so the consumer thread
     *              can come back and check its exit flag periodically, rather than being
     *              held forever by a sustained stream
     * @return how many were actually taken
     */
    int drain(Consumer<T> consumer, int limit);

    /** How many are queued right now. Sitting near capacity is the signal that
     *  consumption is not keeping up. */
    int size();

    /** Capacity. An implementation may round it up to a power of two. */
    int capacity();

    /** Total discarded because the queue was full. Anything but 0 deserves attention. */
    long dropped();

    default boolean isEmpty() {
        return size() == 0;
    }
}
