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

/**
 * A long cursor that occupies an entire cache line on its own.
 *
 * <h2>Why the padding</h2>
 * CPU caches read and write in <b>cache lines</b>, typically 64 bytes. Two variables
 * placed next to each other land on the same line, so when one core writes A and
 * another reads B, B's cache line is invalidated as well -- even though nobody touched
 * B. That is <b>false sharing</b>.
 *
 * <p>A ring buffer contains exactly such a pair: the producers hammer the write cursor
 * while the consumer hammers the read cursor. Placed adjacently, two cores fight over
 * one cache line, and measurements show throughput dropping by more than half.
 *
 * <p>Seven longs of padding on each side (56 bytes) plus the 8-byte value fill a cache
 * line exactly, so no matter what the neighbours do, they cannot touch it.
 *
 * <h2>Why an inheritance chain rather than padding fields in one class</h2>
 * The JVM reorders fields within a class -- grouping by size to reduce gaps -- and the
 * padding would very likely end up after the value, achieving nothing. But <b>a
 * superclass's fields always precede a subclass's</b>: that is a fixed rule of JVM
 * layout, which makes the inheritance chain the only reliable way. Disruptor and
 * JCTools both do it like this.
 *
 * <p>The {@code @Contended} annotation would have the same effect, but it is disabled
 * by default and needs {@code -XX:-RestrictContended} to work -- and something that
 * depends on a JVM flag does not belong in a library.
 *
 * <p>The class is {@code final} because that chain <i>is</i> the memory layout:
 * subclassing it would add fields past the right-hand padding and undo the isolation.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
final class PaddedCursor extends PaddedCursorRhs {

    private static final VarHandle VALUE;

    static {
        try {
            // value is declared on the superclass, so the lookup has to point there too
            VALUE = MethodHandles.privateLookupIn(PaddedCursorValue.class, MethodHandles.lookup())
                    .findVarHandle(PaddedCursorValue.class, "value", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    PaddedCursor(long initial) {
        this.value = initial;
    }

    /** Volatile read. */
    long get() {
        return value;
    }

    /**
     * Plain read, with no memory barrier.
     *
     * <p>Only the <b>sole writer of the field</b> may read its own cursor this way: what
     * it reads is necessarily what it just wrote. The consumer reading its own read
     * cursor is exactly that case, and it saves one volatile read.
     */
    long getPlain() {
        return (long) VALUE.get(this);
    }

    /**
     * A write with release semantics.
     *
     * <p>Cheaper than a volatile write: it guarantees that earlier writes are not
     * reordered past it, without forcing a full pipeline flush. Publishing a slot wants
     * precisely that -- the data must be written before the marker becomes visible to
     * anyone else.
     */
    void setRelease(long newValue) {
        VALUE.setRelease(this, newValue);
    }

    boolean compareAndSet(long expected, long newValue) {
        return VALUE.compareAndSet(this, expected, newValue);
    }

    @Override
    public String toString() {
        return String.valueOf(get());
    }
}

/** Left-hand padding, separating the value from whatever precedes it. */
abstract class PaddedCursorLhs {

    @SuppressWarnings("unused")
    private long p01, p02, p03, p04, p05, p06, p07;
}

/** The value itself, sandwiched between the two layers of padding. */
abstract class PaddedCursorValue extends PaddedCursorLhs {

    volatile long value;
}

/** Right-hand padding, separating the value from whatever follows. Padding one side alone achieves nothing. */
abstract class PaddedCursorRhs extends PaddedCursorValue {

    @SuppressWarnings("unused")
    private long p09, p10, p11, p12, p13, p14, p15;
}
