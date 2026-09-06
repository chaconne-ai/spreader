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

import java.lang.reflect.Constructor;

/**
 * Picks a {@link PayloadQueue} implementation based on what is on the classpath.
 *
 * <p>Same trick as {@code TransportFactory}. JCTools is an {@code optional}
 * dependency, so a bare {@code new JcToolsPayloadQueue<>()} would blow up
 * <b>at class-load time</b> for any project that did not pull it in. Loading it
 * reflectively turns that into one controlled check: no library, no problem --
 * the built-in implementation is used and nothing happens.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class PayloadQueues {

    private static final String JCTOOLS_PROBE = "org.jctools.queues.MpscArrayQueue";
    private static final String JCTOOLS_IMPL =
            "com.chaconneai.spreader.event.JcToolsPayloadQueue";

    /** Probed exactly once: every listener that gets created asks this question. */
    private static final boolean JCTOOLS_PRESENT = probe();

    private PayloadQueues() {
    }

    /** Whether JCTools is on the classpath. Check this when asking why the fast queue is not in use. */
    public static boolean jcToolsAvailable() {
        return JCTOOLS_PRESENT;
    }

    /** Creates a queue, preferring JCTools when it is available. */
    @SuppressWarnings("unchecked")
    public static <T> PayloadQueue<T> create(int capacity) {
        if (JCTOOLS_PRESENT) {
            try {
                Class<?> impl = Class.forName(JCTOOLS_IMPL, true,
                        PayloadQueues.class.getClassLoader());
                Constructor<?> ctor = impl.getDeclaredConstructor(int.class);
                ctor.setAccessible(true);
                return (PayloadQueue<T>) ctor.newInstance(capacity);
            } catch (Throwable t) {
                // The probe class is there but the implementation will not construct:
                // almost always a JCTools version mismatch. Fall back to the built-in
                // one -- same behaviour, merely slower. Not a reason to fail startup
                return new RingPayloadQueue<>(capacity);
            }
        }
        return new RingPayloadQueue<>(capacity);
    }

    private static boolean probe() {
        try {
            Class.forName(JCTOOLS_PROBE, false, PayloadQueues.class.getClassLoader());
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
