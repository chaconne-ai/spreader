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

/**
 * The fill level of one inbound buffer (a RingBuffer).
 *
 * <h2>Why these numbers deserve more attention than latency</h2>
 * Latency and error rate describe problems that have <b>already happened</b>; buffer
 * level describes one that is <b>on its way</b>.
 *
 * <ul>
 *   <li>{@link #usage()} sitting high → consumption is not keeping up with production,
 *       and one traffic spike away from dropping</li>
 *   <li>{@link #dropped()} non-zero → <b>messages are already gone</b>. When the buffer
 *       fills, new messages are discarded outright. Backpressure is not an option here:
 *       stalling the receive thread would stop heartbeats going out and get this node
 *       declared failed. So every increment of this number is real, lost work</li>
 * </ul>
 *
 * <p>And dropping is <b>silent</b> -- the sender does not learn about it, and neither
 * does the business code on the receiving side. Without exposing it here, overload
 * makes no sound at all.
 *
 * @param name     name of the buffer, usually the channel it serves (cache, mutex, rpc...)
 * @param pending  how many are queued right now
 * @param capacity the ceiling
 * @param dropped  total discarded. <b>Non-zero means messages really were lost</b>
 * @param handled  total processed
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public record BufferMetrics(
        String name,
        int pending,
        int capacity,
        long dropped,
        long handled) {

    /** Fill level, 0 to 1. Sustained above 0.7 is the point to grow the capacity or add consumer threads. */
    public double usage() {
        return capacity <= 0 ? 0d : (double) pending / capacity;
    }

    /** How much room is left. */
    public int remaining() {
        return Math.max(0, capacity - pending);
    }

    /**
     * Drop rate, 0 to 1: what fraction of arriving messages was never processed.
     *
     * <p>The denominator is handled plus dropped -- that is, everything that actually
     * arrived.
     */
    public double dropRate() {
        long total = handled + dropped;
        return total == 0 ? 0d : (double) dropped / total;
    }

    /** Whether anything was ever dropped. A yes-or-no question: if it happened, someone
     *  should look. There is no such thing as an acceptable amount. */
    public boolean hasDropped() {
        return dropped > 0;
    }

    @Override
    public String toString() {
        return "buffer[" + name + "] "
                + pending + "/" + capacity
                + String.format("(%.1f%%)", usage() * 100)
                + ", handled=" + handled
                + ", dropped=" + dropped
                + (hasDropped() ? String.format("(%.3f%%) <- messages were lost", dropRate() * 100) : "");
    }
}
