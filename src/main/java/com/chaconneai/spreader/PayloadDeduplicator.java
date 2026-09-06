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
package com.chaconneai.spreader;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * De-duplicates business messages, narrowing "at least once" down to "delivered once in
 * practice".
 *
 * <p>Two things can make the same message arrive twice:
 * <ul>
 *   <li>a timeout retransmission by the sender in ack mode -- the peer may have finished
 *       processing already, and only the ack was lost on the way back</li>
 *   <li>a half-open connection borrowed from the pool and retried on a fresh one -- the
 *       request may already have reached the peer</li>
 * </ul>
 * In both cases the message's {@code sender + seq} is unchanged (a retransmission
 * deliberately reuses the same seq), which is what makes the duplicate recognisable.
 *
 * <h2>It is not infallible</h2>
 * Records are kept for {@code ttlMs} only, 60 seconds by default. A duplicate arriving
 * after that window goes unrecognised -- but retransmissions all happen within seconds,
 * so normal operation never reaches it. At the entry ceiling, expired records are swept
 * first, and if that frees nothing the message is <b>let through</b>: delivering twice is
 * preferable to judging a new message a duplicate and dropping it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
class PayloadDeduplicator {

    private final long ttlMs;
    private final int maxEntries;

    /** key = senderId + '#' + seq, value = expiry timestamp. */
    private final ConcurrentHashMap<String, Long> seen = new ConcurrentHashMap<>();

    PayloadDeduplicator(long ttlMs, int maxEntries) {
        this.ttlMs = ttlMs;
        this.maxEntries = maxEntries;
    }

    /**
     * Records a message.
     *
     * @return true when this message has been seen before and should be discarded
     */
    boolean isDuplicate(String senderId, long seq) {
        long now = System.currentTimeMillis();
        if (seen.size() >= maxEntries) {
            evictExpired(now);
        }
        if (seen.size() >= maxEntries) {
            // Even when full, nothing may be misjudged as a duplicate; let it through
            return false;
        }
        Long previousExpiry = seen.putIfAbsent(senderId + '#' + seq, now + ttlMs);
        if (previousExpiry == null) {
            return false;
        }
        if (previousExpiry < now) {
            // The old record has expired, so treat this as new and push the expiry out
            seen.put(senderId + '#' + seq, now + ttlMs);
            return false;
        }
        return true;
    }

    /** Periodic cleanup, called from the scheduler thread. */
    void evictExpired() {
        evictExpired(System.currentTimeMillis());
    }

    private void evictExpired(long now) {
        for (Iterator<Map.Entry<String, Long>> it = seen.entrySet().iterator(); it.hasNext(); ) {
            if (it.next().getValue() < now) {
                it.remove();
            }
        }
    }

    void clear() {
        seen.clear();
    }

    int size() {
        return seen.size();
    }
}
