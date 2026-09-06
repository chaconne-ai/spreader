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
package com.chaconneai.spreader.loadbalance;

import com.chaconneai.spreader.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hashes the routing key so that one key always lands on the same member.
 *
 * <p>Candidates are sorted by node ID before the modulo, so every node computes the
 * same answer. Without that, the same key would route to different members depending
 * on who was asking, and the stickiness would be gone.
 *
 * <p>This is plain modulo, not consistent hashing: change the member count and most
 * keys change owner. That is fine for cache affinity. If you need minimal
 * reshuffling when membership moves, use
 * {@link ConsistentHashLoadBalancer} instead.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class HashLoadBalancer implements LoadBalancer {

    @Override
    public Node choose(List<Node> candidates, Object key) {
        if (candidates.size() == 1) {
            // Skips the copy and the sort below: with one candidate the answer is a foregone conclusion
            return candidates.get(0);
        }
        if (key == null) {
            return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
        }
        List<Node> sorted = new ArrayList<>(candidates);
        sorted.sort((a, b) -> a.id().compareTo(b.id()));
        int index = Math.floorMod(key.hashCode(), sorted.size());
        return sorted.get(index);
    }
}
