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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round robin.
 *
 * <p>The candidate list comes from the membership view, so its length changes as nodes
 * join and leave. The counter is therefore treated as a monotonically increasing
 * cursor and taken modulo the current size on every call. The distribution jumps
 * slightly at the instant membership changes, which for load balancing does not
 * matter.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private final AtomicInteger cursor = new AtomicInteger();

    @Override
    public Node choose(List<Node> candidates, Object key) {
        if (candidates.size() == 1) {
            // Nothing to rotate through. Note it also does not advance the cursor:
            // spinning it during a single-node stretch would make the starting point
            // effectively random once a second node shows up
            return candidates.get(0);
        }
        // floorMod, so a counter that has overflowed into negative territory
        // does not produce a negative index
        int index = Math.floorMod(cursor.getAndIncrement(), candidates.size());
        return candidates.get(index);
    }
}
