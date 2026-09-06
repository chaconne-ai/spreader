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
package com.chaconneai.spreader.discovery;

import com.chaconneai.spreader.Node;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Chains several lookup strategies together and tries them in order.
 *
 * <p>The order matters: fixed addresses come before range scanning, so a hit on the
 * former means the latter never runs. Scanning is the single most expensive step in
 * startup, and worth skipping wherever possible.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class CompositeIpRange implements IpRange {

    private final List<IpRange> delegates;

    public CompositeIpRange(List<IpRange> delegates) {
        this.delegates = List.copyOf(delegates);
    }

    /** Whether the composite holds no strategy at all -- the case when both discovery settings are blank. */
    public boolean isEmpty() {
        return delegates.isEmpty();
    }

    @Override
    public Node lookup(int port) {
        for (IpRange range : delegates) {
            Node found = range.lookup(port);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Unlike {@link #lookup}, this runs every strategy to completion. The point is to
     * find <i>all</i> split-off sub-clusters, and one that is missed is one that never
     * gets merged back.
     */
    @Override
    public List<Node> lookupAll(int port) {
        Set<Node> all = new LinkedHashSet<>();
        for (IpRange range : delegates) {
            all.addAll(range.lookupAll(port));
        }
        return new ArrayList<>(all);
    }

    @Override
    public String toString() {
        return delegates.toString();
    }
}
