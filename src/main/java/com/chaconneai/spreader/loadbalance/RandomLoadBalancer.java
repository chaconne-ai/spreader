package com.chaconneai.spreader.loadbalance;

import com.chaconneai.spreader.Node;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks one at random. Stateless, and therefore thread-safe by construction.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RandomLoadBalancer implements LoadBalancer {

    @Override
    public Node choose(List<Node> candidates, Object key) {
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
    }
}
