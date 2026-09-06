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
import com.chaconneai.spreader.util.ExecutorUtils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The concurrent probing logic shared by every {@link IpRange} implementation.
 *
 * <p>Subclasses answer one question -- <i>which hosts</i> ({@link #hosts()}) -- and the
 * knocking is done once, here.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public abstract class AbstractIpRange implements IpRange {

    private final NodeProbe probe;
    private final int concurrency;
    private final Supplier<Boolean> stopSignal;

    protected final Logger log;

    protected AbstractIpRange(NodeProbe probe, int concurrency,
                              Supplier<Boolean> stopSignal, Logger log) {
        this.probe = probe;
        this.concurrency = Math.max(1, concurrency);
        this.stopSignal = stopSignal == null ? () -> Boolean.FALSE : stopSignal;
        this.log = log;
    }

    /** The host addresses this strategy covers, without ports. */
    protected abstract List<String> hosts();

    /** A short description for log output. */
    protected abstract String describe();

    @Override
    public Node lookup(int port) {
        List<Node> found = probeAll(port, true);
        return found.isEmpty() ? null : found.get(0);
    }

    @Override
    public List<Node> lookupAll(int port) {
        return probeAll(port, false);
    }

    /**
     * Knocks on every target concurrently.
     *
     * @param stopAtFirst stop at the first hit and cancel the probes not yet started
     */
    private List<Node> probeAll(int port, boolean stopAtFirst) {
        List<InetSocketAddress> targets = targets(port);
        if (targets.isEmpty() || isStopped()) {
            return List.of();
        }

        int threads = Math.min(concurrency, targets.size());
        // A scan is a one-shot batch: the task count is known up front (targets.size()),
        // so the queue is sized to hold exactly that and nothing can be rejected.
        // The backpressure policy is a safety net that normal runs never reach
        ExecutorService pool = ExecutorUtils.backpressured(
                "gossip-discovery", threads, Math.max(16, targets.size()));
        try {
            // CompletionService hands back results in completion order, which is what
            // makes stopping the moment there is a hit possible. invokeAll would have to
            // wait for every task to finish, so it cannot bail out early
            CompletionService<Node> completion = new ExecutorCompletionService<>(pool);
            for (InetSocketAddress addr : targets) {
                completion.submit(() -> isStopped() ? null : probe.probe(addr));
            }

            List<Node> result = new ArrayList<>();
            for (int i = 0; i < targets.size(); i++) {
                try {
                    Node n = completion.take().get();
                    if (n == null) {
                        continue;
                    }
                    result.add(n);
                    if (stopAtFirst) {
                        int skipped = targets.size() - i - 1;
                        if (skipped > 0) {
                            log.debug("Found an entry node at {}, skipping the remaining {} address(es)", n.address(), skipped);
                        }
                        break;
                    }
                } catch (ExecutionException e) {
                    // A single address failing to answer is business as usual
                }
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } finally {
            pool.shutdownNow();
            try {
                pool.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Hosts joined with the port, de-duplicated: the actual probe targets. */
    private List<InetSocketAddress> targets(int port) {
        List<String> hosts;
        try {
            hosts = hosts();
        } catch (RuntimeException e) {
            log.warn("{} could not be parsed and is skipped: {}", describe(), e.getMessage());
            return List.of();
        }
        Set<InetSocketAddress> targets = new LinkedHashSet<>(hosts.size());
        for (String host : hosts) {
            try {
                targets.add(new InetSocketAddress(host.trim(), port));
            } catch (RuntimeException e) {
                log.warn("Ignoring malformed address [{}]: {}", host, e.getMessage());
            }
        }
        return new ArrayList<>(targets);
    }

    protected boolean isStopped() {
        return Boolean.TRUE.equals(stopSignal.get());
    }

    @Override
    public String toString() {
        return describe();
    }
}
