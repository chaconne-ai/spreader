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

import org.slf4j.Logger;
import java.util.List;
import java.util.function.Supplier;

/**
 * An explicit list of hosts, backing the {@code ipAddresses} setting.
 *
 * <p>Hosts only, no ports -- the port is the cluster port every node agrees on,
 * and it arrives as the {@link #lookup(int)} argument. Host <i>names</i> work too
 * (Docker container names, compose service names, Kubernetes service names), so
 * containerised deployments never have to deal in IP addresses.
 *
 * <p>Few addresses and a high hit rate, which is why {@link CompositeIpRange}
 * tries this before scanning a range.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class FixedIpRange extends AbstractIpRange {

    private final List<String> hosts;

    public FixedIpRange(List<String> hosts, NodeProbe probe, int concurrency,
                        Supplier<Boolean> stopSignal, Logger log) {
        super(probe, concurrency, stopSignal, log);
        this.hosts = List.copyOf(hosts);
    }

    @Override
    protected List<String> hosts() {
        return hosts;
    }

    @Override
    protected String describe() {
        return "fixed addresses " + hosts;
    }
}
