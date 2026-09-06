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
 * A host list expanded from address ranges, backing the {@code ipAddressRange} setting.
 *
 * <p>{@link Ipv4Range} documents the syntax: CIDR, start-end pairs (which may cross
 * network boundaries) and last-octet shorthand all work. The appeal is that a new
 * machine joins without anyone editing configuration; the cost is a great many more
 * doors to knock on. That is why {@link CompositeIpRange} puts it after the fixed
 * addresses -- a hit there means never getting this far.
 *
 * <p>Expansion is lazy, redone on each lookup, so a configuration change at runtime
 * takes effect.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class StartEndIpRange extends AbstractIpRange {

    private final List<String> expressions;

    public StartEndIpRange(List<String> expressions, NodeProbe probe, int concurrency,
                           Supplier<Boolean> stopSignal, Logger log) {
        super(probe, concurrency, stopSignal, log);
        this.expressions = List.copyOf(expressions);
    }

    @Override
    protected List<String> hosts() {
        return Ipv4Range.expandAll(expressions);
    }

    @Override
    protected String describe() {
        return "IP range " + expressions;
    }
}
