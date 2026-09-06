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
package com.chaconneai.spreader.transport;

import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.transport.nio.NioTcpTransport;
import com.chaconneai.spreader.transport.nio.NioUdpTransport;
import org.slf4j.Logger;
import java.lang.reflect.Constructor;

/**
 * Builds the transport implementation named by the configuration.
 *
 * <h2>Why reflection</h2>
 * spreader carries no third-party dependency of its own, and Netty, MINA and Grizzly
 * are all {@code optional}. A bare {@code new NettyTcpTransport(...)} here would blow up
 * <b>at class-load time</b> for any project without Netty -- and blow up somewhere
 * entirely unrelated, as a NoClassDefFoundError. Loading reflectively turns that into
 * one controlled check: no library, fall back, leave a warn, and the cluster starts
 * normally.
 *
 * <h2>Fallback rules</h2>
 * The default is {@link TransportProvider#NIO}, which never reaches the reflective path.
 * When another implementation is configured explicitly but its library is missing, this
 * <b>falls back rather than throwing</b> -- failing startup is far too steep a price for
 * a setting that merely picks a networking framework. The log says plainly what
 * happened.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class TransportFactory {

    private TransportFactory() {
    }

    /**
     * Builds a transport from the provider and type in {@code config}.
     */
    public static Transport create(GossipConfig config) {
        Logger log = config.log();
        TransportProvider requested = config.transportProvider();
        TransportProvider actual = resolve(requested, log);

        if (actual == TransportProvider.NIO) {
            return config.transportType() == TransportType.UDP
                    ? new NioUdpTransport(config)
                    : new NioTcpTransport(config);
        }

        String className = actual.implPrefix()
                + (config.transportType() == TransportType.UDP ? "UdpTransport" : "TcpTransport");
        try {
            Class<?> impl = Class.forName(className, true, TransportFactory.class.getClassLoader());
            Constructor<?> ctor = impl.getConstructor(GossipConfig.class);
            return (Transport) ctor.newInstance(config);
        } catch (Throwable t) {
            // The probe class is present but the implementation will not construct:
            // almost always a library version mismatch. Fall back rather than fail startup
            log.warn("Could not create the {} transport ({}); falling back to the built-in NIO implementation",
                    actual, t.toString());
            return config.transportType() == TransportType.UDP
                    ? new NioUdpTransport(config)
                    : new NioTcpTransport(config);
        }
    }

    /**
     * Confirms the provider is usable, falling back to NIO when it is not.
     */
    private static TransportProvider resolve(TransportProvider requested, Logger log) {
        if (requested == null) {
            return TransportProvider.NIO;
        }
        if (requested.available()) {
            return requested;
        }
        log.warn("{} is not on the classpath ({} is missing), so the transport falls back to "
                        + "the built-in NIO implementation. Add the dependency to use it, or set "
                        + "transport-provider to NIO explicitly to silence this line",
                requested, requested.probeClass());
        return TransportProvider.NIO;
    }
}
