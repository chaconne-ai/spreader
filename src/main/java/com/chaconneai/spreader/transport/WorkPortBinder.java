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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The work-port binding strategy, shared by every transport implementation.
 *
 * <p>The work port is merely this node's own address. It travels with the member list,
 * so nobody needs to know it in advance -- hence the default of picking one at random
 * from a high range and trying another whenever one is taken. The cluster port is the
 * only port the cluster has to agree on.
 *
 * <p>It makes no demand on the channel type. Netty's, MINA's and Grizzly's connection
 * objects share no supertype with the JDK's {@code NetworkChannel}, and all that is
 * needed here is two actions -- bind a port, read back the port actually bound -- which
 * a pair of type parameters covers.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class WorkPortBinder {

    /** Maximum attempts when picking randomly. The range holds tens of thousands of
     *  ports, so colliding this many times means the configuration or the environment is
     *  at fault. */
    private static final int MAX_ATTEMPTS = 50;

    private WorkPortBinder() {
    }

    /** Binds a port and returns the resulting channel, throwing IOException when it is taken. */
    @FunctionalInterface
    public interface Binder<C> {
        C bind(int port) throws IOException;
    }

    /** Reads back the port the channel actually bound to. */
    @FunctionalInterface
    public interface PortReader<C> {
        int read(C channel) throws IOException;
    }

    /**
     * Binds the work port.
     *
     * <p>With {@code bindPort} configured it is used, stepping upward per
     * {@code portAutoIncrementRetry} when taken. Without it, ports are tried at random
     * between {@code workPortMin} and {@code workPortMax}.
     */
    public static <C> int bind(GossipConfig config, Binder<C> binder,
                               PortReader<C> portReader) throws IOException {
        IOException lastError = null;

        if (config.bindPort() > 0) {
            for (int i = 0; i <= config.portAutoIncrementRetry(); i++) {
                try {
                    return portReader.read(binder.bind(config.bindPort() + i));
                } catch (IOException e) {
                    lastError = e;
                }
            }
            throw new IOException("could not bind the work port: " + config.bindHost() + ":"
                    + config.bindPort() + " (tried "
                    + (config.portAutoIncrementRetry() + 1) + " port(s))", lastError);
        }

        int min = config.workPortMin();
        int max = config.workPortMax();
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            int port = ThreadLocalRandom.current().nextInt(min, max + 1);
            // The work port must not claim the cluster port; otherwise there would be
            // nothing left for anyone to win the leadership with
            if (port == config.clusterPort()) {
                continue;
            }
            try {
                return portReader.read(binder.bind(port));
            } catch (IOException e) {
                lastError = e;
            }
        }
        throw new IOException("could not bind the work port: " + MAX_ATTEMPTS
                + " random attempts between " + min + " and " + max + " all failed", lastError);
    }
}
