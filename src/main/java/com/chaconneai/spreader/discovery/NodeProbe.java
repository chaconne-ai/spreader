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

import java.net.InetSocketAddress;

/**
 * One probe: shake hands with an address and find out whether a node of this
 * cluster lives there.
 *
 * <p>Keeping it a functional interface lets every {@link IpRange} implementation
 * worry only about <i>which addresses to try</i>. None of them needs to know what
 * the handshake looks like, so none of them depends on the transport layer.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@FunctionalInterface
public interface NodeProbe {

    /**
     * @return the peer's node information when it belongs to this cluster; null if the
     *         address is unreachable, the probe times out, or the cluster name differs
     */
    Node probe(InetSocketAddress address);
}
