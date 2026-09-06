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

/**
 * Transport protocol.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum TransportType {

    /**
     * TCP, the default. Messages have no size ceiling, and connection-level failures
     * (connection refused) come back immediately, which makes liveness decisions
     * clear-cut. The price is a connection setup on every probe.
     */
    TCP,

    /**
     * UDP, the original choice in the SWIM paper: no connection setup, which saves a
     * great deal at scale. The price is that a message must fit in one datagram
     * (roughly 64KB), and with enough members the member list may not.
     */
    UDP
}
