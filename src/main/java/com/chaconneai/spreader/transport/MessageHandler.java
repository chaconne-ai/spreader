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

import com.chaconneai.spreader.protocol.Message;

import java.net.InetSocketAddress;

/**
 * Handler for inbound messages.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@FunctionalInterface
public interface MessageHandler {

    /**
     * Handles one inbound message.
     *
     * @param message the inbound message
     * @param remote  address of the peer it came from
     * @return the message to write back, or {@code null} to send no response
     */
    Message handle(Message message, InetSocketAddress remote);
}
