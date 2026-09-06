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
