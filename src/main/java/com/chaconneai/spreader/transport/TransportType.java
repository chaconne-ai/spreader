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
