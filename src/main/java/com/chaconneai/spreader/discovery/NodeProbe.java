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
