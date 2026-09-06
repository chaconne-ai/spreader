package com.chaconneai.spreader.discovery;

import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.protocol.GossipMessage;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.protocol.MessageType;
import com.chaconneai.spreader.transport.Transport;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Node discovery: knock on the agreed cluster port and find a way into the cluster.
 *
 * <p>This class holds no lookup logic of its own. It does two things:
 * <ol>
 *   <li>assembles the lookup strategy from configuration -- {@code ipAddresses} becomes
 *       a {@link FixedIpRange}, {@code ipAddressRange} a {@link StartEndIpRange}, and
 *       configuring both chains them into a {@link CompositeIpRange};</li>
 *   <li>supplies the knock itself (the {@link MessageType#PROBE} handshake) for the
 *       strategy to use.</li>
 * </ol>
 *
 * <p>The handshake checks {@code clusterName}: a node whose cluster name differs is
 * explicitly turned away even though its port is open. Several clusters can therefore
 * share one network safely.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class NodeDiscovery {

    private final GossipConfig config;
    private final Transport transport;
    private final Supplier<Node> selfSupplier;
    private final Logger log;
    private final CompositeIpRange ipRange;

    public NodeDiscovery(GossipConfig config, Transport transport,
                         Supplier<Node> selfSupplier, Supplier<Boolean> stopSignal) {
        this.config = config;
        this.transport = transport;
        this.selfSupplier = selfSupplier;
        this.log = config.log();
        this.ipRange = buildIpRange(stopSignal);
    }

    /**
     * Finds one entry node -- one is enough.
     *
     * <p>The cluster port has exactly one holder cluster-wide, so whoever answers is the
     * leader. Once the full member list has been fetched from it, every later exchange
     * is point-to-point and never goes through it again.
     *
     * @return the entry node, or null when the cluster has no nodes yet -- in which case
     *         the caller should go and claim the cluster port
     */
    public Node lookup() {
        return ipRange.lookup(config.clusterPort());
    }

    /**
     * Finds every node holding a cluster port.
     *
     * <p>Normally there is exactly one. More than one means the view has split -- several
     * machines started at the same time, none saw the others, and each claimed the
     * cluster port on its own host. Finding all of them is what makes merging back into
     * one cluster possible.
     */
    public List<Node> lookupAll() {
        return ipRange.lookupAll(config.clusterPort());
    }

    /** Whether any lookup strategy is configured at all. */
    public boolean isConfigured() {
        return !ipRange.isEmpty();
    }

    @Override
    public String toString() {
        return ipRange.toString();
    }

    // ------------------------------------------------------------------
    // Assembly
    // ------------------------------------------------------------------

    private CompositeIpRange buildIpRange(Supplier<Boolean> stopSignal) {
        List<IpRange> ranges = new ArrayList<>(2);

        // Few addresses and accurate, so they go first. The connect timeout is enough here
        if (!config.ipAddresses().isEmpty()) {
            ranges.add(new FixedIpRange(config.ipAddresses(),
                    addr -> probe(addr, config.connectTimeoutMs()),
                    config.scanConcurrency(), stopSignal, log));
        }
        // Range scanning knocks on far more doors, so it uses a shorter per-address
        // timeout and does not get held up by a handful of unreachable ones
        if (!config.ipAddressRange().isEmpty()) {
            ranges.add(new StartEndIpRange(config.ipAddressRange(),
                    addr -> probe(addr, config.scanTimeoutMs()),
                    config.scanConcurrency(), stopSignal, log));
        }
        return new CompositeIpRange(ranges);
    }

    // ------------------------------------------------------------------
    // Knocking
    // ------------------------------------------------------------------

    /**
     * Sends a PROBE handshake to one address.
     *
     * @return the peer's node information, or null if the connection failed, timed out,
     *         the cluster name did not match, or the address turned out to be this node
     */
    private Node probe(InetSocketAddress addr, int timeoutMs) {
        Node self = selfSupplier.get();
        GossipMessage probe = GossipMessage.builder(MessageType.PROBE, config.clusterName())
                .sender(self)
                .seq(transport.nextSeq())
                .build();
        try {
            // Direct, bypassing the connection pool: a scan hits a great many addresses
            // that do not exist, and pooling those would only poison it
            Message ack = transport.requestDirect(addr, probe, timeoutMs);
            if (ack.type() != MessageType.PROBE_ACK) {
                return null;
            }
            if (!ack.ok()) {
                log.debug("The node at {} refused the handshake: {}", addr, ack.errorMessage());
                return null;
            }
            Node peer = ack.sender();
            if (peer == null || peer.id().equals(self.id())) {
                // Knocked on the cluster port this very node holds; ignore it
                return null;
            }
            // Periodic re-probing hits the same nodes over and over, so this stays at
            // debug. Actual membership changes are logged at info by MemberList
            log.debug("Probe found a node of this cluster: {}", peer);
            return peer;
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug("Probe to {} got no answer: {}", addr, e.getClass().getSimpleName());
            }
            return null;
        }
    }
}
