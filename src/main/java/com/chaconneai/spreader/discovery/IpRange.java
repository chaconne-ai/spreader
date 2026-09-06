package com.chaconneai.spreader.discovery;

import com.chaconneai.spreader.Node;

import java.util.List;

/**
 * The strategy for <i>where to go looking for the cluster</i>.
 *
 * <p>Whether the machines are listed out one by one ({@link FixedIpRange}) or handed
 * over as a range to scan ({@link StartEndIpRange}), the layer above sees the same
 * thing: <b>knock on this set of hosts at this port</b>. One interface serves both,
 * and callers use {@link #lookup} without caring where the addresses came from.
 *
 * <p>There is a single port parameter because the whole cluster agrees on one cluster
 * port (22000 by default), and whoever is sitting on it is the leader -- so finding
 * it is the same as finding the cluster.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface IpRange {

    /**
     * Finds one entry node -- <b>one is enough</b>.
     *
     * <p>A joining node only has to reach any single member to get the full member list
     * back from it; gossip converges the rest. So this returns the moment it hits and
     * never walks the whole address list: a /24 scan is 254 probes, and stopping early
     * takes startup from several seconds down to usually under one.
     *
     * @param port the port to probe, i.e. the cluster port
     * @return a node of this cluster, or null if none answered
     */
    Node lookup(int port);

    /**
     * Finds every reachable entry node.
     *
     * <p>{@link #lookup} is how a node gets in the door at startup; this one is for the
     * running self-check. When a startup race leaves several nodes in separate
     * sub-clusters, each sub-cluster has a node holding a cluster port, and only by
     * finding all of them can the split views be merged back together.
     */
    List<Node> lookupAll(int port);
}
