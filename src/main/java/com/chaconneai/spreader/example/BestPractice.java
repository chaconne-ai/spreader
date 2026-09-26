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
package com.chaconneai.spreader.example;

import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.event.GossipListener;
import com.chaconneai.spreader.loadbalance.LoadBalancer;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.SplitBrainStatus;
import com.chaconneai.spreader.transport.TransportProvider;
import com.chaconneai.spreader.transport.TransportType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Best practice for spreader: a production-ready usage you can copy as it stands.
 *
 * <h2>The conclusion first</h2>
 * <b>The defaults are the production configuration.</b> Exactly one line genuinely has to
 * change -- where to look for peers:
 * <pre>{@code
 * GossipConfig config = GossipConfig.builder()
 *         .clusterName("order-cluster")             // keeps dev/staging/prod apart; recommended
 *         .ipAddresses("10.0.0.11", "10.0.0.12")    // required: where to look for peers
 *         .build();
 * }</pre>
 * The other thirty-odd knobs carry measured, calibrated defaults that the overwhelming
 * majority of projects never touch from first deployment to last. The "advanced
 * configuration" in {@link #tunedForProduction()} below is <b>not for copying</b> -- it is
 * for people who have actually run into the corresponding problem.
 *
 * <h2>What this gives you, and what it does not</h2>
 * <table border="1">
 *   <caption>The boundaries</caption>
 *   <tr><th>You get</th><th>You do not get</th></tr>
 *   <tr><td>peers found automatically, with no registry</td>
 *       <td>a strongly consistent membership view -- it is eventually consistent</td></tr>
 *   <tr><td>node arrivals and departures noticed within seconds</td>
 *       <td>arbitration under split brain -- both sides of a partition elect a leader</td></tr>
 *   <tr><td>a stable notion of "the leader"</td>
 *       <td>consensus-grade uniqueness of that leader</td></tr>
 *   <tr><td>messaging between members, unicast and multicast</td>
 *       <td>ordering guarantees or durability for those messages</td></tr>
 * </table>
 *
 * <p>What you get in exchange: no ZooKeeper, no etcd, no Consul -- one jar and one line of
 * configuration. The trade is plain enough; whether it suits your situation is your call.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class BestPractice {

    private BestPractice() {
    }

    // ==================================================================
    // 1. The minimum that works -- what most projects should look like
    // ==================================================================

    /**
     * The standard way to start in production.
     *
     * <p><b>Three things are configured</b>: the cluster name, the application name, and
     * where to look for peers. Everything else takes its default.
     */
    public static GossipCluster minimal() throws IOException {
        GossipConfig config = GossipConfig.builder()
                // The cluster name is the only means of isolation: nodes with different
                // names do not recognise one another even when the network can reach them.
                // Where dev, staging and prod share a network, forgetting this merges them
                // into one cluster
                .clusterName("order-cluster")
                // The application name. Instances sharing one are replicas of the same
                // application, and the layers above -- task dispatch, RPC -- use it to tell
                // who belongs with whom
                .nodeName("order-service")
                // Where to look for peers. A few fixed IPs are enough: a new node learns
                // the complete member list from them, so there is no need to list them all
                .ipAddresses("10.0.0.11", "10.0.0.12")
                .build();

        GossipCluster cluster = GossipCluster.create(config);
        cluster.start();
        return cluster;
    }

    /**
     * One extra line when running in a container.
     *
     * <p>A container has several interfaces (eth0, docker0, lo) and auto-detection may well
     * pick an internal address that no other node can reach. <b>Set advertiseHost
     * explicitly whenever running in a container</b>, to whichever address is reachable
     * from outside -- usually injected as an environment variable by the orchestrator.
     */
    public static GossipCluster inContainer(String podIp) throws IOException {
        GossipConfig config = GossipConfig.builder()
                .clusterName("order-cluster")
                .nodeName("order-service")
                .advertiseHost(podIp)
                .ipAddresses("order-service-0.order-headless", "order-service-1.order-headless")
                .build();

        GossipCluster cluster = GossipCluster.create(config);
        cluster.start();
        return cluster;
    }

    // ==================================================================
    // 2. After starting: wait until ready before doing any work
    // ==================================================================

    /**
     * Wait for two things after starting, before doing any work.
     *
     * <p>{@code start()} <b>returns immediately</b> -- finding peers means scanning a
     * range, which can take seconds. Use the cluster without waiting and for those seconds
     * {@code members()} holds only this node, so any "pick a peer" logic concludes there is
     * nobody else.
     *
     * <p>The two waits mean different things:
     * <ul>
     *   <li>{@code awaitJoin} -- the first round of discovery is over. Only then does the
     *       member list mean anything</li>
     *   <li>{@code awaitLeader} -- a leader has been settled. Anything that depends on one,
     *       such as locks or the cache, has to wait for this</li>
     * </ul>
     */
    public static void waitUntilReady(GossipCluster cluster) throws InterruptedException {
        if (!cluster.awaitJoin(30, TimeUnit.SECONDS)) {
            // A timeout does not mean failure -- the range may simply be large and slow to
            // scan. But log it, because "never finding any peers" and "the network is down"
            // look exactly alike
            System.out.println("first discovery round has not finished; the member list may be incomplete");
        }
        if (!cluster.awaitLeader(30, TimeUnit.SECONDS)) {
            System.out.println("no leader yet; anything that depends on one is unavailable for now");
        }
        System.out.println("cluster is ready with " + cluster.members().size() + " member(s)");
    }

    // ==================================================================
    // 3. Listening to events: note it down, kick something off, and nothing more
    // ==================================================================

    /**
     * How to write an event listener.
     *
     * <h2>Do no heavy work in a callback</h2>
     * Events are dispatched synchronously on the dispatch thread. A database query, an HTTP
     * call, or anything else that might block for hundreds of milliseconds <b>holds up
     * every event behind it</b> -- including notices that other nodes have joined or left.
     *
     * <p>For heavy work, submit a task to your own thread pool and let the callback return
     * at once.
     */
    public static GossipListener listener() {
        return new GossipListener() {

            @Override
            public void onClusterJoined(Node self, boolean alone) {
                // alone=true means nobody else answered, so this node leads for now. On a
                // first deployment that is normal; on the fifth node of a running cluster it
                // means discovery did not reach anyone, which is worth an alert
                System.out.println("joined: " + (alone
                        ? "the only node so far, so this one leads"
                        : "other nodes were found"));
            }

            @Override
            public void onNodeJoined(Node node) {
                // Noting it down is enough
                System.out.println("new peer: " + node.label());
            }

            @Override
            public void onNodeSuspect(Node node) {
                // Suspect and recovered come in pairs during a brief network wobble. Seeing
                // this pair rather than "left then joined" is how you tell a blip from a
                // restart
                System.out.println("suspect: " + node.label());
            }

            @Override
            public void onNodeRecovered(Node node) {
                System.out.println("recovered: " + node.label());
            }

            @Override
            public void onNodeLeft(Node node, boolean graceful) {
                // graceful=true means the peer left on its own terms (an ordinary
                // deployment); false means failure detection declared it dead (it crashed).
                // The two usually deserve different treatment: a deployment should not
                // raise an alert, a crash should
                System.out.println("peer left: " + node.label()
                        + (graceful ? " (graceful)" : " (declared failed)"));
            }

            @Override
            public void onLeaderChanged(Node previous, Node current, boolean selfIsLeader) {
                // While leadership changes hands, anything depending on it has a gap of a
                // few seconds. An INFO line belongs here -- it is what makes "why was it
                // erroring for those few seconds" answerable later
                System.out.println("leader changed: " + (current == null ? "vacant" : current.label())
                        + (selfIsLeader ? " (this node)" : ""));
            }

            @Override
            public void onLeaderLeft(Node node) {
                // Distinct from onLeaderChanged: this fires the moment the leader is gone,
                // while a successor has not been settled on yet. Anything that must have a
                // leader should start refusing or queueing here rather than a few seconds
                // later, when the failures would otherwise look unexplained
                System.out.println("leader left: " + node.label() + ", waiting for takeover");
            }

            @Override
            public void onLeaderBack(Node node) {
                // The old leader returned, which means the wobble was in the network rather
                // than in that process
                System.out.println("leader back: " + node.label());
            }

            @Override
            public void onPayload(Node sender, byte[] content) {
                System.out.println("message from " + sender.label() + ": "
                        + new String(content, StandardCharsets.UTF_8));
            }
        };
    }

    // ==================================================================
    // 4. Sending messages: three shapes, for entirely different purposes
    // ==================================================================

    /**
     * The three shapes a message can take.
     *
     * <p><b>Which one to use follows from who ought to do the work</b>:
     * <ul>
     *   <li>everyone needs to know -&gt; multicast</li>
     *   <li>anyone doing it once is enough -&gt; unicast, with the load balancer choosing</li>
     *   <li>it must be one particular node -&gt; unicast to that member</li>
     * </ul>
     */
    public static void sendingMessages(GossipCluster cluster) {
        byte[] payload = "config-changed".getBytes(StandardCharsets.UTF_8);

        // 1) Everyone needs to know. This node is excluded by default -- the process
        //    already knows, so it can simply act
        int delivered = cluster.multicast(payload);
        System.out.println("notified " + delivered + " peer(s)");

        // 2) Including this node. For things like "every instance refreshes its local
        //    cache", and ESPECIALLY important in a single-instance deployment: without the
        //    true, nothing goes out at all -- returning 0 with no error, which is very easy
        //    to mistake for normal
        cluster.multicast(payload, true);

        // 3) Anyone doing it once. The load balancer picks; round robin by default
        Node picked = cluster.unicast(payload);
        System.out.println("handed to " + (picked == null ? "nobody" : picked.label()));

        // 4) Only to instances of one application
        cluster.multicastTo("report-service", payload);

        // 5) One key landing on the same instance every time; requires LoadBalancer.hash()
        cluster.unicast("user-42", payload);
    }

    // ==================================================================
    // 5. Several applications in one cluster: the common case, and its trap
    // ==================================================================

    /**
     * One cluster often runs more than one application: the one doing the work, plus an API
     * facade, a batch job, a console. They share a cluster name so that they can see one
     * another, and {@code nodeName} is what keeps them apart.
     *
     * <p>{@code membersOf} is how you address them. This is the method to reach for, not
     * {@code members()}: "the other replicas of my own application" is almost always the
     * question being asked.
     */
    public static void severalApplications(GossipCluster cluster) {
        // My own replicas, which is who shares my work
        List<Node> myReplicas = cluster.membersOf(cluster.self().name());

        // Another application's instances, for sending it something
        List<Node> facades = cluster.membersOf("api-facade");

        System.out.println("my replicas: " + myReplicas.size()
                + ", facade instances: " + facades.size());
    }

    /**
     * An application that should join but never lead.
     *
     * <p>The leader keeps the lock and permit registers and the authoritative cache copy, so
     * it wants a long lived, evenly loaded instance. A facade that scales to zero overnight
     * and restarts on every deploy will take the port just as readily, and what you get is a
     * change of leader on every deployment. Say so explicitly instead:
     *
     * <p>Such a node still joins, still gossips, still receives work. It never claims the
     * cluster port, and the others skip it when working out whose turn it is to take over.
     *
     * <p>One thing to know: setting this on <b>every</b> application means the cluster has no
     * leader, and since the leader is also where new nodes knock to find the cluster, the
     * members will not discover each other either. That is a misconfiguration, reported as a
     * periodic warning rather than corrected.
     */
    public static GossipConfig followerOnly() {
        return GossipConfig.builder()
                .clusterName("order-cluster")
                .nodeName("api-facade")
                .ipAddresses("10.0.0.11", "10.0.0.12")
                .leaderEligible(false)
                .build();
    }

    /**
     * The trap: <b>do not</b> use {@code isLeader()} to make a scheduled task run once.
     *
     * <pre>{@code
     * // Wrong, where more than one application shares the cluster
     * if (cluster.isLeader()) {
     *     syncOrdersDaily();
     * }
     * }</pre>
     *
     * The leader is a cluster-wide notion that does not distinguish applications. Where an
     * api-facade instance holds the port, every replica of order-service sees
     * {@code isLeader() == false}, and the task <b>never runs at all</b>.
     *
     * <p>Picking one executor per application is what openspreader's
     * {@code @MultiProcessingScheduled} does. Use that, or elect within your own application
     * by comparing against {@code membersOf(self().name())} yourself.
     */
    public static void doNotDoThis() {
    }

    // ==================================================================
    // 6. What to watch in production
    // ==================================================================

    /**
     * Three numbers worth putting on a dashboard, and what each one tells you.
     *
     * <p>The two failures that produce no log line each have a counter here. That is the
     * reason to look at them at all: everything else announces itself.
     */
    public static void observability(GossipCluster cluster) {
        // 1. Split brain: two nodes each believing they lead. Normal operation has one
        // holder; occurrences counts every time that was not so, including episodes that
        // healed in under a second and left no trace anywhere else
        SplitBrainStatus split = cluster.splitBrainStatus();
        if (!split.healthy()) {
            System.out.println("split brain: holders=" + split.holders()
                    + ", for " + split.splittingDurationMillis() + "ms");
        }
        System.out.println("split brain episodes so far: " + split.occurrences());

        // 2. Buffers: every inbound queue is bounded, and a full one drops. Sustained usage
        // above 0.7 is the point to raise the capacity, and any dropped at all means
        // messages were lost, silently as far as the application is concerned
        for (BufferMetrics buffer : cluster.bufferMetrics()) {
            if (buffer.usage() > 0.7 || buffer.hasDropped()) {
                System.out.println("buffer " + buffer.name() + " usage=" + buffer.usage()
                        + " dropped=" + buffer.dropped());
            }
        }

        // 3. Per channel throughput and latency. Channels are independent, so read them
        // separately: a busy replication channel says nothing about a quiet one
        for (ChannelMetrics channel : cluster.metrics().values()) {
            if (!channel.isIdle()) {
                System.out.println("channel " + channel.channel()
                        + " tps=" + channel.tps()
                        + " errorRate=" + channel.errorRate()
                        + " inflight=" + channel.inflight());
            }
        }
    }

    // ==================================================================
    // 7. Advanced configuration: read this once you have the matching problem
    // ==================================================================

    /**
     * Advanced configuration. <b>Not for copying</b> -- every item here exists to solve one
     * specific problem, and until you have that problem you should leave it alone.
     *
     * <p>The defaults are measured and calibrated, and the most common outcome of changing
     * them is breaking something that was working.
     */
    public static GossipConfig tunedForProduction() {
        return GossipConfig.builder()
                .clusterName("order-cluster")
                .nodeName("order-service")
                .ipAddresses("10.0.0.11", "10.0.0.12")

                // ---- Only when a firewall demands a fixed port ----
                // The default picks at random between 50000 and 60000, moving on when taken
                .bindPort(50000)
                .portAutoIncrementRetry(10)

                // ---- Only raise this on an unsteady network that misjudges nodes as down ----
                // The cost of raising it is that a node genuinely crashing takes longer to notice
                .suspectTimeoutMs(8_000L)

                // ---- Only worth considering with very many members, in the hundreds ----
                // UDP has no connection setup and saves a great deal at that scale; messages
                // exceeding one datagram are fragmented automatically, so a large membership
                // still fits. The cost is that packet loss is absorbed only by gossip's
                // periodic repetition
                .transportType(TransportType.UDP)

                // ---- Only touch this to benchmark the transport layer ----
                // The default is the built-in implementation, with no dependency. All four
                // are protocol-identical, so switching changes no behaviour -- only the
                // performance profile
                .transportProvider(TransportProvider.NIO)

                // ---- Only change this when one key must land on one node ----
                // Round robin is the default and distributes most evenly
                .loadBalancer(LoadBalancer.hash())

                // ---- Business metadata, gossiped out with the member list ----
                // The place for things others need to know, such as which zone this instance
                // is in
                .metadata("zone", "az-1")
                .build();
    }

    // ==================================================================
    // 8. Shutting down: always leave gracefully
    // ==================================================================

    /**
     * Leaving gracefully.
     *
     * <p>{@code stop()} broadcasts a departure notice and the other nodes drop you
     * <b>immediately</b>. Without it they wait out the failure detection timeout, a few
     * seconds by default, before noticing you are gone -- and every request sent to you in
     * the meantime fails.
     *
     * <p>In Spring Boot the auto-configuration calls it, so there is nothing to write. Used
     * by hand, remember to register a shutdown hook.
     */
    public static void shutdownGracefully(GossipCluster cluster) {
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
    }

    // ==================================================================

    /**
     * A runnable node, so the pieces above can be watched rather than only read.
     *
     * <p>Start several in separate terminals and watch them find each other, elect a leader,
     * and notice when one is killed:
     *
     * <pre>
     * java -cp spreader-1.0.0-SNAPSHOT.jar com.chaconneai.spreader.example.BestPractice \
     *      --cluster=order-cluster \
     *      --port=22000 \
     *      --ipAddresses=192.168.0.111,192.168.0.63
     * </pre>
     *
     * <p>On one machine add {@code --portRetry=10} and the ports step past each other by
     * themselves.
     */
    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);

        GossipConfig.Builder builder = GossipConfig.builder()
                .clusterName(a.get("cluster", "demo-cluster"))
                .nodeName(a.get("app", "demo-app"))
                .clusterPort(Integer.parseInt(a.get("port", "22000")))
                .portAutoIncrementRetry(Integer.parseInt(a.get("portRetry", "5")));

        String advertise = a.get("advertiseHost", null);
        if (advertise != null) {
            builder.advertiseHost(advertise);
        }
        String addresses = a.get("ipAddresses", null);
        if (addresses != null) {
            builder.ipAddresses(addresses.split(","));
        }
        String range = a.get("ipAddressRange", null);
        if (range != null) {
            builder.ipAddressRange(range.split(","));
        }

        GossipCluster cluster = GossipCluster.create(builder.build());
        cluster.addListener(listener());
        cluster.start();
        shutdownGracefully(cluster);

        waitUntilReady(cluster);
        sendingMessages(cluster);
        severalApplications(cluster);
        observability(cluster);

        // Print the member list periodically, so convergence can actually be watched
        while (cluster.isRunning()) {
            printMembers(cluster);
            TimeUnit.SECONDS.sleep(10);
        }
    }

    /** The member list as it stands, which is what you want in front of you when debugging. */
    public static void printMembers(GossipCluster cluster) {
        Node leader = cluster.leader();
        StringBuilder sb = new StringBuilder();
        sb.append("\n===== cluster [").append(cluster.clusterName()).append("] =====\n");
        sb.append("this node: ").append(cluster.self().label())
                .append(cluster.isLeader() ? "  [leader]" : "").append('\n');
        sb.append("leader:    ").append(leader == null ? "vacant" : leader.label()).append('\n');
        sb.append("members:   ").append(cluster.members().size()).append('\n');
        for (Node n : cluster.members()) {
            sb.append("  - ").append(n.label())
                    .append("  state=").append(n.state())
                    .append(n.id().equals(cluster.self().id()) ? "  <- me" : "")
                    .append(leader != null && leader.id().equals(n.id()) ? "  <- leader" : "")
                    .append('\n');
        }
        System.out.println(sb);
    }

    /** A minimal command-line parser for {@code --key=value}. */
    record Args(Map<String, String> values) {

        static Args parse(String[] args) {
            Map<String, String> map = new LinkedHashMap<>();
            for (String arg : args) {
                if (!arg.startsWith("--")) {
                    continue;
                }
                int eq = arg.indexOf('=');
                if (eq > 2) {
                    map.put(arg.substring(2, eq).trim(), arg.substring(eq + 1).trim());
                }
            }
            return new Args(map);
        }

        String get(String key, String defaultValue) {
            String v = values.get(key);
            return v == null || v.isBlank() ? defaultValue : v;
        }
    }
}
