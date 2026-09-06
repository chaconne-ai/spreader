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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A single-node entry point, for multi-process and multi-machine trials.
 *
 * <pre>
 * java -cp spreader-1.0.0.jar com.chaconneai.spreader.example.GossipDemo \
 *      --cluster=order-cluster \
 *      --port=22000 \
 *      --ipAddresses=192.168.0.111,192.168.0.63 \
 *      --ipAddressRange=192.168.0.1-192.168.5.254
 * </pre>
 *
 * <p>When starting several instances on one machine, add {@code --portRetry=10} and the
 * ports step past each other automatically.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class GossipDemo {

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);

        GossipConfig.Builder builder = GossipConfig.builder()
                .clusterName(a.get("cluster", "default"))
                .clusterPort(Integer.parseInt(a.get("clusterPort", "22000")))
                .portAutoIncrementRetry(Integer.parseInt(a.get("portRetry", "5")))
                .priority(Integer.parseInt(a.get("priority", "0")))
                .metadata("app", a.get("app", "demo"));

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
        cluster.addListener(new ConsoleListener(cluster));
        cluster.start();

        // Graceful shutdown: broadcast a departure notice on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop, "demo-shutdown"));

        cluster.awaitJoin(60, TimeUnit.SECONDS);
        printMembers(cluster);

        // Print the member list every 10 seconds, so convergence can be watched
        while (cluster.isRunning()) {
            Thread.sleep(10_000L);
            printMembers(cluster);
        }
    }

    static void printMembers(GossipCluster cluster) {
        List<Node> members = cluster.members();
        Node leader = cluster.leader();
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== Cluster [").append(cluster.clusterName()).append("] members ==========\n");
        sb.append("this node: ").append(cluster.self().address())
                .append(cluster.isLeader() ? "  [leader]" : "").append('\n');
        sb.append("leader:    ").append(leader == null ? "none" : leader.address()).append('\n');
        sb.append("members:   ").append(members.size()).append('\n');
        for (Node n : members) {
            sb.append("  - ").append(n.address())
                    .append("  state=").append(n.state())
                    .append("  startTime=").append(n.startTime())
                    .append(n.id().equals(cluster.self().id()) ? "  <- me" : "")
                    .append(leader != null && leader.id().equals(n.id()) ? "  <- leader" : "")
                    .append('\n');
        }
        sb.append("================================================\n");
        System.out.println(sb);
    }

    /** Prints cluster events to the console. */
    static final class ConsoleListener implements GossipListener {
        private final GossipCluster cluster;

        ConsoleListener(GossipCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void onClusterJoined(Node self, boolean alone) {
            System.out.println(">>> joined the cluster; "
                    + (alone ? "the only node so far, so this one leads" : "other nodes were found"));
        }

        @Override
        public void onNodeJoined(Node node) {
            System.out.println(">>> [joined] " + node.address() + ", members now " + cluster.members().size());
        }

        @Override
        public void onNodeLeft(Node node, boolean graceful) {
            System.out.println(">>> [left] " + node.address()
                    + (graceful ? " (graceful)" : " (declared failed)")
                    + ", members now " + cluster.members().size());
        }

        @Override
        public void onNodeSuspect(Node node) {
            System.out.println(">>> [suspect] " + node.address());
        }

        @Override
        public void onNodeRecovered(Node node) {
            System.out.println(">>> [recovered] " + node.address());
        }

        @Override
        public void onLeaderChanged(Node previous, Node current, boolean selfIsLeader) {
            System.out.println(">>> [leader changed] "
                    + (previous == null ? "none" : previous.address())
                    + " -> " + (current == null ? "none" : current.address())
                    + (selfIsLeader ? "  (this node is now the leader)" : ""));
        }
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
