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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A replicated H2 database built on the cluster: <b>the leader takes every write, and every
 * node reads its own local copy</b>.
 *
 * <h2>The shape of it</h2>
 * Each node runs its own embedded H2 with the same schema. Nothing is shared, so a read never
 * leaves the process and costs a local JDBC call. Writes take one path only:
 *
 * <pre>
 *   write on a follower      write on the leader
 *          |                        |
 *   sendToLeader(sql)               |
 *          |                        |
 *          +-----> the leader applies it locally
 *                           |
 *                  multicast(sql) to every replica
 *                           |
 *                  each applies it to its own H2
 * </pre>
 *
 * Funnelling writes through one node is what gives them an order: every replica applies the
 * same statements in the same sequence, so the copies converge without any merge logic.
 *
 * <h2>What it is good for, and what it is not</h2>
 * This is <b>asynchronous single leader replication</b>, which is a real design with real
 * limits. Stating them plainly:
 *
 * <ul>
 *   <li>A read on a follower can be a few milliseconds behind. Read your own write only
 *       through the leader</li>
 *   <li>If the leader dies between applying a write and broadcasting it, that write is lost.
 *       There is no write-ahead log and no acknowledgement from replicas</li>
 *   <li>Under a network partition each side can have a leader, and both accept writes. The
 *       copies then diverge, and nothing here merges them again</li>
 * </ul>
 *
 * So it fits data that is <b>read far more than written and tolerates a short lag</b>:
 * reference tables, dictionaries, feature flags, pricing rules, routing tables. It does not
 * fit ledgers, balances or stock counts. Those belong in a real database with transactions.
 *
 * <h2>Running it</h2>
 * H2 is needed at runtime only; this class compiles against {@code java.sql} alone.
 *
 * <pre>
 * java -cp spreader-1.0.0.jar:h2-2.2.224.jar \
 *      com.chaconneai.spreader.example.ReplicatedH2Example --port=22000
 * </pre>
 *
 * Start two or three of them (add {@code --portRetry=10} on one machine) and watch the row
 * count climb on every node while only the leader is inserting.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 26/09/2026
 */
public final class ReplicatedH2Example implements GossipListener, AutoCloseable {

    /**
     * Replication rides its own channel, so a burst of writes cannot hold up membership
     * gossip or anything else the application sends.
     */
    public static final String CHANNEL = "h2-replication";

    /** A follower asking the leader to perform a write. */
    private static final byte WRITE_REQUEST = 1;

    /** The leader telling every replica to apply a statement it has already applied. */
    private static final byte REPLICATE = 2;

    /** A node that has just joined asking for the current contents. */
    private static final byte SNAPSHOT_REQUEST = 3;

    /** The whole database as a script, in reply to the above. */
    private static final byte SNAPSHOT = 4;

    private final GossipCluster cluster;
    private final String jdbcUrl;
    private final AtomicLong applied = new AtomicLong();

    public ReplicatedH2Example(GossipCluster cluster, String jdbcUrl) {
        this.cluster = cluster;
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Creates the schema and starts listening for replication traffic.
     *
     * <p>The schema is created on every node rather than replicated, because DDL that arrives
     * before the table exists has nowhere to go. Treat it as part of the deployment, the same
     * way a migration tool would.
     */
    public void start(String... ddl) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement()) {
            for (String sql : ddl) {
                st.execute(sql);
            }
        }
        cluster.addListener(CHANNEL, this);
    }

    // ==================================================================
    // Reads: local, and never over the network
    // ==================================================================

    /**
     * Runs a query against this node's own copy.
     *
     * <p>No network, no leader, no coordination. This is the whole point of replicating
     * instead of sharing: reads scale with the number of nodes.
     */
    public List<Object[]> read(String sql) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int columns = meta.getColumnCount();
            while (rs.next()) {
                Object[] row = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    // ==================================================================
    // Writes: always through the leader
    // ==================================================================

    /**
     * Performs an insert, update or delete.
     *
     * <p>Callers do not have to know whether they are on the leader; this method takes care of
     * that. It returns as soon as the statement is on its way, so it is <b>fire and forget</b>
     * from the caller's point of view. Where a caller must know the write landed, have the
     * leader answer, or read back through the leader.
     */
    public void write(String sql) throws SQLException {
        byte[] message = encode(cluster.isLeader() ? REPLICATE : WRITE_REQUEST, sql);
        if (cluster.isLeader()) {
            // Apply first, then tell the others. The other order would have a replica holding
            // a row the leader itself does not
            apply(sql);
            cluster.multicastOn(CHANNEL, cluster.self().name(), message, false);
        } else if (!cluster.sendToLeaderOn(CHANNEL, message)) {
            // No leader at this instant, which happens for a few seconds after one dies.
            // Surfacing it beats silently dropping the write
            throw new SQLException("No leader to take the write; retry in a moment");
        }
    }

    // ==================================================================
    // The replication protocol, all four messages
    // ==================================================================

    @Override
    public void onPayload(Node sender, byte[] content) {
        byte type = content[0];
        String body = new String(content, 1, content.length - 1, StandardCharsets.UTF_8);
        try {
            switch (type) {
                case WRITE_REQUEST -> onWriteRequest(body);
                case REPLICATE -> apply(body);
                case SNAPSHOT_REQUEST -> onSnapshotRequest(sender);
                case SNAPSHOT -> onSnapshot(body);
                default -> {
                    // A newer node speaking a message this one does not know. Ignoring it is
                    // the right move: replication carries on, and a rolling upgrade does not
                    // need every node restarted at once
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Replication failed for a " + type + " message", e);
        }
    }

    /** Only the leader acts on these; anyone else received one because leadership just moved. */
    private void onWriteRequest(String sql) throws SQLException {
        if (!cluster.isLeader()) {
            return;
        }
        apply(sql);
        cluster.multicastOn(CHANNEL, cluster.self().name(), encode(REPLICATE, sql), false);
    }

    /**
     * A node that joins an established cluster starts with an empty database, so it asks for
     * the current contents.
     *
     * <p>Without this step a new replica answers reads with nothing at all, which is worse
     * than answering slowly.
     */
    @Override
    public void onClusterJoined(Node self, boolean alone) {
        if (!alone) {
            cluster.sendToLeaderOn(CHANNEL, encode(SNAPSHOT_REQUEST, ""));
        }
    }

    /** H2 hands over a whole database as a script, one statement a row. */
    private void onSnapshotRequest(Node sender) throws SQLException {
        if (!cluster.isLeader()) {
            return;
        }
        StringBuilder script = new StringBuilder();
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SCRIPT")) {
            while (rs.next()) {
                script.append(rs.getString(1)).append('\n');
            }
        }
        cluster.unicastOn(CHANNEL, sender, encode(SNAPSHOT, script.toString()));
    }

    private void onSnapshot(String script) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement()) {
            st.execute("DROP ALL OBJECTS");
            for (String statement : script.split(";\n")) {
                if (!statement.isBlank()) {
                    st.execute(statement);
                }
            }
        }
    }

    private void apply(String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
            applied.incrementAndGet();
        }
    }

    /** How many statements this node has applied, for watching replication catch up. */
    public long appliedCount() {
        return applied.get();
    }

    private static byte[] encode(byte type, String sql) {
        byte[] body = sql.getBytes(StandardCharsets.UTF_8);
        byte[] message = new byte[body.length + 1];
        message[0] = type;
        System.arraycopy(body, 0, message, 1, body.length);
        return message;
    }

    @Override
    public void close() {
        cluster.removeListener(this);
    }

    // ==================================================================

    /**
     * Start two or three of these. Every node inserts a row of its own every few seconds, and
     * every node ends up seeing all of them: the writes converge because they all went through
     * the leader, whichever node they were called on.
     */
    public static void main(String[] args) throws Exception {
        GossipConfig config = GossipConfig.builder()
                .clusterName("h2-cluster")
                .nodeName("catalog-service")
                .clusterPort(argInt(args, "port", 22000))
                .portAutoIncrementRetry(argInt(args, "portRetry", 5))
                .build();

        GossipCluster cluster = GossipCluster.create(config);
        // Every node gets its own in-memory database, named after its own port so that
        // several nodes on one machine do not land in the same one
        String url = "jdbc:h2:mem:catalog_" + config.clusterPort() + ";DB_CLOSE_DELAY=-1";
        ReplicatedH2Example store = new ReplicatedH2Example(cluster, url);

        cluster.start();
        store.start("CREATE TABLE IF NOT EXISTS product ("
                + "id IDENTITY PRIMARY KEY, name VARCHAR(64), price DECIMAL(10,2))");
        cluster.awaitJoin(60, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            store.close();
            cluster.stop();
        }, "h2-demo-shutdown"));

        int round = 0;
        while (cluster.isRunning()) {
            // Every node writes, leader or not. That is the point: the caller does not check
            // who it is, write() routes for it. On a follower the statement goes to the
            // leader, is applied there, and comes back to everyone as replication
            store.write(String.format(
                    "INSERT INTO product (name, price) VALUES ('widget-%s-%d', %d.99)",
                    cluster.self().shortId(), round, 10 + round));
            Thread.sleep(3_000L);
            List<Object[]> count = store.read("SELECT count(*) FROM product");
            System.out.printf("[%s%s] rows=%s applied=%d members=%d%n",
                    cluster.self().address(),
                    cluster.isLeader() ? " leader" : "",
                    count.get(0)[0], store.appliedCount(), cluster.members().size());
            round++;
        }
    }

    private static int argInt(String[] args, String key, int defaultValue) {
        for (String arg : args) {
            if (arg.startsWith("--" + key + "=")) {
                return Integer.parseInt(arg.substring(key.length() + 3));
            }
        }
        return defaultValue;
    }
}
