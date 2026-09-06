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
package com.chaconneai.spreader;

import com.chaconneai.spreader.loadbalance.LoadBalancer;
import com.chaconneai.spreader.transport.TransportProvider;
import com.chaconneai.spreader.transport.TransportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster configuration. Built only through {@link #builder()}, and immutable once built
 * -- which is why the class is {@code final}: a subclass could introduce mutable state
 * and break a guarantee the rest of the system relies on.
 *
 * <h2>Two ports</h2>
 * <ul>
 *   <li>{@link #clusterPort()}, 22000 by default -- <b>the cluster port</b>. Every node
 *       agrees on one value; whoever claims it is the leader, and it is also the door
 *       every new node knocks on.</li>
 *   <li>{@link #bindPort()}, 0 by default, meaning a random port between
 *       {@link #workPortMin()} and {@link #workPortMax()} -- <b>the work port</b>. Each
 *       node holds its own, and it is the address the node publishes in the member list.
 *       Nobody needs to know it in advance, so random is fine.</li>
 * </ul>
 *
 * <h2>Where to look for the cluster</h2>
 * {@link #ipAddresses()} and {@link #ipAddressRange()} are two ways of saying the same
 * thing: give a set of <b>hosts</b>, none of them carrying a port, and the node knocks on
 * the cluster port of each. At least one of the two must be configured.
 *
 * <pre>{@code
 * GossipConfig config = GossipConfig.builder()
 *         .clusterName("order-cluster")
 *         .clusterPort(22000)
 *         .ipAddresses("192.168.0.111", "192.168.0.63")
 *         .ipAddressRange("192.168.0.1-192.168.5.254")
 *         .build();
 * }</pre>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class GossipConfig {

    private final String clusterName;
    private final String nodeName;
    private final String bindHost;
    private final String advertiseHost;
    private final int clusterPort;
    private final int bindPort;
    private final int workPortMin;
    private final int workPortMax;
    private final int portAutoIncrementRetry;
    private final TransportType transportType;
    private final TransportProvider transportProvider;
    private final List<String> ipAddresses;
    private final List<String> ipAddressRange;
    private final int scanTimeoutMs;
    private final int scanConcurrency;
    private final boolean metricsEnabled;
    private final long rediscoverIntervalMs;
    private final long aloneRediscoverIntervalMs;
    private final long leaderQuietPeriodMs;
    private final long takeoverDelayMs;
    private final long gossipIntervalMs;
    private final int gossipFanout;
    private final int probeTimeoutMs;
    private final int indirectProbeNodes;
    private final long suspectTimeoutMs;
    private final long tombstoneTtlMs;
    private final int connectTimeoutMs;
    private final int maxMessageBytes;
    private final int workerThreads;
    private final int priority;
    private final boolean payloadAck;
    private final int payloadRetries;
    private final long payloadRetryDelayMs;
    private final long payloadDedupTtlMs;
    private final int payloadDedupMaxEntries;
    private final int payloadConcurrency;
    private final int payloadDispatchThreads;
    private final boolean connectionPoolEnabled;

    /**
     * Whether to use off-heap (direct) buffers for sending and receiving. Off by default.
     *
     * <h2>Why off by default</h2>
     * Direct buffers save the copy between the kernel and the heap. But this transport's
     * public API sends and receives {@code byte[]} -- the data has to end up on the heap
     * for the application anyway. So the copy that was saved is paid straight back at the
     * direct-to-heap step, and the net gain is close to nothing.
     *
     * <p>Outbound, more so: {@code Codec.encode()} already produces a {@code byte[]}, and
     * Netty's {@code Unpooled.wrappedBuffer} and MINA's {@code IoBuffer.wrap} merely
     * <b>wrap</b> it -- neither allocates nor copies to begin with.
     *
     * <h2>When it is worth turning on</h2>
     * Large messages at high frequency. The larger the datagram, the more that one copy
     * costs in absolute terms, while a direct buffer's allocation cost is fixed and only
     * pays off once amortised. On small messages it is slower -- off-heap allocation is
     * more expensive than on-heap.
     *
     * <p><b>Benchmark before turning it on</b>, and leave headroom in
     * {@code -XX:MaxDirectMemorySize}: off-heap memory is not bounded by the heap limit,
     * so a leak triggers no GC and simply gets the process killed by the system.
     */
    private final boolean directBuffers;
    private final int poolMaxIdlePerHost;
    private final long poolIdleTimeoutMs;
    private final LoadBalancer loadBalancer;
    private final Map<String, String> metadata;
    private final Logger log;

    private GossipConfig(Builder b) {
        this.clusterName = b.clusterName;
        this.nodeName = b.nodeName;
        this.bindHost = b.bindHost;
        this.advertiseHost = b.advertiseHost;
        this.clusterPort = b.clusterPort;
        // 0 means unspecified: the transport picks at random between workPortMin and workPortMax
        this.bindPort = b.bindPort;
        this.workPortMin = b.workPortMin;
        this.workPortMax = b.workPortMax;
        this.portAutoIncrementRetry = b.portAutoIncrementRetry;
        this.transportType = b.transportType;
        this.transportProvider = b.transportProvider;
        this.ipAddresses = List.copyOf(b.ipAddresses);
        this.ipAddressRange = List.copyOf(b.ipAddressRange);
        this.scanTimeoutMs = b.scanTimeoutMs;
        this.scanConcurrency = b.scanConcurrency;
        this.metricsEnabled = b.metricsEnabled;
        this.rediscoverIntervalMs = b.rediscoverIntervalMs;
        this.aloneRediscoverIntervalMs = b.aloneRediscoverIntervalMs;
        this.leaderQuietPeriodMs = b.leaderQuietPeriodMs;
        this.takeoverDelayMs = b.takeoverDelayMs;
        this.gossipIntervalMs = b.gossipIntervalMs;
        this.gossipFanout = b.gossipFanout;
        this.probeTimeoutMs = b.probeTimeoutMs;
        this.indirectProbeNodes = b.indirectProbeNodes;
        this.suspectTimeoutMs = b.suspectTimeoutMs;
        this.tombstoneTtlMs = b.tombstoneTtlMs;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.maxMessageBytes = b.maxMessageBytes;
        this.workerThreads = b.workerThreads;
        this.priority = b.priority;
        this.payloadAck = b.payloadAck;
        this.payloadRetries = b.payloadRetries;
        this.payloadRetryDelayMs = b.payloadRetryDelayMs;
        this.payloadDedupTtlMs = b.payloadDedupTtlMs;
        this.payloadDedupMaxEntries = b.payloadDedupMaxEntries;
        this.payloadConcurrency = b.payloadConcurrency;
        this.payloadDispatchThreads = b.payloadDispatchThreads;
        this.connectionPoolEnabled = b.connectionPoolEnabled;
        this.directBuffers = b.directBuffers;
        this.poolMaxIdlePerHost = b.poolMaxIdlePerHost;
        this.poolIdleTimeoutMs = b.poolIdleTimeoutMs;
        this.loadBalancer = b.loadBalancer == null ? LoadBalancer.roundRobin() : b.loadBalancer;
        this.metadata = b.metadata.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(b.metadata));
        this.log = b.log == null ? LoggerFactory.getLogger("com.chaconneai.spreader") : b.log;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The cluster name. Only nodes sharing it form one cluster. */
    public String clusterName() {
        return clusterName;
    }

    /**
     * This node's application name; in Spring Boot this usually holds
     * {@code spring.application.name}.
     *
     * <p>It plays <b>no part in dividing clusters</b> -- {@link #clusterName()} does that.
     * Several applications can share one cluster, and the name merely groups them, which
     * is what makes "send only to the order service" possible. Any number of instances may
     * share a name.
     */
    public String nodeName() {
        return nodeName;
    }

    /** The listening address; 0.0.0.0 by default. */
    public String bindHost() {
        return bindHost;
    }

    /** The IP published to everyone else; empty means detect this machine's address automatically. */
    public String advertiseHost() {
        return advertiseHost;
    }

    /**
     * The cluster port; 22000 by default.
     *
     * <p>Every node agrees on one value. It serves two roles at once: the credential of
     * leadership (claim it and you lead), and the entrance for new nodes (everyone knocks
     * on this port).
     */
    public int clusterPort() {
        return clusterPort;
    }

    /**
     * This node's work port; 0 by default, meaning a random port between
     * {@link #workPortMin()} and {@link #workPortMax()}.
     *
     * <p>Random is enough: the work port is merely this node's address, it travels with
     * the member list, and nobody needs to know it in advance. {@link #clusterPort()} is
     * the only port the cluster has to agree on.
     *
     * <p>When set explicitly and already taken, it steps upward, trying at most
     * {@link #portAutoIncrementRetry()} times.
     */
    public int bindPort() {
        return bindPort;
    }

    /** Lower bound for a random work port; 50000 by default. */
    public int workPortMin() {
        return workPortMin;
    }

    /** Upper bound for a random work port; 60000 by default. */
    public int workPortMax() {
        return workPortMax;
    }

    /** How many ports to step through when an explicitly set work port is taken; 5 by default. */
    public int portAutoIncrementRetry() {
        return portAutoIncrementRetry;
    }

    /** The transport protocol; TCP by default. */
    public TransportType transportType() {
        return transportType;
    }

    /** Who moves the bytes: plain NIO, Netty, MINA or Grizzly. */
    public TransportProvider transportProvider() {
        return transportProvider;
    }

    /**
     * An explicit list of hosts, without ports -- the port is {@link #clusterPort()}.
     * Host names work, so a containerised deployment can name services directly.
     */
    public List<String> ipAddresses() {
        return ipAddresses;
    }

    /**
     * Address ranges to scan. Semantically the same as {@link #ipAddresses()}, written as
     * ranges instead. CIDR, start-end pairs that may cross networks, and last-octet
     * shorthand are all accepted.
     */
    public List<String> ipAddressRange() {
        return ipAddressRange;
    }

    /** Per-address probe timeout during a range scan; 300ms by default. */
    public int scanTimeoutMs() {
        return scanTimeoutMs;
    }

    /** How many probes run concurrently during discovery; 64 by default. */
    public int scanConcurrency() {
        return scanConcurrency;
    }

    /**
     * Whether to collect observability data. <b>On</b> by default.
     *
     * <p>Collection is lock-free and adds a few atomic increments per message, which is
     * negligible. The switch exists for a reason nonetheless: where latency matters to an
     * extreme, turning it off leaves empty methods that the JIT inlines away entirely,
     * making it genuinely zero-cost.
     */
    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    /**
     * How often to re-probe the cluster port once already in a cluster; 30s by default.
     * It finds nodes that ended up in a different sub-cluster and merges them back.
     * &lt;= 0 turns it off.
     */
    public long rediscoverIntervalMs() {
        return rediscoverIntervalMs;
    }

    /** How often a node alone restarts full discovery; 5s by default. */
    public long aloneRediscoverIntervalMs() {
        return aloneRediscoverIntervalMs;
    }

    /**
     * The startup quiet period; 3s by default.
     *
     * <p>No leader is announced during it. When several nodes start at once and have not
     * yet seen one another, each may briefly believe it leads; the quiet period holds that
     * intermediate state back so the application is not triggered repeatedly.
     */
    public long leaderQuietPeriodMs() {
        return leaderQuietPeriodMs;
    }

    /**
     * The stagger unit for cluster-port takeover; 300ms by default.
     *
     * <p>After a leader departs, the node ranked n in the takeover order waits
     * {@code n x takeoverDelayMs} before going for the port, and the first waits not at
     * all. So under normal conditions the first always gets it and the rest never have to
     * act.
     */
    public long takeoverDelayMs() {
        return takeoverDelayMs;
    }

    /** The gossip interval; 1s by default. */
    public long gossipIntervalMs() {
        return gossipIntervalMs;
    }

    /** How many peers are picked at random each gossip round; 3 by default. */
    public int gossipFanout() {
        return gossipFanout;
    }

    /**
     * Whether business messages require an acknowledgement.
     *
     * <ul>
     *   <li>{@code true}, the default -- wait for the peer's ACK and retransmit when it
     *       does not come, at most {@link #payloadRetries()} times, then discard and report
     *       failure. The receiver de-duplicates on {@code sender + seq}, so retransmission
     *       never causes duplicate delivery.</li>
     *   <li>{@code false} -- sent is done: no waiting and no retransmission. Latency halves
     *       and throughput rises, but whether the peer received it is unknowable. Suited to
     *       notifications that can afford to be lost.</li>
     * </ul>
     */
    public boolean payloadAck() {
        return payloadAck;
    }

    /** Maximum retransmissions in ack mode; beyond that the message is discarded. 0 means none. */
    public int payloadRetries() {
        return payloadRetries;
    }

    /** The interval between two retransmissions. */
    public long payloadRetryDelayMs() {
        return payloadRetryDelayMs;
    }

    /** How long the receiver keeps de-duplication records. Retransmissions all happen
     *  within seconds, so the 60s default covers them comfortably. */
    public long payloadDedupTtlMs() {
        return payloadDedupTtlMs;
    }

    /** Ceiling on de-duplication records, so extreme traffic cannot grow them without bound. */
    public int payloadDedupMaxEntries() {
        return payloadDedupMaxEntries;
    }

    /** Send concurrency for multicast. Multicast sends in parallel, so the total time
     *  depends on the slowest member rather than the member count. */
    public int payloadConcurrency() {
        return payloadConcurrency;
    }

    /**
     * Dispatch concurrency for business messages on the receiving side; 1 (serial) by
     * default.
     *
     * <p>Raising it improves receive throughput at the cost of <b>no ordering guarantee
     * between business messages</b> -- including several sent back to back by one sender.
     * Cluster events (joins, departures, leader changes) are unaffected and stay strictly
     * serial.
     */
    public int payloadDispatchThreads() {
        return payloadDispatchThreads;
    }

    /** Whether sending and receiving use off-heap buffers. See the field documentation --
     *  there is a reason it is off by default. */
    public boolean directBuffers() {
        return directBuffers;
    }

    public boolean connectionPoolEnabled() {
        return connectionPoolEnabled;
    }

    /** How many idle connections may be cached per target address. */
    public int poolMaxIdlePerHost() {
        return poolMaxIdlePerHost;
    }

    /** Idle ceiling for pooled connections. Must be under the server's 60s idle reclaim time. */
    public long poolIdleTimeoutMs() {
        return poolIdleTimeoutMs;
    }

    /** Timeout for a single probe; 800ms by default. */
    public int probeTimeoutMs() {
        return probeTimeoutMs;
    }

    /** How many relays SWIM's indirect probe delegates to; 3 by default. */
    public int indirectProbeNodes() {
        return indirectProbeNodes;
    }

    /** The longest a node may stay SUSPECT before being declared departed; 5s by default. */
    public long suspectTimeoutMs() {
        return suspectTimeoutMs;
    }

    /** How long a departed node's tombstone is kept; 60s by default. */
    public long tombstoneTtlMs() {
        return tombstoneTtlMs;
    }

    /** TCP connect timeout; 1s by default. */
    public int connectTimeoutMs() {
        return connectTimeoutMs;
    }

    /** Ceiling on a single message; 8MB by default. Over UDP it is capped at what one
     *  datagram holds. */
    public int maxMessageBytes() {
        return maxMessageBytes;
    }

    /** Worker threads handling inbound messages; 16 by default. */
    public int workerThreads() {
        return workerThreads;
    }

    /**
     * Takeover priority: the lower the number, the higher the precedence. 0 by default.
     * Start time is only compared when priorities are equal, which makes this the knob for
     * nominating which machine should lead.
     */
    public int priority() {
        return priority;
    }

    /** The strategy for choosing a unicast target; round robin by default. */
    public LoadBalancer loadBalancer() {
        return loadBalancer;
    }

    /** Business metadata; it gossips out to the whole cluster. */
    public Map<String, String> metadata() {
        return metadata;
    }

    public Logger log() {
        return log;
    }

    /** The configuration builder. */
    public static final class Builder {
        private String clusterName = "default";
        private String nodeName = Node.DEFAULT_NAME;
        private String bindHost = "0.0.0.0";
        private String advertiseHost;
        private int clusterPort = 22000;
        private int bindPort = 0;
        private int workPortMin = 50_000;
        private int workPortMax = 60_000;
        private int portAutoIncrementRetry = 5;
        private TransportType transportType = TransportType.TCP;
        private TransportProvider transportProvider = TransportProvider.NIO;
        private final List<String> ipAddresses = new ArrayList<>();
        private final List<String> ipAddressRange = new ArrayList<>();
        private int scanTimeoutMs = 300;
        private int scanConcurrency = 64;
        private boolean metricsEnabled = true;
        private long rediscoverIntervalMs = 30_000L;
        private long aloneRediscoverIntervalMs = 5_000L;
        private long leaderQuietPeriodMs = 3_000L;
        private long takeoverDelayMs = 300L;
        private long gossipIntervalMs = 1_000L;
        private int gossipFanout = 3;
        private int probeTimeoutMs = 800;
        private int indirectProbeNodes = 3;
        private long suspectTimeoutMs = 5_000L;
        private long tombstoneTtlMs = 60_000L;
        private int connectTimeoutMs = 1_000;
        private int maxMessageBytes = 8 * 1024 * 1024;
        private int workerThreads = 16;
        private int priority = 0;
        private boolean payloadAck = true;
        private int payloadRetries = 2;
        private long payloadRetryDelayMs = 100L;
        private long payloadDedupTtlMs = 60_000L;
        private int payloadDedupMaxEntries = 100_000;
        private int payloadConcurrency = 8;
        private int payloadDispatchThreads = 1;
        private boolean connectionPoolEnabled = true;
        private boolean directBuffers = false;
        private int poolMaxIdlePerHost = 4;
        private long poolIdleTimeoutMs = 30_000L;
        private LoadBalancer loadBalancer;
        private final Map<String, String> metadata = new LinkedHashMap<>();
        private Logger log;

        /** The cluster name. Only nodes sharing it form one cluster. */
        public Builder clusterName(String clusterName) {
            this.clusterName = require(clusterName, "clusterName must not be empty");
            return this;
        }

        /**
         * This node's application name; in Spring Boot, {@code spring.application.name}.
         * Any number of instances may share one; unconfigured it is {@code "default"}.
         */
        public Builder nodeName(String nodeName) {
            this.nodeName = require(nodeName, "nodeName must not be empty");
            return this;
        }

        public Builder bindHost(String bindHost) {
            this.bindHost = require(bindHost, "bindHost must not be empty");
            return this;
        }

        /** Sets the published IP explicitly. Recommended where several interfaces exist. */
        public Builder advertiseHost(String advertiseHost) {
            this.advertiseHost = advertiseHost;
            return this;
        }

        /** The cluster port. Must be identical across the cluster; 22000 by default. */
        public Builder clusterPort(int port) {
            this.clusterPort = checkPort(port, "clusterPort");
            return this;
        }

        /** This node's work port. Leave it alone -- the default is random between 50000 and 60000. */
        public Builder bindPort(int port) {
            this.bindPort = checkPort(port, "bindPort");
            return this;
        }

        /** The range random work ports are drawn from; 50000 to 60000 by default. */
        public Builder workPortRange(int min, int max) {
            checkPort(min, "workPortMin");
            checkPort(max, "workPortMax");
            if (min > max) {
                throw new IllegalArgumentException("workPortMin exceeds workPortMax: " + min + " > " + max);
            }
            this.workPortMin = min;
            this.workPortMax = max;
            return this;
        }

        /** How many ports to step through when an explicitly set work port is taken. */
        public Builder portAutoIncrementRetry(int retry) {
            this.portAutoIncrementRetry = Math.max(0, retry);
            return this;
        }

        /** The transport protocol; TCP by default. */
        public Builder transportType(TransportType type) {
            if (type != null) {
                this.transportType = type;
            }
            return this;
        }

        /**
         * The transport framework; {@link TransportProvider#NIO} by default -- the
         * built-in implementation, with no dependency.
         *
         * <p>Netty, MINA and Grizzly each require adding their jar yourself. Configuring
         * one whose library is absent falls back to the built-in implementation with a warn
         * line, rather than failing startup.
         *
         * <p>All four are protocol-identical and can share one cluster -- which is exactly
         * how the comparative benchmarks are run.
         */
        public Builder transportProvider(TransportProvider provider) {
            if (provider != null) {
                this.transportProvider = provider;
            }
            return this;
        }

        /**
         * Appends explicit host addresses, without ports. May be called several times.
         * At least one of this and {@link #ipAddressRange} must be configured, or
         * {@link #build()} throws.
         */
        public Builder ipAddresses(String... addresses) {
            return ipAddresses(Arrays.asList(addresses));
        }

        /** Appends explicit host addresses. */
        public Builder ipAddresses(Collection<String> addresses) {
            addAll(this.ipAddresses, addresses);
            return this;
        }

        /**
         * Appends address ranges to scan. May be called several times.
         * At least one of this and {@link #ipAddresses} must be configured, or
         * {@link #build()} throws.
         */
        public Builder ipAddressRange(String... ranges) {
            return ipAddressRange(Arrays.asList(ranges));
        }

        /** Appends address ranges to scan. */
        public Builder ipAddressRange(Collection<String> ranges) {
            addAll(this.ipAddressRange, ranges);
            return this;
        }

        public Builder scanTimeoutMs(int ms) {
            this.scanTimeoutMs = positive(ms, "scanTimeoutMs");
            return this;
        }

        public Builder scanConcurrency(int n) {
            this.scanConcurrency = positive(n, "scanConcurrency");
            return this;
        }

        /** Whether to collect observability data; true by default. */
        public Builder metricsEnabled(boolean enabled) {
            this.metricsEnabled = enabled;
            return this;
        }

        public Builder rediscoverIntervalMs(long ms) {
            this.rediscoverIntervalMs = ms;
            return this;
        }

        /** How often a node alone restarts full discovery; &lt;= 0 turns it off. */
        public Builder aloneRediscoverIntervalMs(long ms) {
            this.aloneRediscoverIntervalMs = ms;
            return this;
        }

        /** The startup quiet period, during which no leader is announced. */
        public Builder leaderQuietPeriodMs(long ms) {
            this.leaderQuietPeriodMs = Math.max(0L, ms);
            return this;
        }

        /** The stagger unit for cluster-port takeover. */
        public Builder takeoverDelayMs(long ms) {
            this.takeoverDelayMs = Math.max(0L, ms);
            return this;
        }

        public Builder gossipIntervalMs(long ms) {
            this.gossipIntervalMs = positive(ms, "gossipIntervalMs");
            return this;
        }

        public Builder gossipFanout(int n) {
            this.gossipFanout = positive(n, "gossipFanout");
            return this;
        }

        public Builder probeTimeoutMs(int ms) {
            this.probeTimeoutMs = positive(ms, "probeTimeoutMs");
            return this;
        }

        public Builder indirectProbeNodes(int n) {
            this.indirectProbeNodes = Math.max(0, n);
            return this;
        }

        public Builder suspectTimeoutMs(long ms) {
            this.suspectTimeoutMs = positive(ms, "suspectTimeoutMs");
            return this;
        }

        public Builder tombstoneTtlMs(long ms) {
            this.tombstoneTtlMs = positive(ms, "tombstoneTtlMs");
            return this;
        }

        public Builder connectTimeoutMs(int ms) {
            this.connectTimeoutMs = positive(ms, "connectTimeoutMs");
            return this;
        }

        public Builder maxMessageBytes(int bytes) {
            this.maxMessageBytes = positive(bytes, "maxMessageBytes");
            return this;
        }

        public Builder workerThreads(int n) {
            this.workerThreads = positive(n, "workerThreads");
            return this;
        }

        /** Takeover priority: the lower the number, the higher the precedence. */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * The strategy for choosing a unicast target; {@link LoadBalancer#roundRobin()} by
         * default. {@link LoadBalancer#random()} and {@link LoadBalancer#hash()} are also
         * available.
         */
        public Builder loadBalancer(LoadBalancer loadBalancer) {
            this.loadBalancer = loadBalancer;
            return this;
        }

        public Builder metadata(String key, String value) {
            if (key != null && !key.isBlank()) {
                this.metadata.put(key, value == null ? "" : value);
            }
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            if (metadata != null) {
                metadata.forEach(this::metadata);
            }
            return this;
        }

        /** Takes over the component's internal logging. Unset, it logs to
         *  {@code org.slf4j.LoggerFactory.getLogger("com.chaconneai.spreader")}. */
        public Builder log(Logger log) {
            this.log = log;
            return this;
        }

        /**
         * Whether business messages require an acknowledgement. true, the default, waits
         * for the ACK and retransmits on failure; false sends and moves on.
         * This is an agreement between both ends: with false, a dedicated one-way message
         * type is used and the peer sends nothing back.
         */
        public Builder payloadAck(boolean payloadAck) {
            this.payloadAck = payloadAck;
            return this;
        }

        /** Maximum retransmissions in ack mode; beyond that the message is discarded. 0 means none. */
        public Builder payloadRetries(int payloadRetries) {
            if (payloadRetries < 0) {
                throw new IllegalArgumentException("payloadRetries must not be negative");
            }
            this.payloadRetries = payloadRetries;
            return this;
        }

        /** The interval between two retransmissions. */
        public Builder payloadRetryDelayMs(long ms) {
            if (ms < 0) {
                throw new IllegalArgumentException("payloadRetryDelayMs must not be negative");
            }
            this.payloadRetryDelayMs = ms;
            return this;
        }

        /** The receiver's de-duplication window. Set it shorter than the total
         *  retransmission time and retransmissions turn into duplicate deliveries. */
        public Builder payloadDedupTtlMs(long ms) {
            this.payloadDedupTtlMs = positive(ms, "payloadDedupTtlMs");
            return this;
        }

        public Builder payloadDedupMaxEntries(int n) {
            this.payloadDedupMaxEntries = positive(n, "payloadDedupMaxEntries");
            return this;
        }

        /** Send concurrency for multicast. */
        public Builder payloadConcurrency(int n) {
            this.payloadConcurrency = positive(n, "payloadConcurrency");
            return this;
        }

        /**
         * Dispatch concurrency for business messages on the receiving side; 1 (serial) by
         * default. Before raising it, confirm the application can accept messages arriving
         * out of order.
         */
        public Builder payloadDispatchThreads(int n) {
            this.payloadDispatchThreads = positive(n, "payloadDispatchThreads");
            return this;
        }

        /**
         * Switches sending and receiving to off-heap buffers. Off by default.
         *
         * <p>It affects Netty and MINA only; the built-in NIO transport is unaffected.
         * Benchmark before turning it on -- on small messages it is usually slower, see
         * {@link GossipConfig#directBuffers()}.
         */
        public Builder directBuffers(boolean enabled) {
            this.directBuffers = enabled;
            return this;
        }

        public Builder connectionPoolEnabled(boolean enabled) {
            this.connectionPoolEnabled = enabled;
            return this;
        }

        public Builder poolMaxIdlePerHost(int n) {
            this.poolMaxIdlePerHost = positive(n, "poolMaxIdlePerHost");
            return this;
        }

        /** Idle ceiling for pooled connections. Must be under the server's 60s idle reclaim time. */
        public Builder poolIdleTimeoutMs(long ms) {
            this.poolIdleTimeoutMs = positive(ms, "poolIdleTimeoutMs");
            return this;
        }

        public GossipConfig build() {
            if (ipAddresses.isEmpty() && ipAddressRange.isEmpty()) {
                throw new IllegalStateException(
                        "at least one of ipAddresses and ipAddressRange must be configured, so "
                                + "that other nodes can be found. For example "
                                + ".ipAddresses(\"192.168.0.111\", \"192.168.0.63\") "
                                + "or .ipAddressRange(\"192.168.0.1-192.168.5.254\")");
            }
            if (bindPort > 0 && bindPort == clusterPort) {
                throw new IllegalStateException(
                        "bindPort must not equal clusterPort(" + clusterPort + "): the cluster "
                                + "port has to be left for a leader to claim, and a work port sitting "
                                + "on it means no other node can ever win it. Leave the work port "
                                + "unset -- it defaults to a random port between " + workPortMin
                                + " and " + workPortMax);
            }
            // The server closes idle connections after 60s (TcpTransport.IDLE_TIMEOUT_MS).
            // Holding them longer than that means lending out nothing but connections the
            // peer has closed -- slower than not pooling at all.
            if (connectionPoolEnabled && poolIdleTimeoutMs >= 60_000L) {
                throw new IllegalStateException(
                        "poolIdleTimeoutMs(" + poolIdleTimeoutMs + ") must be under the server's "
                                + "60000ms idle reclaim time, or the pool caches nothing but "
                                + "connections the peer has already closed");
            }
            // The de-duplication window must cover the whole retransmission cycle, or the
            // last few retransmissions get taken for new messages and delivered twice
            long retryWindowMs = (long) payloadRetries * (probeTimeoutMs + payloadRetryDelayMs);
            if (payloadAck && payloadDedupTtlMs <= retryWindowMs) {
                throw new IllegalStateException(
                        "payloadDedupTtlMs(" + payloadDedupTtlMs + ") must exceed the "
                                + "retransmission window of " + retryWindowMs
                                + "ms（payloadRetries × (probeTimeoutMs + payloadRetryDelayMs)），"
                                + ", or the receiver takes retransmissions for new messages and "
                                + "delivers them to the application twice");
            }
            if (clusterPort >= workPortMin && clusterPort <= workPortMax) {
                throw new IllegalStateException(
                        "clusterPort(" + clusterPort + ") falls inside the random work port range "
                                + workPortMin + "-" + workPortMax + ", where this node's own work "
                                + "port could claim it. Adjust clusterPort or workPortRange");
            }
            return new GossipConfig(this);
        }

        private static void addAll(List<String> target, Collection<String> source) {
            if (source == null) {
                return;
            }
            for (String s : source) {
                if (s != null && !s.isBlank()) {
                    target.add(s.trim());
                }
            }
        }

        private static String require(String value, String message) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(message);
            }
            return value.trim();
        }

        private static int checkPort(int port, String name) {
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException(name + " is out of range: " + port);
            }
            return port;
        }

        private static int positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be greater than 0: " + value);
            }
            return value;
        }

        private static long positive(long value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be greater than 0: " + value);
            }
            return value;
        }
    }
}
