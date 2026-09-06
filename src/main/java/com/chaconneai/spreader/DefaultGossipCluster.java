package com.chaconneai.spreader;

import com.chaconneai.spreader.discovery.NodeDiscovery;
import com.chaconneai.spreader.event.ClusterEvent;
import com.chaconneai.spreader.event.ClusterEventType;
import com.chaconneai.spreader.event.EventBus;
import com.chaconneai.spreader.event.GossipListener;
import com.chaconneai.spreader.loadbalance.LoadBalancer;
import com.chaconneai.spreader.membership.MemberList;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.MetricsRegistry;
import com.chaconneai.spreader.metrics.SplitBrainStatus;
import com.chaconneai.spreader.membership.MembershipChange;
import com.chaconneai.spreader.protocol.GossipMessage;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.protocol.MessageType;
import com.chaconneai.spreader.transport.Transport;
import com.chaconneai.spreader.transport.TransportFactory;
import com.chaconneai.spreader.util.NamedThreadFactory;
import com.chaconneai.spreader.util.NetworkUtils;
import org.slf4j.Logger;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default {@link GossipCluster}: it orchestrates discovery, membership propagation,
 * failure detection and port takeover into one whole.
 *
 * <p>Leadership here has exactly one source -- whether {@link Transport#open} won the
 * cluster port. No votes, no terms, no central node: whoever holds that port leads.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
class DefaultGossipCluster implements GossipCluster {

    /** Prefix for the framework's own coordination channels. Messages on them are unaffected by takeBreak. */
    private static final String SYSTEM_CHANNEL_PREFIX = "spreader.";

    private final GossipConfig config;
    private final Logger log;
    private final EventBus eventBus;
    private final Transport transport;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final CountDownLatch joinLatch = new CountDownLatch(1);
    private final CountDownLatch leaderLatch = new CountDownLatch(1);
    private final AtomicReference<Node> lastKnownLeader = new AtomicReference<>();

    /**
     * When the quiet period ends. No leader is announced before it.
     */
    private volatile long leaderAnnounceAfterMs;

    /**
     * Whether the first round of discovery has finished.
     */
    private final AtomicBoolean discoveryCompleted = new AtomicBoolean();

    /**
     * Whether a leader has ever been announced. Once one has, the quiet period no longer applies.
     */
    private final AtomicBoolean firstAnnounced = new AtomicBoolean();

    /**
     * Whether a takeover is already queued, so it is not scheduled twice.
     */
    private final AtomicBoolean takeoverPending = new AtomicBoolean();

    /**
     * When the cluster port was last re-probed.
     */
    private volatile long lastRediscoverMs;

    /**
     * De-duplication for business messages, so a timeout retransmission does not become a
     * duplicate delivery.
     */
    private final PayloadDeduplicator deduplicator;

    /**
     * Observability data collected per channel.
     *
     * <p>Collection lives <b>entirely at this layer</b>: every message sent or received
     * passes through here, and the layers above -- spreader-commons, the Prometheus export
     * -- only transform and expose it, never instrumenting again. Instrumenting twice
     * inevitably produces two sets of numbers that disagree, and nobody can say which to
     * believe.
     */
    private final MetricsRegistry metrics;

    /**
     * How many split brains have been detected. Monotonic.
     *
     * <p>The yielding mechanism converges the cluster back to a single leader on its own,
     * so a split is usually over in a flash -- it happened, it healed, and no trace is left.
     * This counter is that trace: during a split <b>both leaders are doing leader work</b>,
     * granting locks and coordinating cache writes, and even at a few hundred milliseconds
     * someone ought to know that the data from that moment may be suspect.
     */
    private final AtomicLong splitBrainCount = new AtomicLong();

    /** Timestamp of the most recent split-brain detection. */
    private volatile long lastSplitBrainAt;

    /**
     * When the current split began; 0 when healthy.
     *
     * <p>It is what makes "how long has it been split" computable -- and the <b>duration</b>
     * matters far more than the yes-or-no. A few hundred milliseconds is an ordinary race,
     * two nodes starting at almost the same moment; minutes without healing is what says
     * the yielding mechanism genuinely is not working.
     */
    private volatile long splitBrainSince;

    private MemberList memberList;
    private NodeDiscovery discovery;
    private ScheduledExecutorService scheduler;
    private ExecutorService probeExecutor;

    /**
     * The pool that sends multicasts in parallel. Kept separate from the gossip protocol's
     * threads, so no amount of business traffic can crush the protocol itself.
     */
    private ExecutorService payloadExecutor;

    DefaultGossipCluster(GossipConfig config) {
        this.config = config;
        this.log = config.log();
        this.eventBus = new EventBus(log, config.payloadDispatchThreads());
        this.deduplicator = new PayloadDeduplicator(
                config.payloadDedupTtlMs(), config.payloadDedupMaxEntries());
        this.metrics = config.metricsEnabled()
                ? new MetricsRegistry() : MetricsRegistry.DISABLED;
        this.transport = createTransport(config);
    }

    private static Transport createTransport(GossipConfig c) {
        return TransportFactory.create(c);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("the cluster node is already started; start() must not be called twice");
        }

        String host = config.advertiseHost() != null && !config.advertiseHost().isBlank()
                ? config.advertiseHost().trim()
                : NetworkUtils.detectLocalAddress();

        transport.setHandler(this::onMessage);
        transport.start();

        Node self = new Node(
                UUID.randomUUID().toString(),
                config.nodeName(),
                host,
                transport.boundPort(),
                System.currentTimeMillis(),
                0L,
                NodeState.ALIVE,
                config.priority(),
                false,
                config.metadata());

        this.memberList = new MemberList(self, config.tombstoneTtlMs(), log);
        this.discovery = new NodeDiscovery(config, transport, memberList::self, stopped::get);
        this.lastKnownLeader.set(null);
        this.lastRediscoverMs = System.currentTimeMillis();
        this.leaderAnnounceAfterMs = System.currentTimeMillis() + config.leaderQuietPeriodMs();

        this.scheduler = new ScheduledThreadPoolExecutor(
                2, new NamedThreadFactory("gossip-sched", true));
        ((ScheduledThreadPoolExecutor) scheduler).setRemoveOnCancelPolicy(true);
        this.probeExecutor = new ThreadPoolExecutor(
                2, Math.max(4, config.gossipFanout() * 2 + config.indirectProbeNodes()),
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1024),
                new NamedThreadFactory("gossip-probe", true),
                new ThreadPoolExecutor.DiscardPolicy());
        // Business messages must not be dropped the way probes are: a full queue has the
        // calling thread run the task (backpressure) rather than DiscardPolicy
        this.payloadExecutor = new ThreadPoolExecutor(
                1, Math.max(2, config.payloadConcurrency()),
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1024),
                new NamedThreadFactory("gossip-payload", true),
                new ThreadPoolExecutor.CallerRunsPolicy());

        log.info("Node started: cluster={}, name={}, id={}, workPort={}, clusterPort={}, transport={}/{}",
                config.clusterName(), self.name(), self.shortId(), self.address(),
                config.clusterPort(), config.transportType(),
                transport.getClass().getSimpleName());
        publish(ClusterEventType.SELF_STARTED, self, null, false);

        // Discovery runs in the background so the caller is not blocked; scanning a range
        // can take seconds
        Thread bootstrap = new Thread(this::bootstrap, "gossip-bootstrap");
        bootstrap.setDaemon(true);
        bootstrap.start();

        scheduleTasks();
    }

    private void bootstrap() {
        try {
            probe();
        } catch (Throwable t) {
            log.error("The startup discovery flow threw", t);
        } finally {
            discoveryCompleted.set(true);
            publish(ClusterEventType.CLUSTER_JOINED, memberList.self(), null, false);
            joinLatch.countDown();
            // The first discovery round is over; if the quiet period has passed too, this is
            // where the first leader announcement happens
            checkLeaderChange();
        }
    }

    @Override
    public boolean awaitJoin(long timeout, TimeUnit unit) throws InterruptedException {
        return joinLatch.await(timeout, unit);
    }

    @Override
    public boolean awaitLeader(long timeout, TimeUnit unit) throws InterruptedException {
        return leaderLatch.await(timeout, unit);
    }

    @Override
    public void stop() {
        if (!started.get() || !stopped.compareAndSet(false, true)) {
            return;
        }
        log.info("Node is leaving: {}", memberList.self().address());

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        // Release the cluster port before broadcasting, so the departure notice already
        // says "I am no longer the leader". The others can then begin taking over at once,
        // instead of waiting to discover the port is free for themselves
        releaseClusterPort();
        broadcastLeave();

        if (probeExecutor != null) {
            probeExecutor.shutdownNow();
        }
        if (payloadExecutor != null) {
            payloadExecutor.shutdownNow();
        }
        deduplicator.clear();
        transport.stop();

        Node self = memberList.self();
        publish(ClusterEventType.SELF_STOPPED, self, null, true);
        eventBus.close();
        joinLatch.countDown();
        leaderLatch.countDown();
        log.info("Node has left: {}", self.address());
    }

    @Override
    public boolean isRunning() {
        return started.get() && !stopped.get();
    }

    // ------------------------------------------------------------------
    // Discovery and port takeover
    // ------------------------------------------------------------------

    /**
     * One full round of discovery: knock first, and claim the cluster port if nobody
     * answers.
     *
     * <p>The order cannot be reversed. Across machines every host's cluster port is free, so
     * claiming without knocking first would have simultaneously starting nodes each claim
     * the port on their own machine, none of them aware of the others -- which is where two
     * leaders come from. Looking before claiming, together with the staggered takeover
     * delay, is what keeps there to one leader under normal conditions.
     */
    @Override
    public void probe() {
        if (stopped.get() || memberList == null) {
            return;
        }
        if (transport.holds(config.clusterPort())) {
            // This node is the leader; there is nothing to look for
            return;
        }

        Node entry = discovery.lookup();
        if (entry != null) {
            join(List.of(entry));
            return;
        }

        // Nobody answered: the cluster has no leader yet, so go and claim the port
        if (!takeClusterPort()) {
            // Someone got there first while claiming; knock once more to identify them
            Node winner = discovery.lookup();
            if (winner != null) {
                join(List.of(winner));
            }
        }
    }

    /**
     * Claims the cluster port.
     *
     * @return whether it was won
     */
    private boolean takeClusterPort() {
        InetSocketAddress bound = transport.open(config.clusterPort());
        if (bound == null) {
            return false;
        }
        Node updated = memberList.updateSelfClusterPort(true);
        if (updated != null) {
            checkLeaderChange();
            // Tell everyone who leads at once, rather than waiting for the next gossip round
            probeExecutor.execute(this::broadcast);
        }
        return true;
    }

    /**
     * Releases the cluster port and reflects that in the local view.
     */
    private void releaseClusterPort() {
        if (!transport.holds(config.clusterPort())) {
            return;
        }
        transport.close(config.clusterPort());
        memberList.updateSelfClusterPort(false);
    }

    /**
     * Schedules this node to take over at a staggered moment while leadership is vacant.
     *
     * <p>The first waits not at all; the node ranked n waits {@code n x takeoverDelayMs}.
     * Under normal conditions the first has long since claimed it, and when a later node's
     * turn comes it finds the port taken and joins that holder instead -- so the delay is
     * both the stagger and the safety net.
     */
    private void scheduleTakeover() {
        if (stopped.get() || transport.holds(config.clusterPort())) {
            return;
        }
        if (!takeoverPending.compareAndSet(false, true)) {
            return;
        }
        long delay = (long) memberList.selfRank() * config.takeoverDelayMs();
        try {
            scheduler.schedule(() -> {
                takeoverPending.set(false);
                try {
                    if (!stopped.get() && memberList.leader() == null) {
                        log.info("The cluster port is vacant; this node (rank {} in the takeover order) is attempting it",
                                memberList.selfRank() + 1);
                        probe();
                    }
                } catch (Throwable t) {
                    log.error("Taking over the cluster port failed", t);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            takeoverPending.set(false);
        }
    }

    /**
     * Yielding: this node holds the cluster port, but the view shows another holder that
     * ranks ahead of it.
     *
     * <p>This only arises after a split view heals -- two groups that could not see each
     * other, each having elected a leader. Once merged, the one ranking later releases its
     * port and the cluster is back to a single leader.
     */
    private void yieldClusterPortIfNeeded() {
        if (stopped.get() || !transport.holds(config.clusterPort())) {
            return;
        }
        Node leader = memberList.leader();
        Node self = memberList.self();
        if (leader == null || leader.id().equals(self.id())) {
            return;
        }
        // Reaching here is a split brain: this node holds the cluster port and can see
        // another holder that ranks ahead of it. Record it -- once it heals this is the
        // only trace left
        recordSplitBrain();
        log.warn("{} also holds the cluster port and ranks ahead of this node, so this node "
                + "yields leadership ({} split-brain occurrence(s) so far)",
                leader.address(), splitBrainCount.get());
        releaseClusterPort();
        checkLeaderChange();
        probeExecutor.execute(this::broadcast);
    }

    /**
     * The split-brain self-check.
     *
     * <h2>It is a different thing from yielding</h2>
     * {@link #yieldClusterPortIfNeeded()} fires only when <b>this node ought to yield</b> --
     * meaning the leader that ranks <b>ahead</b>, and every member that is <b>not</b> a
     * leader, can all see the split and record nothing. The result would be a split leaving
     * a trace on at most one node, invisible in the monitoring of every other.
     *
     * <p>So the self-check runs separately: <b>any</b> node seeing more than one holder in
     * its membership view records an occurrence. That way the problem is discoverable from
     * whichever node monitoring happens to be looking at.
     *
     * <p>One machine or many, the logic is identical -- the test is only "how many holders
     * are in the view" and has nothing to do with where the nodes run. What differs is the
     * likelihood.
     */
    private void checkSplitBrain() {
        if (stopped.get() || memberList == null) {
            return;
        }
        List<Node> holders = memberList.clusterPortHolders();
        if (holders.size() <= 1) {
            // Healed. Reset this round's timer so the next split is timed afresh
            long since = splitBrainSince;
            if (since != 0L) {
                splitBrainSince = 0L;
                log.info("Split brain healed after {} ms ({} occurrence(s) so far)",
                        System.currentTimeMillis() - since, splitBrainCount.get());
            }
            return;
        }
        // The start of this round is recorded once and later checks do not overwrite it,
        // or the duration could never be computed
        if (splitBrainSince == 0L) {
            splitBrainSince = System.currentTimeMillis();
        }
        recordSplitBrain();
        log.warn("Split-brain check: the cluster port is held by {} nodes at once {}; the ones "
                + "ranking later will yield ({} ms so far, {} occurrence(s) in total)",
                holders.size(),
                holders.stream().map(Node::address).toList(),
                System.currentTimeMillis() - splitBrainSince, splitBrainCount.get());
    }

    /**
     * Records one split brain.
     *
     * <p>It debounces: one split is seen over and over, on several nodes and across many
     * gossip messages, and counting every sighting would turn a single event into dozens
     * and rob the number of meaning. At most one per second -- a split exists as a stretch
     * of time rather than an instant anyway.
     */
    private void recordSplitBrain() {
        long now = System.currentTimeMillis();
        if (now - lastSplitBrainAt < 1_000L) {
            return;
        }
        lastSplitBrainAt = now;
        splitBrainCount.incrementAndGet();
    }

    /**
     * Sends JOIN to the entry nodes and gets their full member lists back.
     */
    private void join(List<Node> entries) {
        List<Node> pending = new ArrayList<>(entries.size());
        for (Node n : entries) {
            Node known = memberList.get(n.id());
            // A tombstoned node is worth rejoining too: it is in fact alive, and the JOIN
            // lets it see the misjudgement about itself and refute it, bringing itself back
            // into the member list
            if (known == null || !known.state().isMember()) {
                pending.add(n);
            }
        }
        if (pending.isEmpty()) {
            return;
        }

        int succeeded = 0;
        for (Node peer : pending) {
            GossipMessage joinMsg = GossipMessage.builder(MessageType.JOIN, config.clusterName())
                    .sender(memberList.self())
                    .members(memberList.allEntries())
                    .seq(transport.nextSeq())
                    .build();
            try {
                Message ack = transport.request(peer.socketAddress(), joinMsg,
                        config.probeTimeoutMs());
                if (ack.type() != MessageType.JOIN_ACK || !ack.ok()) {
                    log.warn("Node {} refused this node's join request: {}", peer.address(), ack.errorMessage());
                    continue;
                }
                applyIncoming(ack);
                succeeded++;
            } catch (IOException e) {
                log.warn("Sending JOIN to {} failed: {}", peer.address(), e.toString());
            }
        }

        if (succeeded == 0) {
            // The door opened but JOIN failed, so merge the probed nodes into the view and
            // let gossip converge the rest
            log.warn("No JOIN succeeded; merging the {} probed node(s) and waiting for gossip "
                    + "to converge", pending.size());
            applyChanges(memberList.merge(pending));
        }

        Node leader = memberList.leader();
        log.info("Joined cluster [{}] with {} member(s); leader is {}",
                config.clusterName(), memberList.size(),
                leader == null ? "none yet" : leader.address());
        // Let the other members learn of this arrival promptly, rather than at the next
        // gossip round
        broadcast();
    }

    // ------------------------------------------------------------------
    // Scheduled tasks
    // ------------------------------------------------------------------

    private void scheduleTasks() {
        long interval = config.gossipIntervalMs();
        scheduler.scheduleWithFixedDelay(this::safeGossipRound, interval, interval, TimeUnit.MILLISECONDS);

        long reapInterval = Math.max(200L, Math.min(config.suspectTimeoutMs() / 2, 1_000L));
        scheduler.scheduleWithFixedDelay(this::safeReap, reapInterval, reapInterval, TimeUnit.MILLISECONDS);

        // Sweep de-duplication records at half the window, so expired ones cannot pile up
        long dedupTick = Math.max(1_000L, config.payloadDedupTtlMs() / 2);
        scheduler.scheduleWithFixedDelay(this::safeEvictDedup, dedupTick, dedupTick, TimeUnit.MILLISECONDS);

        // The work-port check. Following the failed-node reaping frequency is enough: what
        // it repairs is a silent failure, noticing within seconds is ample, and the check
        // itself merely asks whether a socket is open, which costs almost nothing
        scheduler.scheduleWithFixedDelay(this::safeCheckWorkPort,
                reapInterval, reapInterval, TimeUnit.MILLISECONDS);

        // The split-brain check runs periodically too. Checking only on receiving gossip
        // would freeze "how long has it been split" at the moment of the last message --
        // and what most needs watching about a split is precisely that it is not healing,
        // in which case messages may well not be arriving at all
        scheduler.scheduleWithFixedDelay(this::safeCheckSplitBrain,
                reapInterval, reapInterval, TimeUnit.MILLISECONDS);

        long rediscoverTick = minPositive(config.aloneRediscoverIntervalMs(), config.rediscoverIntervalMs());
        if (rediscoverTick > 0) {
            scheduler.scheduleWithFixedDelay(this::safeRediscover,
                    rediscoverTick, rediscoverTick, TimeUnit.MILLISECONDS);
        }

        // One more check when the quiet period ends, to release the leader announcement
        // that startup held back
        if (config.leaderQuietPeriodMs() > 0) {
            scheduler.schedule(this::checkLeaderChange,
                    config.leaderQuietPeriodMs() + 50L, TimeUnit.MILLISECONDS);
        }
    }

    private static long minPositive(long a, long b) {
        if (a <= 0) {
            return Math.max(b, 0L);
        }
        if (b <= 0) {
            return a;
        }
        return Math.min(a, b);
    }

    private void safeGossipRound() {
        try {
            if (stopped.get()) {
                return;
            }
            for (Node peer : memberList.randomMembers(config.gossipFanout(), null)) {
                probeExecutor.execute(() -> probeMember(peer));
            }
        } catch (Throwable t) {
            log.error("The periodic gossip task threw", t);
        }
    }

    /**
     * Scans for SUSPECT nodes that have timed out and sweeps expired tombstones.
     */
    private void safeEvictDedup() {
        try {
            if (!stopped.get()) {
                deduplicator.evictExpired();
            }
        } catch (Throwable t) {
            log.error("Sweeping business message de-duplication records failed", t);
        }
    }

    private void safeReap() {
        try {
            if (stopped.get()) {
                return;
            }
            List<MembershipChange> changes = memberList.reapExpired(config.suspectTimeoutMs());
            if (!changes.isEmpty()) {
                publishChanges(changes);
                checkLeaderChange();
                // A departure is important information; spread it at once rather than
                // waiting for the next gossip round
                broadcast();
            }
        } catch (Throwable t) {
            log.error("The failed-node reaping task threw", t);
        }
    }

    /**
     * Checks the work port and rebinds it in place if it has failed.
     *
     * <h2>What it guards against is "alive but deaf"</h2>
     * Once the work port's listening socket fails without our knowing, the node receives
     * nothing ever again -- and <b>not one</b> of the existing self-healing paths can be
     * relied on:
     *
     * <ul>
     *   <li>the member list is updated by receiving messages, so receiving nothing freezes
     *       it on an old snapshot. {@code memberList.size() > 1} then holds forever and the
     *       "rediscover only when alone" branch in {@link #safeRediscover()} never fires</li>
     *   <li>the low-frequency re-probe branch does run, but it uses <b>the same broken
     *       transport</b></li>
     *   <li>the node still sends probes and the others still reply; only the replies never
     *       arrive -- so it declares <b>the entire cluster</b> failed, one node at a time,
     *       while believing everything is fine</li>
     * </ul>
     *
     * <p>A pure silent failure: the log looks perfectly normal and nothing reports an error.
     * Without this check, no mechanism would ever find it.
     */
    private void safeCheckSplitBrain() {
        try {
            checkSplitBrain();
        } catch (Throwable t) {
            log.error("The split-brain check task threw", t);
        }
    }

    private void safeCheckWorkPort() {
        try {
            if (stopped.get()) {
                return;
            }
            transport.checkAndRepairWorkPort();
        } catch (Throwable t) {
            log.error("The work-port check task threw", t);
        }
    }

    /**
     * Periodic rediscovery, covering two kinds of split view:
     * <ul>
     *   <li><b>Alone</b>, with only this node in the cluster: run {@link #probe()} again,
     *       which covers the race window where several nodes started at once and missed one
     *       another.</li>
     *   <li><b>Already in a cluster</b>: re-probe the cluster port at low frequency to find
     *       every holder. This path cannot be dropped -- when several nodes have formed
     *       separate sub-clusters, both sides have a size above 1, and "rediscover only when
     *       alone" would never merge them.</li>
     * </ul>
     */
    private void safeRediscover() {
        try {
            if (stopped.get()) {
                return;
            }
            if (memberList.size() <= 1) {
                if (config.aloneRediscoverIntervalMs() <= 0) {
                    return;
                }
                if (transport.holds(config.clusterPort())) {
                    // ALONE, AND HOLDING THE CLUSTER PORT.
                    //
                    // On one machine that means "I am the only leader", because the operating
                    // system guarantees a port cannot have two holders. BUT ACROSS MACHINES
                    // THE EQUIVALENCE DOES NOT HOLD: every host can bind its own 22000 with
                    // no conflict whatsoever at the OS level. Here, holds() says only "nobody
                    // on this machine is competing with me", and there may well be a node on
                    // each of two other machines believing exactly the same thing.
                    //
                    // probe() cannot be used: the first thing it does is the holds() check,
                    // so it returns immediately -- and three isolated leaders, each going
                    // their own way, WOULD NEVER MERGE. (Measured: three containers started
                    // together were still three memberCount=1 leaders ten minutes later.)
                    //
                    // So knock around actively, find every holder, and stand aside by
                    // takeover order -- the same actions as the "already in a cluster" path
                    // below
                    List<Node> holders = discovery.lookupAll();
                    if (!holders.isEmpty() && !stopped.get()) {
                        join(holders);
                        checkLeaderChange();
                        yieldClusterPortIfNeeded();
                    }
                    return;
                }
                log.debug("Currently alone; starting discovery again");
                probe();
                return;
            }

            long interval = config.rediscoverIntervalMs();
            if (interval <= 0 || System.currentTimeMillis() - lastRediscoverMs < interval) {
                return;
            }
            lastRediscoverMs = System.currentTimeMillis();

            // Find every node holding the cluster port: more than one means the view has split
            List<Node> holders = discovery.lookupAll();
            if (!holders.isEmpty() && !stopped.get()) {
                join(holders);
                checkLeaderChange();
                yieldClusterPortIfNeeded();
            }
        } catch (Throwable t) {
            log.error("The rediscovery task threw", t);
        }
    }

    // ------------------------------------------------------------------
    // SWIM failure detection
    // ------------------------------------------------------------------

    private void probeMember(Node peer) {
        GossipMessage ping = GossipMessage.builder(MessageType.PING, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .seq(transport.nextSeq())
                .build();
        try {
            Message ack = transport.request(peer.socketAddress(), ping, config.probeTimeoutMs());
            if (ack.ok()) {
                applyIncoming(ack);
                applyChange(memberList.markAlive(peer.id()));
                return;
            }
            log.debug("Node {} returned a failure response: {}", peer.address(), ack.errorMessage());
        } catch (IOException e) {
            log.debug("Direct probe of {} failed: {}", peer.address(), e.toString());
        }
        if (!stopped.get()) {
            indirectProbe(peer);
        }
    }

    /**
     * The indirect probe: ask other nodes to PING the target on this node's behalf.
     * It filters out misjudgements caused by jitter on the link between this node and the
     * target -- if any relay can reach the target, the target is alive.
     */
    private void indirectProbe(Node target) {
        List<Node> helpers = memberList.randomMembers(config.indirectProbeNodes(), target.id());
        if (helpers.isEmpty()) {
            // No relay is available, so the local judgement has to stand
            suspect(target);
            return;
        }

        GossipMessage req = GossipMessage.builder(MessageType.PING_REQ, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .target(target.id(), target.host(), target.port())
                .seq(transport.nextSeq())
                .build();

        int timeout = config.probeTimeoutMs() * 2;
        for (Node helper : helpers) {
            if (stopped.get()) {
                return;
            }
            try {
                Message ack = transport.request(helper.socketAddress(), req, timeout);
                applyIncoming(ack);
                if (ack.ok()) {
                    log.debug("An indirect probe via {} found {} alive", helper.address(), target.address());
                    applyChange(memberList.markAlive(target.id()));
                    return;
                }
            } catch (IOException e) {
                log.debug("Delegating an indirect probe to {} failed: {}", helper.address(), e.toString());
            }
        }
        suspect(target);
    }

    private void suspect(Node target) {
        MembershipChange change = memberList.markSuspect(target.id());
        if (change != null) {
            publishChange(change);
            checkLeaderChange();
            broadcast();
        }
    }

    // ------------------------------------------------------------------
    // Inbound message handling
    // ------------------------------------------------------------------

    private Message onMessage(Message msg, InetSocketAddress remote) {
        // One-way messages are never answered: the sender does not read a response, and
        // bytes sent back would sit on the connection and impersonate the next request's
        // response once it was reused. Even errors can only be logged.
        boolean oneWay = msg.type() == MessageType.PAYLOAD_ONEWAY;

        // Cluster name check: nodes of different clusters do not accept one another
        if (!config.clusterName().equals(msg.clusterName())) {
            log.debug("Refusing a message from cluster [{}]; this node belongs to [{}]",
                    msg.clusterName(), config.clusterName());
            return oneWay ? null : GossipMessage.builder(respondType(msg.type()), config.clusterName())
                    .sender(memberList == null ? null : memberList.self())
                    .seq(msg.seq())
                    .error("cluster name mismatch: expected " + config.clusterName()
                            + ", got " + msg.clusterName())
                    .build();
        }
        if (memberList == null || stopped.get()) {
            return oneWay ? null : GossipMessage.builder(respondType(msg.type()), config.clusterName())
                    .seq(msg.seq())
                    .error("this node is not ready yet")
                    .build();
        }

        return switch (msg.type()) {
            case PROBE -> handleProbe(msg);
            case JOIN -> handleJoin(msg);
            case PING, SYNC -> handlePing(msg);
            case PING_REQ -> handlePingReq(msg);
            case LEAVE -> handleLeave(msg);
            case PAYLOAD, PAYLOAD_ONEWAY -> handlePayload(msg);
            // ACK-family messages only appear in the requester's synchronous read and never reach here
            case ACK, JOIN_ACK, PROBE_ACK -> null;
        };
    }

    private static MessageType respondType(MessageType requestType) {
        return switch (requestType) {
            case JOIN -> MessageType.JOIN_ACK;
            case PROBE -> MessageType.PROBE_ACK;
            default -> MessageType.ACK;
        };
    }

    /**
     * The discovery handshake: it reports this node's own information only and changes no
     * membership, so that scanning cannot pollute cluster state.
     */
    private Message handleProbe(Message msg) {
        return GossipMessage.builder(MessageType.PROBE_ACK, config.clusterName())
                .sender(memberList.self())
                .seq(msg.seq())
                .ok(true)
                .build();
    }

    /**
     * Handles a node joining: merge its view, send back the full member list, and spread
     * the news to the other members at once.
     */
    private Message handleJoin(Message msg) {
        applyIncoming(msg);
        GossipMessage ack = GossipMessage.builder(MessageType.JOIN_ACK, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .seq(msg.seq())
                .ok(true)
                .build();
        if (msg.sender() != null) {
            log.info("Node {} is requesting to join", msg.sender().address());
            probeExecutor.execute(this::broadcast);
        }
        return ack;
    }

    /**
     * Handles liveness probes and view synchronisation: member lists are exchanged both ways.
     */
    private Message handlePing(Message msg) {
        applyIncoming(msg);
        return GossipMessage.builder(MessageType.ACK, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .seq(msg.seq())
                .ok(true)
                .build();
    }

    /**
     * Probes a target on another node's behalf and reports the outcome faithfully.
     */
    private Message handlePingReq(Message msg) {
        applyIncoming(msg);

        boolean alive = false;
        String error = "";
        InetSocketAddress target = new InetSocketAddress(msg.targetHost(), msg.targetPort());
        GossipMessage ping = GossipMessage.builder(MessageType.PING, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .seq(transport.nextSeq())
                .build();
        try {
            Message ack = transport.request(target, ping, config.probeTimeoutMs());
            alive = ack.ok();
            applyIncoming(ack);
        } catch (IOException e) {
            error = e.toString();
        }

        GossipMessage.Builder ack = GossipMessage.builder(MessageType.ACK, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .seq(msg.seq())
                .ok(alive);
        if (!alive) {
            ack.error("indirect probe failed: " + error);
        }
        return ack.build();
    }

    /**
     * Handles a graceful departure notice.
     */
    private Message handleLeave(Message msg) {
        Node leaver = msg.sender();
        if (leaver != null) {
            log.info("Received a departure notice from {}", leaver.address());
            // The sender has already set itself to LEFT, so merging it is enough
            List<Node> incoming = new ArrayList<>(msg.members().size() + 1);
            incoming.add(leaver.withState(NodeState.LEFT, leaver.incarnation()));
            incoming.addAll(msg.members());
            applyChanges(memberList.merge(incoming));
            checkLeaderChange();
            // Spread it onward, so the whole cluster converges quickly
            probeExecutor.execute(this::broadcast);
        }
        return GossipMessage.builder(MessageType.ACK, config.clusterName())
                .sender(memberList.self())
                .seq(msg.seq())
                .ok(true)
                .build();
    }

    /**
     * A business message arrived. It becomes an event for the application and takes no part
     * in the protocol itself.
     *
     * <p>A sender retransmits when its ACK is lost, and a connection-pool retry on a fresh
     * connection can also deliver the same message twice, so this de-duplicates on
     * {@code sender + seq}. A duplicate still gets its ACK -- which is what stops the
     * sender retransmitting -- but is not delivered to the application again.
     *
     * <p>A one-way message ({@link MessageType#PAYLOAD_ONEWAY}) <b>must not be answered</b>:
     * the sender never reads a response, and bytes sent back would sit on the connection and
     * impersonate the next request's response once it was reused.
     */
    private Message handlePayload(Message msg) {
        boolean oneWay = msg.type() == MessageType.PAYLOAD_ONEWAY;
        Node sender = msg.sender();
        String channel = msg.channel();

        if (sender != null) {
            if (deduplicator.isDuplicate(sender.id(), msg.seq())) {
                metrics.onDuplicate(channel);
                log.debug("Discarding a duplicate business message: sender={}, seq={}", sender.label(), msg.seq());
            } else {
                // What is measured here is DELIVERY time, not business processing time:
                // events are dispatched asynchronously and this is done the moment publish
                // returns. Measuring how long the application itself takes is the listener's
                // business; measuring it at this layer would only ever yield the enqueue time
                long start = metrics.onReceiveStart(channel);
                boolean ok = false;
                try {
                    eventBus.publish(ClusterEvent.payload(sender, channel, msg.content(),
                            memberList.members(), memberList.leader(), memberList.isLeader()));
                    ok = true;
                } finally {
                    metrics.onReceiveEnd(channel, start, ok);
                }
            }
        }

        if (oneWay) {
            return null;
        }
        return GossipMessage.builder(MessageType.ACK, config.clusterName())
                .sender(memberList.self())
                .seq(msg.seq())
                .ok(true)
                .build();
    }

    // ------------------------------------------------------------------
    // Merging and propagating the view
    // ------------------------------------------------------------------

    /**
     * Merges the membership view carried by a message into the local one.
     */
    private void applyIncoming(Message msg) {
        List<Node> incoming = new ArrayList<>(msg.members().size() + 1);
        if (msg.sender() != null) {
            incoming.add(msg.sender());
        }
        incoming.addAll(msg.members());
        applyChanges(memberList.merge(incoming));
        checkLeaderChange();
        // The merged view may conceal another leader. Run the self-check to leave a trace
        // first, then decide whether to yield: only the node that ought to yield does so,
        // whereas every node runs the check -- which is what makes the split visible from
        // whichever node monitoring is looking at
        checkSplitBrain();
        yieldClusterPortIfNeeded();
    }

    private void applyChanges(MemberList.MergeResult result) {
        if (result.isEmpty()) {
            return;
        }
        publishChanges(result.changes());
        if (result.refuted()) {
            // Broadcast the refutation at once, to correct the misjudgement on other nodes
            // as quickly as possible
            probeExecutor.execute(this::broadcast);
        }
    }

    private void applyChange(MembershipChange change) {
        if (change != null) {
            publishChange(change);
            checkLeaderChange();
        }
    }

    /**
     * Pushes the current view to a few random members, to speed up convergence after an
     * important change.
     */
    private void broadcast() {
        if (stopped.get()) {
            return;
        }
        List<Node> targets = memberList.randomMembers(config.gossipFanout() * 2, null);
        if (targets.isEmpty()) {
            return;
        }
        GossipMessage sync = GossipMessage.builder(MessageType.SYNC, config.clusterName())
                .sender(memberList.self())
                .members(memberList.allEntries())
                .seq(transport.nextSeq())
                .build();
        for (Node peer : targets) {
            try {
                Message ack = transport.request(peer.socketAddress(), sync, config.probeTimeoutMs());
                if (ack.ok()) {
                    applyIncoming(ack);
                }
            } catch (IOException e) {
                log.debug("Pushing the view to {} failed: {}", peer.address(), e.toString());
            }
        }
    }

    /**
     * On shutdown, sends LEAVE to every known member concurrently, on a best-effort basis,
     * so the cluster learns of it immediately.
     */
    private void broadcastLeave() {
        Node self = memberList.markSelfLeft();
        List<Node> peers = new ArrayList<>(memberList.members());
        peers.removeIf(n -> n.id().equals(self.id()));
        if (peers.isEmpty()) {
            return;
        }

        GossipMessage leave = GossipMessage.builder(MessageType.LEAVE, config.clusterName())
                .sender(self)
                .members(memberList.allEntries())
                .seq(transport.nextSeq())
                .build();

        // The shutdown path must not linger: notify concurrently with a short timeout
        int timeout = Math.min(config.probeTimeoutMs(), 500);
        List<Thread> senders = new ArrayList<>(peers.size());
        for (Node peer : peers) {
            Thread t = new Thread(() -> {
                try {
                    transport.send(peer.socketAddress(), leave, timeout);
                } catch (IOException e) {
                    log.debug("Sending a departure notice to {} failed: {}", peer.address(), e.toString());
                }
            }, "gossip-leave");
            t.setDaemon(true);
            senders.add(t);
            t.start();
        }
        long deadline = System.currentTimeMillis() + timeout + config.connectTimeoutMs();
        for (Thread t : senders) {
            long remain = deadline - System.currentTimeMillis();
            if (remain <= 0) {
                break;
            }
            try {
                t.join(remain);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("Broadcast a departure notice to {} member(s)", peers.size());
    }

    // ------------------------------------------------------------------
    // Public queries and operations
    // ------------------------------------------------------------------

    @Override
    public Node self() {
        checkStarted();
        return memberList.self();
    }

    @Override
    public List<Node> members() {
        checkStarted();
        return memberList.members();
    }

    @Override
    public List<Node> allEntries() {
        checkStarted();
        return memberList.allEntries();
    }

    @Override
    public Node leader() {
        checkStarted();
        return memberList.leader();
    }

    @Override
    public boolean isLeader() {
        checkStarted();
        // Go by actual possession of the port: the local view may not have refreshed yet
        return transport.holds(config.clusterPort());
    }

    @Override
    public String clusterName() {
        return config.clusterName();
    }

    @Override
    public GossipConfig config() {
        return config;
    }

    @Override
    public void updateMetadata(Map<String, String> metadata) {
        checkStarted();
        Node updated = memberList.updateSelfMetadata(metadata);
        log.info("This node's metadata has been updated: {}", updated.metadata());
        broadcast();
    }

    @Override
    public Map<String, ChannelMetrics> metrics() {
        return metrics.snapshot();
    }

    @Override
    public ChannelMetrics metrics(String channel) {
        return metrics.snapshot(channel);
    }

    @Override
    public SplitBrainStatus splitBrainStatus() {
        if (memberList == null) {
            return SplitBrainStatus.healthy(null, 0L, 0L);
        }
        List<Node> holders = memberList.clusterPortHolders();
        Node leader = memberList.leader();
        long count = splitBrainCount.get();
        if (holders.size() <= 1) {
            return SplitBrainStatus.healthy(
                    leader == null ? null : leader.address(), count, lastSplitBrainAt);
        }
        return new SplitBrainStatus(false,
                holders.stream().map(Node::address).toList(),
                leader == null ? null : leader.address(),
                count, lastSplitBrainAt,
                splitBrainSince == 0L ? System.currentTimeMillis() : splitBrainSince);
    }

    @Override
    public List<BufferMetrics> bufferMetrics() {
        return eventBus.bufferMetrics();
    }

    @Override
    public void resetMetrics() {
        metrics.reset();
    }

    @Override
    public boolean isOnBreak() {
        checkStarted();
        return memberList.self().onBreak();
    }

    /**
     * Toggles the resting state and broadcasts it.
     *
     * <p>Nothing happens when the state does not change: repeated calls should produce no
     * surplus broadcasts, and certainly should not send everyone a stream of "it is resting
     * again" events.
     *
     * <p>The broadcast takes exactly the same path as a metadata update: change this node,
     * then {@link #broadcast()} pushes the membership view. The resting flag travels with
     * the {@code Node}, so no new message type is needed and protocol compatibility is
     * untouched -- all four transport implementations still interoperate.
     *
     * <p>An {@code onNodeBreak} event is emitted for this node too: the application's
     * handling of "who is resting" should be one and the same for itself and for others,
     * and without it the caller would have to write a separate branch for its own case.
     */
    @Override
    public void takeBreak(boolean resting) {
        checkStarted();
        Node before = memberList.self();
        Node updated = memberList.updateSelfOnBreak(resting);
        if (updated == null) {
            return;
        }
        publish(resting ? ClusterEventType.NODE_BREAK : ClusterEventType.NODE_RESUME,
                updated, before, false);
        broadcast();
    }

    @Override
    public int multicast(byte[] content) {
        return multicastTo(null, content, false);
    }

    @Override
    public int multicast(byte[] content, boolean includeSelf) {
        return multicastTo(null, content, includeSelf);
    }

    /**
     * Multicast: sent to every target <b>in parallel</b>.
     *
     * <p>Sent serially the total would be member count times one round trip -- tens of
     * milliseconds at ten members -- and one member with a stalled network would leave every
     * member behind it queued up waiting for its timeout. In parallel, the total depends on
     * the slowest one.
     */
    @Override
    public int multicastTo(String name, byte[] content) {
        return multicastOn(EventBus.DEFAULT_CHANNEL, name, content, false);
    }

    @Override
    public int multicastTo(String name, byte[] content, boolean includeSelf) {
        return multicastOn(EventBus.DEFAULT_CHANNEL, name, content, includeSelf);
    }

    @Override
    public int multicastOn(String channel, String name, byte[] content) {
        return multicastOn(channel, name, content, false);
    }

    @Override
    public int multicastOn(String channel, String name, byte[] content, boolean includeSelf) {
        checkStarted();
        if (restingSoSkipSend(channel, "multicast")) {
            return 0;
        }
        // This node's own copy is dispatched locally: no network, and not part of the
        // parallel send
        int selfDelivered = includeSelf && memberList.self().matchesName(name)
                ? (deliverLocally(channel, content) ? 1 : 0)
                : 0;

        List<Node> targets = otherMembers(channel, name);
        if (targets.isEmpty()) {
            return selfDelivered;
        }
        return selfDelivered + multicastToPeers(channel, targets, content);
    }

    /**
     * Hands a message straight to this node's listeners.
     *
     * <p>No network, and <b>no de-duplication</b>: de-duplication exists to catch one
     * message arriving twice through retransmission, and a local dispatch happens exactly
     * once. Routing it through the loopback would only consume a seq for it, and could
     * collide with a genuine retransmission.
     *
     * @return true when it was dispatched
     */
    private boolean deliverLocally(String channel, byte[] content) {
        Node self = memberList.self();
        eventBus.publish(ClusterEvent.payload(self, channel, content,
                memberList.members(), memberList.leader(), memberList.isLeader()));
        return true;
    }

    private int multicastToPeers(String channel, List<Node> targets, byte[] content) {
        if (targets.isEmpty()) {
            return 0;
        }
        if (targets.size() == 1) {
            return unicastOn(channel, targets.get(0), content) ? 1 : 0;
        }

        List<CompletableFuture<Boolean>> futures = new ArrayList<>(targets.size());
        for (Node peer : targets) {
            futures.add(CompletableFuture.supplyAsync(
                    () -> unicastOn(channel, peer, content), payloadExecutor));
        }
        int delivered = 0;
        for (CompletableFuture<Boolean> f : futures) {
            try {
                // unicast already bounds its own timeout and retransmissions, so this cannot
                // wait indefinitely
                if (Boolean.TRUE.equals(f.get())) {
                    delivered++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                log.warn("A multicast threw: {}", e.getCause() == null ? e : e.getCause().toString());
            }
        }
        return delivered;
    }

    /**
     * Unicast to one member.
     *
     * <p>Whether it waits for an acknowledgement follows {@link GossipConfig#payloadAck()}:
     * <ul>
     *   <li>Acknowledged -- wait for the ACK and retransmit when it does not come, at most
     *       {@link GossipConfig#payloadRetries()} times, then discard and return false.
     *       <b>Every retransmission reuses the same seq</b>, which is what lets the receiver
     *       recognise it as one message and not deliver it to the application twice.</li>
     *   <li>Unacknowledged -- returns true as soon as it is sent. That true means only that
     *       it was written to the network, not that the peer received it.</li>
     * </ul>
     */
    @Override
    public boolean unicast(Node target, byte[] content) {
        return unicastOn(EventBus.DEFAULT_CHANNEL, target, content);
    }

    @Override
    public boolean sendToLeader(byte[] content) {
        return sendToLeaderOn(EventBus.DEFAULT_CHANNEL, content);
    }

    @Override
    public boolean sendToLeaderOn(String channel, byte[] content) {
        checkStarted();
        if (restingSoSkipSend(channel, "a message to the leader")) {
            return false;
        }

        // isLeader() rather than leader().equals(self) -- the two answer from different
        // evidence. The former reads actual possession of the cluster port, which is a fact;
        // the latter reads the local membership view, which travels by gossip and lags.
        // A node that has just claimed the port already returns true from isLeader() while
        // leader() in its view may still point at the outgoing one, so deciding by the view
        // would send the message to a node that has already stepped down
        if (isLeader()) {
            return deliverLocally(channel, content);
        }

        Node leader = memberList.leader();
        if (leader == null) {
            // The window after the old leader departed and before a new one claimed the
            // port. Measured at roughly 6 seconds in containers
            log.debug("There is no leader right now; the message to the leader was not sent");
            return false;
        }
        if (leader.id().equals(memberList.self().id())) {
            // The view says this node leads, but the port is not in its hands: a handover is
            // under way. Dispatching locally would be wrong (this node does not lead) and so
            // would claiming delivery, so it honestly returns false
            log.debug("The membership view disagrees with port possession (handover in "
                    + "progress); the message to the leader was not sent");
            return false;
        }
        return unicastOn(channel, leader, content);
    }

    @Override
    public boolean unicastOn(String channel, Node target, byte[] content) {
        checkStarted();
        if (target == null || target.id().equals(memberList.self().id())) {
            return false;
        }
        if (restingSoSkipSend(channel, "unicast")) {
            return false;
        }

        boolean ack = config.payloadAck();
        // The seq is generated outside the loop: retransmissions must reuse it, or the
        // receiver's de-duplication cannot recognise them
        GossipMessage msg = GossipMessage.builder(
                        ack ? MessageType.PAYLOAD : MessageType.PAYLOAD_ONEWAY, config.clusterName())
                .sender(memberList.self())
                .channel(channel)
                .content(content)
                .seq(transport.nextSeq())
                .build();

        long started = metrics.onSendStart(channel);
        if (!ack) {
            // Unacknowledged: what is measured is the time to write to the network, with no
            // round trip. It is still worth having -- failing to write at all (a full buffer,
            // a broken connection) is a problem just the same
            try {
                transport.send(target.socketAddress(), msg, config.probeTimeoutMs());
                metrics.onSendEnd(channel, started, true, 0);
                return true;
            } catch (IOException e) {
                metrics.onSendEnd(channel, started, false, 0);
                log.warn("Sending a business message to {} failed: {}", target.label(), e.toString());
                return false;
            }
        }

        int attempts = config.payloadRetries() + 1;
        for (int i = 0; i < attempts; i++) {
            if (i > 0 && !sleepBeforeRetry()) {
                metrics.onSendEnd(channel, started, false, i);
                return false;
            }
            try {
                Message response =
                        transport.request(target.socketAddress(), msg, config.probeTimeoutMs());
                if (response.ok()) {
                    if (i > 0) {
                        log.debug("Retransmission {} of a business message to {} succeeded", i, target.label());
                    }
                    // The latency runs from the first send to eventual success, retries and
                    // backoff included -- that is what the caller actually waited. Timing only
                    // the last attempt would make the latency look absurdly good
                    metrics.onSendEnd(channel, started, true, i);
                    return true;
                }
                // The peer refused explicitly, for instance because it is not ready yet, so
                // retransmitting is worthwhile
                log.debug("{} refused a business message: {}", target.label(), response.errorMessage());
            } catch (IOException e) {
                log.debug("Sending a business message to {} failed (attempt {}/{}): {}",
                        target.label(), i + 1, attempts, e.toString());
            }
        }
        metrics.onSendEnd(channel, started, false, config.payloadRetries());
        log.warn("Sending a business message to {} failed after {} retransmission(s); discarding it",
                target.label(), config.payloadRetries());
        return false;
    }

    /**
     * @return false when the thread was interrupted and retransmission should be abandoned at once
     */
    private boolean sleepBeforeRetry() {
        long delay = config.payloadRetryDelayMs();
        if (delay <= 0) {
            return true;
        }
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public Node unicast(byte[] content) {
        // The cast is required: without it, null resolves to the more specific
        // unicast(Node, byte[])
        return unicast((Object) null, content);
    }

    @Override
    public Node unicast(byte[] content, boolean includeSelf) {
        return unicast((Object) null, content, includeSelf);
    }

    @Override
    public Node unicast(Object key, byte[] content) {
        return unicastTo(null, key, content, false);
    }

    @Override
    public Node unicast(Object key, byte[] content, boolean includeSelf) {
        return unicastTo(null, key, content, includeSelf);
    }

    @Override
    public Node unicastTo(String name, byte[] content) {
        return unicastTo(name, null, content, false);
    }

    @Override
    public Node unicastTo(String name, byte[] content, boolean includeSelf) {
        return unicastTo(name, null, content, includeSelf);
    }

    @Override
    public Node unicastTo(String name, Object key, byte[] content) {
        return unicastOn(EventBus.DEFAULT_CHANNEL, name, key, content, false);
    }

    @Override
    public Node unicastTo(String name, Object key, byte[] content, boolean includeSelf) {
        return unicastOn(EventBus.DEFAULT_CHANNEL, name, key, content, includeSelf);
    }

    @Override
    public Node unicastOn(String channel, String name, Object key, byte[] content) {
        return unicastOn(channel, name, key, content, false);
    }

    @Override
    public Node unicastOn(String channel, String name, Object key, byte[] content,
                          boolean includeSelf) {
        // No strategy given means the globally configured one, so this behaves exactly as
        // the method always did
        return unicastOn(channel, name, key, content, includeSelf, null);
    }

    @Override
    public Node unicastOn(String channel, String name, Object key, byte[] content,
                          boolean includeSelf, LoadBalancer balancer) {
        checkStarted();
        if (restingSoSkipSend(channel, "unicast")) {
            return null;
        }
        List<Node> candidates = availableTargets(channel, name, includeSelf);
        if (candidates.isEmpty()) {
            log.debug("No matching member, so the unicast was not sent: name={}", name);
            return null;
        }
        // Falls back to the globally configured one when none is given. Do not write this
        // as "a null strategy means no load balancing" -- that would strip the ability to
        // choose a target from the dozen-odd overloads above that pass balancer as null
        LoadBalancer lb = balancer != null ? balancer : config.loadBalancer();
        Node target = lb.choose(candidates, key);
        if (target == null) {
            return null;
        }
        // The choice happens to be this node: dispatch locally, without the network
        if (target.id().equals(memberList.self().id())) {
            return deliverLocally(channel, content) ? target : null;
        }
        return unicastOn(channel, target, content) ? target : null;
    }

    @Override
    public List<Node> membersOf(String name) {
        checkStarted();
        List<Node> result = new ArrayList<>();
        for (Node n : memberList.members()) {
            if (n.matchesName(name)) {
                result.add(n);
            }
        }
        return result;
    }

    /**
     * Members that can be sent to: matching the application name, and excluding this node.
     *
     * <p>Excluding this node is deliberate -- sending a business message to oneself means
     * nothing when the call can simply be made locally, and going round the network would
     * additionally have {@code onPayload} receive one's own message.
     *
     * @param name the application name; null or an empty string means no restriction
     */
    private List<Node> otherMembers(String channel, String name) {
        return availableTargets(channel, name, false);
    }

    /**
     * While this node is resting, no business message goes out.
     *
     * <p>Resting works <b>both ways</b>: others skip it when choosing targets (see
     * {@link #availableTargets}), and it sends nothing outward itself. Blocking one
     * direction only would leave the half-measure of "a resting node still handing out work"
     * -- and {@code takeBreak} means the whole business channel falls silent.
     *
     * <p>Note this stops business messages only. Gossip, probing and membership sync carry
     * on as usual, or the node would be declared failed and would have genuinely left.
     *
     * @return true when it is resting and this send should be skipped
     */
    private boolean restingSoSkipSend(String channel, String what) {
        if (!isBusinessChannel(channel) || !memberList.self().onBreak()) {
            return false;
        }
        log.debug("This node is resting; {} was not sent", what);
        return true;
    }

    /**
     * Whether this channel carries <b>the user's business messages</b>.
     *
     * <p>{@code takeBreak} governs business messages only. The framework's own coordination
     * channels -- everything prefixed {@code spreader.}, covering cache replication, locks,
     * RPC and task dispatch -- are <b>entirely unaffected</b>.
     *
     * <h2>Why they must be separated</h2>
     * Treating them alike would cause two serious problems:
     * <ul>
     *   <li><b>Responses could not get back</b> -- lock grants, cache snapshots and RPC
     *       return values all go out over these channels. A resting leader could send no
     *       responses while <b>still being the leader</b> (resting does not change
     *       leadership), so lock acquisition and cache writes across the whole cluster would
     *       time out together</li>
     *   <li><b>Replicas would stop updating</b> -- cache replication is multicast, and
     *       filtering out a resting node freezes its replica, so it comes back holding stale
     *       data</li>
     * </ul>
     *
     * <p>A component above that also wants to withhold work from a resting node -- RPC not
     * routing to it, say -- should decide that for itself from {@link Node#onBreak()}. Those
     * components know which messages are new requests and which are responses that must go
     * back; the transport layer cannot tell them apart.
     */
    private static boolean isBusinessChannel(String channel) {
        return channel == null || channel.isEmpty()
                || !channel.startsWith(SYSTEM_CHANNEL_PREFIX);
    }

    /**
     * Members eligible to receive a business message.
     *
     * <p>Three filters, each for its own reason:
     * <ul>
     *   <li><b>Matching the application name</b> -- targeting such as "only the order
     *       service"</li>
     *   <li><b>Excluding this node</b>, unless includeSelf says otherwise -- sending to
     *       oneself means nothing when the call can be made locally, and going round the
     *       network would additionally have {@code onPayload} receive one's own message</li>
     *   <li><b>Excluding resting nodes</b> -- they remain full members, still gossiping and
     *       still in the election, and merely take no business for now. Skipping them here
     *       is the entirety of what {@link #takeBreak(boolean)} does</li>
     * </ul>
     *
     * @param name        the application name; null or an empty string means no restriction
     * @param includeSelf whether this node counts as a candidate
     */
    private List<Node> availableTargets(String channel, String name, boolean includeSelf) {
        boolean skipResting = isBusinessChannel(channel);
        String selfId = memberList.self().id();
        List<Node> targets = new ArrayList<>();
        for (Node n : memberList.members()) {
            if (skipResting && n.onBreak()) {
                continue;
            }
            if (!includeSelf && n.id().equals(selfId)) {
                continue;
            }
            if (n.matchesName(name)) {
                targets.add(n);
            }
        }
        return targets;
    }

    @Override
    public void addListener(GossipListener listener) {
        eventBus.addListener(listener);
    }

    @Override
    public void addListener(String channel, GossipListener listener) {
        eventBus.addListener(channel, listener);
    }

    @Override
    public void removeListener(GossipListener listener) {
        eventBus.removeListener(listener);
    }

    private void checkStarted() {
        if (memberList == null) {
            throw new IllegalStateException("the cluster node is not started; call start() first");
        }
    }

    // ------------------------------------------------------------------
    // Event publication
    // ------------------------------------------------------------------

    private void publishChanges(Collection<MembershipChange> changes) {
        for (MembershipChange c : changes) {
            publishChange(c);
        }
    }

    private void publishChange(MembershipChange change) {
        Node node = change.node();
        boolean graceful = change.type() == MembershipChange.Type.LEFT;

        ClusterEventType type = switch (change.type()) {
            case JOINED -> ClusterEventType.NODE_JOINED;
            case LEFT -> ClusterEventType.NODE_LEFT;
            case DEAD -> ClusterEventType.NODE_DEAD;
            case SUSPECT -> ClusterEventType.NODE_SUSPECT;
            case RECOVERED -> ClusterEventType.NODE_RECOVERED;
            case UPDATED -> {
                // A change of resting state arrives as UPDATED too, since it is an update to
                // the node's information -- but its meaning is nothing like "the metadata
                // changed": it decides whether the node should still be given work.
                // Applications almost always handle it separately, hence its own event type
                Node before = change.previous();
                if (before != null && before.onBreak() != node.onBreak()) {
                    yield node.onBreak()
                            ? ClusterEventType.NODE_BREAK : ClusterEventType.NODE_RESUME;
                }
                yield ClusterEventType.NODE_UPDATED;
            }
        };
        publish(type, node, change.previous(), graceful);
    }

    /**
     * Detects a change of leader and publishes LEADER_CHANGED when there is one; schedules
     * a takeover when leadership falls vacant.
     */
    private void checkLeaderChange() {
        // The first announcement requires two conditions at once:
        //   1) the first discovery round has finished -- machines starting together have not
        //      yet seen one another, and each would compute itself as the leader;
        //   2) the quiet period has passed -- leaving time for newly discovered nodes to
        //      exchange views.
        if (!firstAnnounced.get()
                && (!discoveryCompleted.get() || System.currentTimeMillis() < leaderAnnounceAfterMs)) {
            return;
        }
        Node current = memberList.leader();
        Node previous = lastKnownLeader.get();

        if (current == null && discoveryCompleted.get()) {
            // Leadership is vacant; go for the cluster port, staggered by takeover order
            scheduleTakeover();
        }
        if (sameNode(previous, current)) {
            return;
        }
        if (!lastKnownLeader.compareAndSet(previous, current)) {
            return;
        }
        boolean selfIsLeader = current != null && current.id().equals(memberList.self().id());
        boolean first = firstAnnounced.getAndSet(true);
        log.info("Leader changed: {} -> {}{}",
                previous == null ? "none" : previous.address(),
                current == null ? "none" : current.address(),
                selfIsLeader ? " (this node is now the leader)" : "");
        eventBus.publish(new ClusterEvent(ClusterEventType.LEADER_CHANGED, current, previous,
                memberList.members(), current, selfIsLeader, false, null, "",
                System.currentTimeMillis()));

        // The LEFT/BACK pair expresses "there is a leader" versus "there is none" and says
        // nothing about who holds the role: the one taking over is usually whichever member
        // ranks first, not necessarily the one that just left coming back
        if (current == null) {
            log.warn("Leader {} has left; the cluster is without one for now", previous.address());
            publish(ClusterEventType.LEADER_LEFT, previous, null, false);
        } else {
            leaderLatch.countDown();
            if (first) {
                // Settling a leader for the first time is not a comeback; the cluster has
                // merely just formed
                publish(ClusterEventType.LEADER_BACK, current, previous, false);
            }
        }
    }

    private static boolean sameNode(Node a, Node b) {
        if (a == b) {
            return true;
        }
        return a != null && b != null && a.id().equals(b.id());
    }

    private void publish(ClusterEventType type, Node node, Node previous, boolean graceful) {
        Node leader = memberList == null ? null : memberList.leader();
        boolean selfIsLeader = memberList != null && memberList.isLeader();
        List<Node> members = memberList == null ? List.of() : memberList.members();
        eventBus.publish(ClusterEvent.of(type, node, previous, members, leader,
                selfIsLeader, graceful));
    }
}
