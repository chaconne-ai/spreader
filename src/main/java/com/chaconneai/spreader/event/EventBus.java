package com.chaconneai.spreader.event;

import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.util.ExecutorUtils;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The event dispatcher. It isolates listener execution from the gossip protocol
 * itself, so however slow a listener is, it cannot drag the protocol down with it.
 *
 * <h2>Two dispatch paths, because the two kinds of event differ on ordering</h2>
 * <ul>
 *   <li><b>Cluster events</b> (joins, departures, leader changes) go through a
 *       <b>single serial thread</b>, so the order listeners observe matches the order in
 *       which cluster state actually changed. This one cannot be concurrent: let
 *       {@code onNodeJoined(A)} and {@code onNodeLeft(A)} arrive out of order and the
 *       application ends up believing A is still online, and out-of-order leader changes
 *       corrupt the "am I the leader" flag outright.</li>
 *   <li><b>Business messages</b> ({@code onPayload}) go through a <b>pool whose
 *       concurrency is configurable</b>. Business messages from different senders have no
 *       causal relationship to begin with, and dispatching them serially would only make
 *       the receiving side the throughput bottleneck.</li>
 * </ul>
 *
 * <p>Concurrency defaults to 1, preserving the fully serial behaviour. Before raising
 * it, confirm the application can accept that <b>business messages carry no ordering
 * guarantee</b> -- including several sent back to back by one sender. Where order
 * matters, put a sequence number in the message and let the application sort it out.
 *
 * <h2>Business messages are isolated by channel</h2>
 * A listener subscribes to one channel and <b>receives business messages from that
 * channel only</b>. Without that isolation, the internal traffic of every toolkit
 * sharing the cluster (distributed locks, for instance) would pour into the
 * application's own {@code onPayload} and force it to recognise other people's message
 * formats.
 *
 * <p>Cluster events have no channel and reach every listener -- joins, departures and
 * leader changes are public information by nature.
 *
 * <h2>The queue is bounded, and throttles when full</h2>
 * This once used {@code Executors.newFixedThreadPool}, which is backed by an
 * <b>unbounded</b> queue. That is a trap: when a listener slows down, the queue grows
 * until the heap is gone, and <b>nothing anywhere reports a thing</b> along the way.
 * Overload was hidden completely.
 *
 * <p>And {@code onPayload} is the common entrance for every component -- cache
 * broadcasts, lock requests and RPC all arrive through it. One slow listener could take
 * the whole process down.
 *
 * <h2>Why a full queue discards rather than blocks</h2>
 * The producer is the <b>network receive thread</b>. Blocking it means gossip
 * heartbeats stop being handled, so this node is declared failed by everyone else --
 * taking the entire node out of the cluster to avoid dropping one business message,
 * which is plainly a bad trade.
 *
 * <p>So the policy is: <b>wait a moment first</b>, absorbing a transient spike, and
 * discard and count only if it is still full. Business messages already have acks and
 * retries above them (see {@code payloadAck}), so losing one is not a disaster -- and
 * the drop count stays visible ({@link #droppedPayloads()}), so overload is no longer
 * silent.
 *
 * <p><b>Cluster events are exempt.</b> They are tiny in volume (joins, departures,
 * leader changes) and losing one corrupts the membership view, so queueing is always
 * preferable to dropping.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class EventBus {

    /** The default channel, used when none is given -- the application's own business messages. */
    public static final String DEFAULT_CHANNEL = "";

    /** A listener together with the business-message channel it subscribed to. */
    private record Subscription(GossipListener listener, String channel) {
    }

    private final List<Subscription> listeners = new CopyOnWriteArrayList<>();

    /** Default capacity of the business message queue. */
    public static final int DEFAULT_PAYLOAD_CAPACITY = 10_000;

    /** How long to wait for a slot once the queue is full. */
    private static final long OFFER_TIMEOUT_MS = 50L;

    /** Cluster events: must be serial. */
    private final ExecutorService dispatcher;

    /** Business messages: may run concurrently. At a concurrency of 1 this is the same
     *  executor as the dispatcher, behaving exactly as the serial version did. */
    private final ExecutorService payloadDispatcher;

    /**
     * Permits for business messages in flight; the count is the queue capacity.
     *
     * <p>A semaphore rather than the bounded queue alone, because the permits cover the
     * <b>total</b> of queued plus executing, whereas queue capacity governs only the
     * queued part. The executing ones hold memory and threads just the same, and leaving
     * them out would make the throttling wrong.
     */
    private final Semaphore payloadPermits;

    private final int payloadCapacity;

    /** Drop counter. Overload should not be silent, and this number has to reach monitoring. */
    private final AtomicLong dropped = new AtomicLong();

    /** When a drop was last logged, for rate limiting -- a line per drop would flood the
     *  log under overload. */
    private volatile long lastDropLogMs;

    private final Logger log;
    private volatile boolean closed;

    public EventBus(Logger log) {
        this(log, 1);
    }

    /**
     * @param payloadThreads dispatch concurrency for business messages; 1 shares the
     *                       single serial thread with cluster events
     */
    public EventBus(Logger log, int payloadThreads) {
        this(log, payloadThreads, DEFAULT_PAYLOAD_CAPACITY);
    }

    /**
     * @param payloadThreads  dispatch concurrency for business messages; 1 shares the
     *                        single serial thread with cluster events
     * @param payloadCapacity ceiling on business messages queued plus executing; see the
     *                        class documentation
     */
    public EventBus(Logger log, int payloadThreads, int payloadCapacity) {
        this.log = log;
        this.payloadCapacity = Math.max(1, payloadCapacity);
        this.payloadPermits = new Semaphore(this.payloadCapacity);
        // Cluster events get far more room than business messages: tiny in volume, but
        // not one may be lost -- drop a single join or departure notice and the
        // membership view is wrong, with nothing to heal it
        this.dispatcher = ExecutorUtils.singleThread("gossip-event", 100_000);
        // Queue capacity matches the permit count: the semaphore is what actually holds
        // the line, and this merely stops the queue becoming a hidden unbounded buffer.
        // backpressured rather than discarding -- reaching this point means the semaphore
        // let the message through, so it has been promised handling and must not be dropped
        this.payloadDispatcher = payloadThreads <= 1
                ? this.dispatcher
                : ExecutorUtils.backpressured("gossip-event-payload",
                        payloadThreads, this.payloadCapacity);
    }

    /** Subscribes to the default channel. */
    public void addListener(GossipListener listener) {
        addListener(DEFAULT_CHANNEL, listener);
    }

    /**
     * Subscribes to business messages on a given channel.
     *
     * <p>Cluster events have nothing to do with channels and still arrive in full; the
     * channel filters {@code onPayload} only.
     */
    public void addListener(String channel, GossipListener listener) {
        if (listener != null) {
            listeners.add(new Subscription(listener, channel == null ? DEFAULT_CHANNEL : channel));
        }
    }

    public void removeListener(GossipListener listener) {
        listeners.removeIf(s -> s.listener().equals(listener));
    }

    public void clearListeners() {
        listeners.clear();
    }

    /** Dispatches an event asynchronously. Business messages and cluster events take
     *  different paths; see the class documentation. */
    public void publish(ClusterEvent event) {
        if (closed || listeners.isEmpty()) {
            return;
        }
        if (event.type() == ClusterEventType.PAYLOAD_RECEIVED) {
            publishPayload(event);
            return;
        }
        try {
            // Cluster events are not throttled: tiny in volume, and losing one corrupts
            // the membership view
            dispatcher.execute(() -> doPublish(event));
        } catch (RuntimeException e) {
            log.debug("Event dispatcher is closed; discarding event: {}", event);
        }
    }

    /**
     * Dispatches one business message, with throttling.
     *
     * <p>When no permit is available it waits briefly first: transient spikes are common
     * -- a batch of cache updates arriving together -- and 50 milliseconds is usually
     * enough for one to pass. Only if it is still full does the message get discarded.
     */
    private void publishPayload(ClusterEvent event) {
        boolean acquired;
        try {
            acquired = payloadPermits.tryAcquire(OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!acquired) {
            recordDrop(event);
            return;
        }
        try {
            payloadDispatcher.execute(() -> {
                try {
                    doPublish(event);
                } finally {
                    // The permit must be released in a finally. Miss once and one permit
                    // is gone for good; enough of those and the process starts dropping
                    // messages for no apparent reason
                    payloadPermits.release();
                }
            });
        } catch (RuntimeException e) {
            payloadPermits.release();
            log.debug("Event dispatcher is closed; discarding event: {}", event);
        }
    }

    /** Records one drop. The logging is rate-limited, or it would become a new bottleneck
     *  of its own under overload. */
    private void recordDrop(ClusterEvent event) {
        long total = dropped.incrementAndGet();
        long now = System.currentTimeMillis();
        if (now - lastDropLogMs >= 1_000L) {
            lastDropLogMs = now;
            log.warn("Business message dispatch queue is full (capacity {}); {} message(s) "
                            + "discarded so far. Listeners cannot keep up -- either move the "
                            + "heavy work out of onPayload onto your own thread pool, or "
                            + "raise payload-queue-capacity",
                    payloadCapacity, total);
        }
    }

    /**
     * The fill level of every inbound buffer: the dispatcher's own queue and every
     * buffered listener.
     *
     * <p>When a buffer fills, messages are <b>discarded outright</b> -- backpressure is
     * not an option, since stalling the receive thread would stop gossip heartbeats and
     * get this node declared failed -- and the discarding is <b>silent</b>: neither the
     * sender nor the receiving application learns of it. Which is why this data has to
     * reach monitoring.
     */
    public List<BufferMetrics> bufferMetrics() {
        List<BufferMetrics> out = new ArrayList<>();
        // The dispatcher's own queue: every event passes through it, so its filling up
        // is the most serious case of all
        out.add(new BufferMetrics("event-dispatch",
                payloadCapacity - payloadPermits.availablePermits(),
                payloadCapacity, dropped.get(), 0L));
        for (Subscription s : listeners) {
            if (s.listener() instanceof BufferedGossipListener b) {
                out.add(b.bufferMetrics());
            }
        }
        return out;
    }

    /**
     * How many business messages have been discarded in total.
     *
     * <p><b>Alert the moment this is not 0.</b> It means listeners cannot keep pace with
     * arrivals, and the discarded messages can only be recovered by retries at a higher
     * layer -- if there are any.
     */
    public long droppedPayloads() {
        return dropped.get();
    }

    /**
     * How many business messages are in flight right now, queued plus executing.
     *
     * <p>Sitting near capacity is the warning sign of overload -- and it arrives far
     * earlier than waiting for drops to start.
     */
    public int inflightPayloads() {
        return payloadCapacity - payloadPermits.availablePermits();
    }

    /** Ceiling on business messages in flight. */
    public int payloadCapacity() {
        return payloadCapacity;
    }

    private void doPublish(ClusterEvent event) {
        boolean isPayload = event.type() == ClusterEventType.PAYLOAD_RECEIVED;
        for (Subscription s : listeners) {
            // Business messages go only to listeners subscribed to that channel;
            // cluster events go to everyone
            if (isPayload && !s.channel().equals(event.channel())) {
                continue;
            }
            try {
                s.listener().onEvent(event);
                dispatchTyped(s.listener(), event);
            } catch (Throwable t) {
                log.error("Event listener threw: " + event, t);
            }
        }
    }

    private void dispatchTyped(GossipListener l, ClusterEvent e) {
        switch (e.type()) {
            case SELF_STARTED -> l.onSelfStarted(e.node());
            case SELF_STOPPED -> l.onSelfStopped(e.node());
            case CLUSTER_JOINED -> l.onClusterJoined(e.node(), e.members().size() <= 1);
            case NODE_JOINED -> l.onNodeJoined(e.node());
            case NODE_LEFT -> l.onNodeLeft(e.node(), true);
            case NODE_DEAD -> l.onNodeLeft(e.node(), false);
            case NODE_SUSPECT -> l.onNodeSuspect(e.node());
            case NODE_RECOVERED -> l.onNodeRecovered(e.node());
            case NODE_UPDATED -> l.onNodeUpdated(e.node(), e.previous());
            case LEADER_CHANGED -> l.onLeaderChanged(e.previous(), e.node(), e.selfIsLeader());
            case LEADER_LEFT -> l.onLeaderLeft(e.node());
            case LEADER_BACK -> l.onLeaderBack(e.node());
            case PAYLOAD_RECEIVED -> l.onPayload(e.node(), e.content());
            case NODE_BREAK -> l.onNodeBreak(e.node());
            case NODE_RESUME -> l.onNodeResume(e.node());
        }
    }

    /** Shuts the dispatcher down, letting events already queued finish. */
    public void close() {
        closed = true;
        if (payloadDispatcher != dispatcher) {
            shutdown(payloadDispatcher);
        }
        shutdown(dispatcher);
    }

    private void shutdown(ExecutorService executor) {
        ExecutorUtils.shutdownGracefully(executor, 2_000L);
    }
}
