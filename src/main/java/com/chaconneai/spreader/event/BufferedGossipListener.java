package com.chaconneai.spreader.event;

import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A listener base class that separates <b>receiving</b> an {@code onPayload} from
 * <b>processing</b> it.
 *
 * <pre>{@code
 * public class OrderListener extends BufferedGossipListener {
 *
 *     public OrderListener() {
 *         super("order", 8192);
 *     }
 *
 *     @Override
 *     protected void handlePayload(Node sender, byte[] content) {
 *         // This may be slow: it runs on its own consumer thread and never holds up
 *         // spreader's dispatch
 *         saveToDatabase(content);
 *     }
 * }
 *
 * cluster.addListener("order", listener);
 * listener.startDispatch();
 * // on shutdown
 * listener.stopDispatch();
 * }</pre>
 *
 * <h2>The problem it solves</h2>
 * {@code onPayload} is invoked on spreader's <b>dispatch thread</b>, and that thread is
 * shared by every component. Querying a database or making an HTTP call inside it makes
 * the entire dispatch chain run at the speed of your one listener -- cache broadcasts,
 * lock requests and other applications' messages all queue up behind it.
 *
 * <p>Extending this class, {@code onPayload} does exactly one thing: <b>push the message
 * into a ring buffer and return</b>. The real work happens on a consumer thread of its
 * own, and the dispatch thread stays fast for good.
 *
 * <h2>This is optional</h2>
 * Implementing {@link GossipListener} directly is perfectly fine -- where the handling
 * is light (flip a flag in memory, signal a condition) it is simpler and saves a thread
 * switch.
 *
 * <p><b>When to switch to this instead:</b> {@code handlePayload} does IO, takes a lock,
 * or does anything else that might exceed a few tens of microseconds.
 *
 * <h2>A full buffer drops, and that is deliberate</h2>
 * A full buffer means consumption has fallen behind production and stayed there.
 * Blocking the producer at that point only pushes the pressure back onto the whole
 * cluster's dispatch chain, while the root cause sits on the consuming side -- blocking
 * cannot save it and will drag more things down.
 *
 * <p>So a full buffer discards and counts ({@link #dropped()}), and calls
 * {@link #onOverflow} once so the user has a chance to record, alert, or degrade some
 * other way. <b>A non-zero drop count needs acting on</b>: add consumer threads, make
 * {@code handlePayload} faster, or enlarge the buffer.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public abstract class BufferedGossipListener implements GossipListener {

    /** The most to drain in one go. The cap exists so the consumer thread can check its
     *  exit flag periodically. */
    private static final int DRAIN_LIMIT = 256;

    private final PayloadQueue<Payload> buffer;
    private final String name;
    private final int consumerCount;
    private final Logger log;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong handled = new AtomicLong();
    private Thread[] consumers;

    /** One business message awaiting processing. */
    private record Payload(Node sender, byte[] content) {
    }

    /**
     * @param name     thread name prefix, so a thread dump says at a glance who this is
     * @param capacity buffer capacity; rounded up to a power of two
     */
    protected BufferedGossipListener(String name, int capacity) {
        this(name, capacity, 1, LoggerFactory.getLogger("spreader.listener." + name));
    }

    /**
     * @param consumerCount number of consumer threads.
     *                      <p><b>Anything above 1 gives up ordering</b> -- two messages
     *                      sent back to back by one sender may be processed by two
     *                      threads at once. Keep it at 1 where order matters.
     */
    protected BufferedGossipListener(String name, int capacity, int consumerCount, Logger log) {
        this.name = name;
        this.buffer = PayloadQueues.create(capacity);
        this.consumerCount = Math.max(1, consumerCount);
        this.log = log;
    }

    /**
     * Processes one business message. <b>Runs on its own consumer thread</b> and may be
     * slow.
     *
     * <p>Throwing affects no other message and leaves an error log line behind -- the
     * consumer thread has to stay alive.
     */
    protected abstract void handlePayload(Node sender, byte[] content);

    /**
     * Called once whenever the buffer is full and a message is discarded. Does nothing by
     * default.
     *
     * <p><b>Do no heavy work here.</b> It runs on the dispatch thread, and under overload
     * it is called constantly. Bump a counter or write a rate-limited log line; that is
     * enough.
     */
    protected void onOverflow(Node sender, byte[] content) {
    }

    /**
     * Starts the consumer threads. It must be called, or messages go in and never come
     * out.
     *
     * <p>The {@code Dispatch} in the name avoids colliding with the user's own
     * {@code start()}: components usually have lifecycle methods already, and since these
     * two are final, a collision would not compile.
     */
    public final void startDispatch() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        consumers = new Thread[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            Thread t = new NamedThreadFactory(name + "-consumer", true)
                    .newThread(this::consumeLoop);
            consumers[i] = t;
            t.start();
        }
        log.info("Listener {} started: buffer capacity={}, consumer threads={}, queue={}",
                name, buffer.capacity(), consumerCount,
                PayloadQueues.jcToolsAvailable() ? "JCTools" : "built-in");
    }

    /** Stops the consumer threads. Whatever is left in the buffer is discarded -- to be
     *  sure everything is processed, wait for {@link #pending()} to reach zero first. */
    public final void stopDispatch() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (consumers != null) {
            for (Thread t : consumers) {
                t.interrupt();
            }
        }
        log.info("Listener {} stopped: {} message(s) handled, {} discarded",
                name, handled.get(), buffer.dropped());
    }

    /**
     * Whether this message should go through the buffer. Everything does, by default.
     *
     * <h2>When to let one through directly</h2>
     * The buffer is not free: it buys "never hold up the dispatch thread" at the cost of
     * <b>one extra thread switch</b>. For messages like these, that cost is pure loss:
     * <ul>
     *   <li><b>Responses to requests</b> -- the handling is {@code future.complete(msg)},
     *       a matter of tens of nanoseconds. Routing it through the buffer adds a thread
     *       wake-up to every single request/response exchange, while the caller sits
     *       blocked waiting for it</li>
     *   <li>Anything whose handling is "flip one flag in memory"</li>
     * </ul>
     *
     * <p>What belongs in the buffer, conversely, is the work that is <b>actually
     * work</b>: invoking a business method, writing to a database, applying a batch of
     * state changes.
     *
     * <p>So the typical shape is a split by message type:
     * <pre>{@code
     * @Override
     * protected boolean shouldBuffer(Node sender, byte[] content) {
     *     MyMessage msg = MyMessage.peek(content);
     *     return msg != null && msg.type() != MyMessageType.RESPONSE;
     * }
     * }</pre>
     *
     * <p>A message that skips the buffer calls {@link #handlePayload} <b>on the dispatch
     * thread</b> -- so it had better genuinely be fast.
     */
    protected boolean shouldBuffer(Node sender, byte[] content) {
        return true;
    }

    /**
     * All spreader's dispatch thread does here: enqueue and return.
     *
     * <p>The {@code final} is deliberate. A subclass overriding it would undo the entire
     * decoupling, and nothing in the code would show that had happened. To split traffic,
     * override {@link #shouldBuffer} instead.
     */
    @Override
    public final void onPayload(Node sender, byte[] content) {
        if (!running.get()) {
            // Not started yet, or already stopped: drop it, rather than accumulate
            // messages that will never be processed
            return;
        }
        if (!shouldBuffer(sender, content)) {
            // Straight through: handled here on the dispatch thread. Only for messages
            // too fast to be worth a thread switch
            dispatchOne(new Payload(sender, content));
            return;
        }
        if (!buffer.offer(new Payload(sender, content))) {
            onOverflow(sender, content);
        }
    }

    private void consumeLoop() {
        while (running.get()) {
            int count = buffer.drain(this::dispatchOne, DRAIN_LIMIT);
            if (count == 0) {
                // Nothing to do, so pause. Spinning would burn a core for nothing at low volume
                idle();
            }
        }
    }

    /**
     * Pauses when there is nothing to do.
     *
     * <p>{@code parkNanos} rather than spinning: spinning burns a core for nothing at low
     * volume, and this buffer's typical load is "empty most of the time, with occasional
     * bursts".
     */
    private static void idle() {
        LockSupport.parkNanos(100_000L);
    }

    private void dispatchOne(Payload payload) {
        try {
            handlePayload(payload.sender(), payload.content());
            handled.incrementAndGet();
        } catch (Throwable t) {
            // One failed message must not kill the consumer thread -- that would leave
            // every message behind it unprocessed
            log.error("Listener " + name + " failed to handle a message", t);
        }
    }

    // ------------------------------------------------------------------
    // Observability
    // ------------------------------------------------------------------

    /** How many are still unprocessed. Sitting near capacity means consumption is behind. */
    public final int pending() {
        return buffer.size();
    }

    /** How many were discarded because the buffer was full. <b>Anything but 0 needs acting on.</b> */
    public final long dropped() {
        return buffer.dropped();
    }

    /** How many have been processed in total. */
    public final long handled() {
        return handled.get();
    }

    public final int capacity() {
        return buffer.capacity();
    }

    /** Name of this buffer, usually the channel it serves. */
    public final String bufferName() {
        return name;
    }

    /**
     * Takes all the fill-level data in one go.
     *
     * <p>Reading the four getters separately yields the same fields, but from four
     * different instants -- and a dashboard would then show impossible combinations such
     * as {@code pending} exceeding {@code capacity}.
     */
    public final BufferMetrics bufferMetrics() {
        return new BufferMetrics(name, pending(), capacity(), dropped(), handled());
    }

    @Override
    public String toString() {
        return "BufferedGossipListener{" + name + ", pending=" + pending()
                + "/" + capacity() + ", handled=" + handled() + ", dropped=" + dropped() + '}';
    }
}
