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
package com.chaconneai.spreader.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The single entry point for thread pools.
 *
 * <h2>It belongs to spreader, so it depends on no container</h2>
 * {@code spreader} stands alone -- {@code slf4j-api}, a logging facade, is its only
 * dependency -- so it has to build and close its own pools. {@code spreader-commons},
 * which sits on top of it, does the opposite: its pools belong to Spring, see
 * {@code ExecutorServiceHolder} over there. The two postures differ because the two
 * layers do.
 *
 * <h2>Why funnel everything through here</h2>
 * Every scattered {@code Executors.newFixedThreadPool(4, ...)} shares one problem:
 * <b>an unbounded queue sits behind it</b>. That is the great trap in the JDK factory
 * methods -- the name mentions only the thread count and says nothing about the queue,
 * and the queue is what decides what happens under overload.
 *
 * <p>With an unbounded queue, a consumer that falls behind lets the queue grow until
 * the process runs out of memory, and <b>nothing anywhere reports a thing</b> on the
 * way. Overload has been hidden completely.
 *
 * <p>So every factory method here <b>requires</b> a capacity, and each method name says
 * outright what happens when it fills -- forcing the caller to settle that question at
 * the moment the pool is created.
 *
 * <p>The factory methods on {@code java.util.concurrent.Executors} are
 * <b>deliberately unused</b> in this project, for exactly that reason.
 *
 * <h2>What to do when full: it depends who the producer is</h2>
 * That is the <b>only</b> criterion for choosing among these:
 *
 * <table border="1">
 *   <caption>When each policy applies</caption>
 *   <tr><th>Method</th><th>When full</th><th>Use when</th></tr>
 *   <tr><td>{@link #backpressured}</td><td>the calling thread runs it</td>
 *       <td>the producer is a <b>business thread</b> -- it can afford to be slowed down,
 *           which is exactly what backpressure is for</td></tr>
 *   <tr><td>{@link #discarding}</td><td>discard</td>
 *       <td>the producer is a <b>network or dispatch thread</b> -- stalling it takes the
 *           whole node down with it</td></tr>
 *   <tr><td>{@link #dropOldest}</td><td>drop the oldest</td>
 *       <td>only the latest state matters (heartbeats, liveness probes); the old ones
 *           have already lost their meaning</td></tr>
 * </table>
 *
 * <p><b>Never use {@link #backpressured} where the producer is a network thread.</b>
 * It would have the network receive thread execute business logic, and a single slow
 * method would then stop gossip heartbeats going out, getting this node declared failed
 * by everyone else -- taking the entire node out of the cluster to avoid dropping one
 * message.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class ExecutorUtils {

    /** How long an idle thread survives. Core threads included: a pool that has gone
     *  quiet should not keep holding threads. */
    private static final long KEEP_ALIVE_SECONDS = 60L;

    /**
     * Minimum interval between overload log lines, in milliseconds.
     *
     * <p>Five seconds is the trade between noticing quickly and not flooding: an
     * overload usually lasts seconds to tens of seconds, and at this interval it leaves
     * several lines -- enough to see the duration and the magnitude -- without burying
     * everything else in the log.
     */
    private static final long WARN_INTERVAL_MS = 5_000L;

    /**
     * Sits under {@code com.chaconneai.spreader}, on the same tree as the rest of the
     * component's logging, so configuring
     * {@code logging.level.com.chaconneai.spreader} covers this too.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorUtils.class);

    private ExecutorUtils() {
    }

    /**
     * A bounded pool that, <b>when full, runs the task on the calling thread</b>.
     *
     * <p>This is the strongest form of backpressure: the thread submitting work is made
     * to do it, and naturally slows down. Use it only where <b>the producer can afford
     * to be slowed</b> -- a business thread can, a network thread cannot.
     *
     * @param name          thread name prefix
     * @param threads       thread count
     * @param queueCapacity queue capacity. <b>Unbounded is not allowed</b>; see the class
     *                      documentation for why
     */
    public static ExecutorService backpressured(String name, int threads, int queueCapacity) {
        return pool(name, threads, queueCapacity, new LoggingCallerRunsPolicy(name));
    }

    /**
     * A bounded pool that <b>discards new tasks when full</b>.
     *
     * <p>For places where the producer absolutely must not block: network receive
     * threads, event dispatch threads.
     *
     * <p>Discarding is not giving up. It converts overload from "pile up silently until
     * the heap is gone" into "drop it explicitly, and count it". The layer above usually
     * retries anyway (acks on business messages, timeout-driven resends), so losing one
     * message is not a disaster. Taking the whole process down is.
     */
    public static ExecutorService discarding(String name, int threads, int queueCapacity) {
        return pool(name, threads, queueCapacity, new LoggingDiscardPolicy(name));
    }

    /**
     * A bounded pool that <b>drops the oldest task when full</b>.
     *
     * <p>Suited to work where only the newest item means anything: heartbeats, liveness
     * probes, metric reporting. The old tasks queued behind have already expired in that
     * setting, and dropping them is the correct thing to do.
     */
    public static ExecutorService dropOldest(String name, int threads, int queueCapacity) {
        return pool(name, threads, queueCapacity, new LoggingDiscardOldestPolicy(name));
    }

    /**
     * A bounded pool that <b>throws {@code RejectedExecutionException} when full</b>.
     *
     * <p>For places where the caller <b>can actually do something about being
     * rejected</b> -- most typically, answer immediately with "I am overloaded". That
     * beats discarding silently: the other side learns at once that it should degrade,
     * instead of waiting out a timeout while still sending new requests.
     *
     * <p>The price is that the caller has to catch it. If you are not going to handle it,
     * use {@link #discarding} instead.
     */
    public static ExecutorService rejecting(String name, int threads, int queueCapacity) {
        return pool(name, threads, queueCapacity, new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * A single-threaded executor: <b>serial and bounded</b>.
     *
     * <p>{@code Executors.newSingleThreadExecutor()} is unbounded as well, which is why
     * it is not used.
     *
     * <p>For work whose <b>order must hold</b>: cluster event dispatch, state machine
     * transitions. Running these concurrently corrupts state outright, so being serial
     * is a correctness requirement here, not a performance trade.
     */
    public static ExecutorService singleThread(String name, int queueCapacity) {
        return pool(name, 1, queueCapacity, new LoggingCallerRunsPolicy(name));
    }

    /**
     * A single-threaded scheduler.
     *
     * <p>{@code setRemoveOnCancelPolicy(true)} is mandatory. Without it, a cancelled
     * periodic task stays in the queue until the moment it would have run. Where
     * scheduled tasks are created and cancelled constantly -- every lock and every
     * barrier may carry a renewal task -- those corpses hold memory the whole time.
     */
    public static ScheduledExecutorService scheduler(String name) {
        return scheduler(name, 1);
    }

    /**
     * A multi-threaded scheduler.
     *
     * <p>Single-threaded schedulers have a property that is easy to overlook: <b>one slow
     * task makes every task behind it wait</b>. Mix a cleanup that takes hundreds of
     * milliseconds with a renewal that has to be punctual and this stops being a
     * performance question and becomes a correctness one.
     *
     * <p>So whenever the tasks in a pool differ widely in duration, give it several
     * threads -- or split it into two pools outright.
     */
    public static ScheduledExecutorService scheduler(String name, int threads) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                Math.max(1, threads), new NamedThreadFactory(name, true));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * Shuts an executor down, politely first.
     *
     * <p>{@code shutdown()} first, letting submitted work finish; {@code shutdownNow()}
     * only once patience runs out. Going straight to {@code shutdownNow()} interrupts
     * running tasks -- and those tasks may be holding a lock, or sending back an answer
     * somebody is waiting on.
     *
     * @param timeoutMs how long to wait at most. A shutdown path should not drag; a few
     *                  seconds is plenty
     */
    public static void shutdownGracefully(ExecutorService executor, long timeoutMs) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** How many tasks are queued. Not every implementation can answer; -1 when it cannot. */
    public static int queueSize(ExecutorService executor) {
        return executor instanceof ThreadPoolExecutor pool ? pool.getQueue().size() : -1;
    }

    private static ExecutorService pool(String name, int threads, int queueCapacity,
                                        RejectedExecutionHandler handler) {
        int size = Math.max(1, threads);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                size, size,
                KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(Math.max(1, queueCapacity)),
                new NamedThreadFactory(name, true),
                new CountingHandler(handler));
        // Core threads may be reclaimed too: a pool idle most of the time should not
        // sit on threads
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * How many tasks were turned away because the queue was full.
     *
     * @return -1 when there is no counter -- when the pool was built elsewhere, say
     */
    public static long rejectedCount(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor pool
                && pool.getRejectedExecutionHandler() instanceof CountingHandler h) {
            return h.count();
        }
        return -1L;
    }

    /** Queue capacity, or -1 when it cannot be determined. */
    public static int queueCapacity(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor pool) {
            BlockingQueue<Runnable> q = pool.getQueue();
            return q.size() + q.remainingCapacity();
        }
        return -1;
    }

    /**
     * Wraps a policy so that rejections are counted before the real policy runs.
     *
     * <h2>Why counting is not optional</h2>
     * {@code DiscardPolicy} is implemented as an <b>empty method</b> -- the task is
     * simply gone, with no exception, no log line and no trace. Discarding is deliberate
     * here (pushing back would stall the gossip dispatch thread and get this node
     * declared failed instead), but "the design permits dropping" is not the same as
     * "nobody needs to be told".
     *
     * <p>Without this counter, overload is completely silent: latency normal, error rate
     * zero, everything looking fine -- only a batch of messages missing.
     *
     * <p>It <b>only counts</b>. The logging is each policy's own job -- see
     * {@link LoggingRejectionPolicy}. Splitting it that way lets {@code AbortPolicy} stay
     * the plain JDK one: it needs no log line, but it still needs to be counted.
     */
    private static final class CountingHandler implements RejectedExecutionHandler {

        private final RejectedExecutionHandler delegate;
        private final LongAdder rejected = new LongAdder();

        CountingHandler(RejectedExecutionHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            rejected.increment();
            delegate.rejectedExecution(r, executor);
        }

        long count() {
            return rejected.sum();
        }
    }

    /**
     * A rejection policy that leaves a warn line behind.
     *
     * <h2>Why not just use the three from the JDK</h2>
     * The JDK's {@code CallerRunsPolicy}, {@code DiscardPolicy} and
     * {@code DiscardOldestPolicy} are all <b>silent</b>: {@code Discard} is an empty
     * method, {@code DiscardOldest} quietly throws away the head of the queue, and
     * {@code CallerRuns} loses no task but stalls the calling thread. In all three cases,
     * overload shows up nowhere at all.
     *
     * <p>{@code CallerRuns} is the one that most needs a log line. It looks the safest --
     * not a single task lost -- but the price is a stalled submitting thread. In gossip
     * the submitter is the network receive thread, and slowing it down manifests as
     * <b>this node being declared failed by everyone else</b>. Nothing about that symptom
     * points at a thread pool.
     *
     * <p>{@code AbortPolicy} is excluded: it throws {@code RejectedExecutionException},
     * so the caller already knows, and a log line would only repeat it. That one stays
     * the plain JDK policy.
     *
     * <h2>A subclass answers only two questions</h2>
     * What to do with the rejected task ({@link #reject}) and what the consequence of
     * doing that is ({@link #consequence}). Throttling and log formatting are handled
     * once, here, so three policies cannot each get it slightly wrong.
     */
    private abstract static class LoggingRejectionPolicy implements RejectedExecutionHandler {

        private final String poolName;
        private final LongAdder sinceLastLog = new LongAdder();
        private final LongAdder total = new LongAdder();
        private final AtomicLong nextLogAt = new AtomicLong();

        LoggingRejectionPolicy(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public final void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // Log first, then handle. CallerRunsPolicy runs the whole task inside
            // reject() before returning, which can take a long while -- logging after
            // that would put the line well behind the incident
            maybeWarn(executor);
            reject(r, executor);
        }

        /** What this policy does with the rejected task. Semantics identical to the
         *  corresponding JDK policy. */
        abstract void reject(Runnable r, ThreadPoolExecutor executor);

        /** One sentence naming the consequence; it goes straight into the log for a human to read. */
        abstract String consequence();

        /**
         * Logs, but with throttling.
         *
         * <p>Under overload, rejections arrive <b>continuously</b>, and one line each
         * would produce tens of thousands in an instant -- the logging itself becoming a
         * new bottleneck and making the overload worse. So the first one is logged
         * immediately (an incident should be visible at once), and after that at most one
         * line per {@value ExecutorUtils#WARN_INTERVAL_MS} milliseconds, carrying the
         * count accumulated in between. <b>What gets collapsed is lines, not
         * information.</b>
         */
        private void maybeWarn(ThreadPoolExecutor executor) {
            sinceLastLog.increment();
            total.increment();
            long now = System.currentTimeMillis();
            long due = nextLogAt.get();
            // A failed CAS means another thread has just logged; yield to it rather than retry
            if (now < due || !nextLogAt.compareAndSet(due, now + WARN_INTERVAL_MS)) {
                return;
            }
            BlockingQueue<Runnable> q = executor.getQueue();
            LOG.warn("Thread pool [{}] overloaded: {}. {} rejection(s) in the last {}ms, "
                            + "{} in total, queue={}/{}, active threads={}/{}",
                    poolName, consequence(),
                    sinceLastLog.sumThenReset(), WARN_INTERVAL_MS, total.sum(),
                    q.size(), q.size() + q.remainingCapacity(),
                    executor.getActiveCount(), executor.getPoolSize());
        }
    }

    /** {@link ThreadPoolExecutor.CallerRunsPolicy} plus a warn line. */
    private static final class LoggingCallerRunsPolicy extends LoggingRejectionPolicy {

        LoggingCallerRunsPolicy(String poolName) {
            super(poolName);
        }

        @Override
        void reject(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                r.run();
            }
        }

        @Override
        String consequence() {
            return "the submitting thread runs it instead (that thread is slowed down; "
                    + "if it is a network thread, this node may be declared failed as a result)";
        }
    }

    /** {@link ThreadPoolExecutor.DiscardPolicy} plus a warn line. */
    private static final class LoggingDiscardPolicy extends LoggingRejectionPolicy {

        LoggingDiscardPolicy(String poolName) {
            super(poolName);
        }

        @Override
        void reject(Runnable r, ThreadPoolExecutor executor) {
            // Same as the JDK: do nothing. The base class has already logged
        }

        @Override
        String consequence() {
            return "the task was discarded";
        }
    }

    /** {@link ThreadPoolExecutor.DiscardOldestPolicy} plus a warn line. */
    private static final class LoggingDiscardOldestPolicy extends LoggingRejectionPolicy {

        LoggingDiscardOldestPolicy(String poolName) {
            super(poolName);
        }

        @Override
        void reject(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                executor.getQueue().poll();
                executor.execute(r);
            }
        }

        @Override
        String consequence() {
            return "the oldest queued task was dropped to make room for this one";
        }
    }
}
