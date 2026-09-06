package com.chaconneai.spreader.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory with a naming prefix, so threads can be told apart in production dumps.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class NamedThreadFactory implements ThreadFactory {

    private final String prefix;
    private final boolean daemon;
    private final AtomicInteger counter = new AtomicInteger();

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
        t.setDaemon(daemon);
        return t;
    }
}
