package com.orquestador.v1;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EMR stays up while a batch is in flight, including a write that runs for an hour.
 * The idle limit starts only when the active count returns to zero.
 */
public final class IdleWatch {

    private final AtomicInteger active = new AtomicInteger();
    private final long limitNanos;
    private long markNanos;

    public IdleWatch(Duration limit, long startedAtNanos) {
        if (limit.isNegative() || limit.isZero()) {
            throw new IllegalArgumentException("idle limit must be positive");
        }
        this.limitNanos = limit.toNanos();
        this.markNanos = startedAtNanos;
    }

    public void started() {
        active.incrementAndGet();
    }

    public void finished(long nowNanos) {
        int left = active.decrementAndGet();
        if (left < 0) {
            active.incrementAndGet();
            throw new IllegalStateException("finished with no active batch");
        }
        if (left == 0) {
            markNanos = nowNanos;
        }
    }

    public boolean shouldStop(long nowNanos) {
        return active.get() == 0 && nowNanos - markNanos >= limitNanos;
    }

    public int active() {
        return active.get();
    }
}
