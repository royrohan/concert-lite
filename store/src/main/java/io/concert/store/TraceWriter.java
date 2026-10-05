package io.concert.store;

import io.concert.common.TraceRow;
import io.concert.common.TraceSampling;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batches trace rows off the hot path: callers enqueue and return immediately; a virtual thread
 * writes one multi-row insert per {@code flushMillis} window (or per {@code maxBatch} rows). Tracing is best effort: when the
 * buffer is full rows are dropped and counted rather than slowing the pipeline down.
 */
public final class TraceWriter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TraceWriter.class);

    private final StateStore store;
    private final BlockingQueue<TraceRow> queue;
    private final int maxBatch;
    private final long flushMillis;
    private final AtomicLong dropped = new AtomicLong();
    private final Thread flusher;
    private volatile boolean running = true;

    public TraceWriter(StateStore store) {
        this(store, 100_000, 500, 50);
    }

    public TraceWriter(StateStore store, int capacity, int maxBatch, long flushMillis) {
        this.store = store;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.maxBatch = maxBatch;
        this.flushMillis = flushMillis;
        this.flusher = Thread.ofVirtual().name("trace-writer").start(this::loop);
    }

    public void add(TraceRow row) {
        if (!TraceSampling.sampled(row.eventId())) {
            return;
        }
        if (!queue.offer(row)) {
            dropped.incrementAndGet();
        }
    }

    public void addAll(List<TraceRow> rows) {
        rows.forEach(this::add);
    }

    public long droppedCount() {
        return dropped.get();
    }

    private void loop() {
        List<TraceRow> batch = new ArrayList<>(maxBatch);
        while (running || !queue.isEmpty()) {
            try {
                TraceRow first = queue.poll(flushMillis, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                // Linger up to flushMillis so a trickle of rows becomes one multi-row insert.
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(flushMillis);
                while (batch.size() < maxBatch && running) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        break;
                    }
                    TraceRow next = queue.poll(left, TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                    queue.drainTo(batch, maxBatch - batch.size());
                }
                store.appendTrace(batch);
            } catch (InterruptedException e) {
                running = false;
            } catch (RuntimeException e) {
                dropped.addAndGet(batch.size());
                log.warn("trace flush failed, dropped {} rows: {}", batch.size(), e.getMessage());
            } finally {
                batch.clear();
            }
        }
    }

    /** Flushes what is buffered (bounded wait) and stops. */
    @Override
    public void close() {
        running = false;
        try {
            flusher.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
