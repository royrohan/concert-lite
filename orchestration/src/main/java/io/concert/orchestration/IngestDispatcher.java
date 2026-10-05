package io.concert.orchestration;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.api.LockRequest;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ingest hand-off from a shard batch into Temporal:
 *
 * <ol>
 *   <li>dedupe the whole batch in one DSQL round trip; drop duplicates,
 *   <li>group the survivors by their first lock key; groups run in parallel, events inside a group
 *       run sequentially in shard order (that is the per-key ordering guarantee),
 *   <li>for each event, durably enqueue it in Temporal and return (we do not wait for processing):
 *       an {@code acquire} update on the lock of its first sorted key, waiting only for ACCEPTED
 *       (direct types: UpdateWithStart on the entity instead),
 *   <li>mark the batch DISPATCHED.
 * </ol>
 */
public final class IngestDispatcher implements AutoCloseable {

    private final StateStore store;
    private final TraceWriter traces;
    private final DispatchActivitiesImpl dispatch;
    private final Set<String> directSmTypes;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore inFlight;

    private final AtomicLong dispatched = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    public IngestDispatcher(
            StateStore store,
            TraceWriter traces,
            DispatchActivitiesImpl dispatch,
            Set<String> directSmTypes,
            int maxParallelGroups) {
        this.store = store;
        this.traces = traces;
        this.dispatch = dispatch;
        this.directSmTypes = Set.copyOf(directSmTypes);
        this.inFlight = new Semaphore(maxParallelGroups);
    }

    public long dispatchedCount() {
        return dispatched.get();
    }

    public long droppedCount() {
        return dropped.get();
    }

    public void dispatchBatch(List<EventEnvelope> events) {
        if (events.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        Set<String> claimed = store.claimForDispatch(events.stream().map(EventEnvelope::eventId).toList(), now);

        Map<String, List<EventEnvelope>> groups = new LinkedHashMap<>();
        Set<String> seenInBatch = new HashSet<>();
        for (EventEnvelope e : events) {
            if (!claimed.contains(e.eventId()) || !seenInBatch.add(e.eventId())) {
                dropped.incrementAndGet();
                traces.add(new TraceRow(e.eventId(), now, TraceRow.Stage.DROPPED, e.entityKey(), null, "duplicate"));
                continue;
            }
            long claimedAt = System.currentTimeMillis();
            traces.add(new TraceRow(e.eventId(), claimedAt, TraceRow.Stage.RECEIVED, e.entityKey(), null,
                    "kinesisMs=" + (e.sourceTsMillis() > 0 ? e.ingestTsMillis() - e.sourceTsMillis() : -1)
                            + " dedupeMs=" + (claimedAt - e.ingestTsMillis()) + " keys=" + e.effectiveLockKeys()));
            groups.computeIfAbsent(e.effectiveLockKeys().get(0), k -> new ArrayList<>()).add(e);
        }

        List<Future<?>> running = new ArrayList<>(groups.size());
        for (List<EventEnvelope> group : groups.values()) {
            inFlight.acquireUninterruptibly();
            running.add(pool.submit(() -> {
                try {
                    for (EventEnvelope e : group) {
                        dispatchOne(e);
                    }
                } finally {
                    inFlight.release();
                }
            }));
        }
        for (Future<?> f : running) {
            try {
                f.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while dispatching", ie);
            } catch (ExecutionException ee) {
                // Fail the batch: the consumer activity retries from the last checkpoint, dedupe
                // re-dispatches whatever is still RECEIVED, and Temporal ids absorb duplicates.
                throw ee.getCause() instanceof RuntimeException re ? re : new IllegalStateException(ee.getCause());
            }
        }
        store.markDispatched(seenInBatch);
    }

    void dispatchOne(EventEnvelope e) {
        if (e.isSingleKey() && directSmTypes.contains(e.smType())) {
            dispatch.dispatchDirect(e); // ordering is fixed once ACCEPTED; completion is traced async
        } else {
            // Single- and multi-key alike: enqueue on the first sorted key; the lock chain does the rest.
            dispatch.enqueueLock(e.effectiveLockKeys().get(0), LockRequest.first(e, System.currentTimeMillis()));
        }
        dispatched.incrementAndGet();
    }

    @Override
    public void close() {
        pool.close();
    }
}
