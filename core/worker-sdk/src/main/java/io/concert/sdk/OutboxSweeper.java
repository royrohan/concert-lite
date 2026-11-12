package io.concert.sdk;

import io.concert.common.Env;
import io.concert.common.EventEnvelope;
import io.concert.sdk.events.EventEnqueueActivities;
import io.concert.store.StateStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers outbox rows that the fast path did not (DB-truth): the entity workflow hands its emitted events to their
 * lock chains right after the transition commits, so a row is only left pending if the worker died in between, or the
 * delivery activity is still retrying. Every {@code OUTBOX_SWEEP_SECONDS} (10) the sweeper delivers rows of its type
 * older than {@code OUTBOX_SWEEP_AFTER_SECONDS} (30). Several workers may sweep the same rows: delivery is idempotent
 * (child event ids, lock and entity deduplication). It also deletes sent outbox rows after
 * {@code OUTBOX_RETENTION_HOURS} (24) and applied-event records after {@code APPLIED_EVENT_RETENTION_HOURS} (168): a
 * redelivery older than that is no longer recognized.
 */
final class OutboxSweeper {
    private static final Logger log = LoggerFactory.getLogger(OutboxSweeper.class);

    private OutboxSweeper() {}

    static void start(StateStore store, String smType, EventEnqueueActivities enqueue) {
        long every = Env.getLong("OUTBOX_SWEEP_SECONDS", 10) * 1000;
        long after = Env.getLong("OUTBOX_SWEEP_AFTER_SECONDS", 30) * 1000;
        long keepSent = Env.getLong("OUTBOX_RETENTION_HOURS", 24) * 3_600_000L;
        long keepApplied = Env.getLong("APPLIED_EVENT_RETENTION_HOURS", 168) * 3_600_000L;
        if (every <= 0) {
            return;
        }
        Thread.ofVirtual().name("outbox-sweeper-" + smType).start(() -> {
            long lastCleanup = System.currentTimeMillis();
            while (true) {
                try {
                    Thread.sleep(every);
                    int n = sweepOnce(store, smType, enqueue, System.currentTimeMillis() - after);
                    if (n > 0) {
                        log.info("outbox sweeper delivered {} pending events of {}", n, smType);
                    }
                    long now = System.currentTimeMillis();
                    if (now - lastCleanup > 600_000) {
                        lastCleanup = now;
                        store.sweepEntityLog(now - keepSent, now - keepApplied, 1000);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (UnsupportedOperationException e) {
                    log.warn("outbox sweeper stopped: {}", e.getMessage());
                    return;
                } catch (RuntimeException e) {
                    log.warn("outbox sweep of {} failed: {}", smType, e.getMessage());
                }
            }
        });
    }

    /** Delivers the type's pending rows created before the cut-off, grouped by the event that emitted them. */
    static int sweepOnce(StateStore store, String smType, EventEnqueueActivities enqueue, long createdBeforeMillis) {
        List<EventEnvelope> pending = store.pendingOutbox(smType + ":", createdBeforeMillis, 500);
        Map<String, List<EventEnvelope>> byParent = new LinkedHashMap<>();
        for (EventEnvelope e : pending) {
            byParent.computeIfAbsent(String.valueOf(e.parentEventId()), k -> new java.util.ArrayList<>()).add(e);
        }
        byParent.forEach((parent, events) -> enqueue.deliverOutbox(events, parent));
        return pending.size();
    }
}
