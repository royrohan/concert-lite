package io.concert.sdk;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.CommitOutcome;
import io.concert.common.api.EntityLoad;
import io.concert.common.api.TransitionActivities;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.events.EventEnqueueActivities;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;
import io.temporal.failure.ApplicationFailure;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TransitionActivities}: one event, one database transaction (see {@link AbstractStateMachine}).
 *
 * <ul>
 *   <li>load the entity's committed row (one read);
 *   <li>decide on a fresh machine instance bound to the committed row;
 *   <li>commit state + applied event + outbox, conditional on the version decided on. ALREADY_APPLIED (a retry or
 *       redelivery of an event applied before) returns the stored result; CONFLICT loads and decides again;
 *   <li>after commit, deliver the outbox (the {@link OutboxSweeper} delivers it if this fails or the worker dies) and
 *       run the machine's post-commit hooks. Neither can fail the event: it is committed.
 * </ul>
 */
public final class TransitionActivitiesImpl implements TransitionActivities {
    private static final Logger log = LoggerFactory.getLogger(TransitionActivitiesImpl.class);

    /** Conflicts in a row before the attempt fails (and the activity is retried): events of an entity are serialized. */
    static final int MAX_CONFLICTS = 10;

    private final StateStore store;
    private final Map<String, Class<? extends AbstractStateMachine>> machines;
    private final EventEnqueueActivities outbox;
    private final EntitySnapshotPublisher snapshots;
    private final TraceWriter traces;

    /**
     * @param outbox delivers emitted events after commit ({@code null}: leave them to the sweeper)
     * @param snapshots completed-entity snapshots ({@code null}: logged only)
     */
    public TransitionActivitiesImpl(StateStore store, Map<String, Class<? extends AbstractStateMachine>> machines,
            EventEnqueueActivities outbox, EntitySnapshotPublisher snapshots, TraceWriter traces) {
        this.store = store;
        this.machines = Map.copyOf(machines);
        this.outbox = outbox;
        this.snapshots = snapshots;
        this.traces = traces;
    }

    @Override
    public TransitionResult applyTransition(EventEnvelope event, Map<String, Long> lockWaitMs) {
        long started = System.currentTimeMillis();
        String id = WorkflowIds.entity(event.smType(), event.instanceKey());
        if (lockWaitMs != null) {
            lockWaitMs.forEach((key, waited) -> trace(new TraceRow(event.eventId(), started, TraceRow.Stage.LOCK_GRANTED, id,
                    key, "waitedMs=" + waited)));
        }
        Class<? extends AbstractStateMachine> impl = machines.get(event.smType());
        if (impl == null) {
            throw ApplicationFailure.newNonRetryableFailure("this worker has no machine " + event.smType(), "UnknownMachine");
        }
        for (int conflicts = 0; conflicts < MAX_CONFLICTS; conflicts++) {
            // one read: the row. A redelivered event is recognized by the commit (ALREADY_APPLIED), not by a lookup
            EntityLoad load = store.loadEntity(id, null);
            AbstractStateMachine machine = newMachine(impl);
            machine.bind(event.smType(), event.instanceKey(), load.row(), System.currentTimeMillis(), snapshots);
            AbstractStateMachine.Attempt attempt = machine.attempt(event);
            CommitOutcome out = store.commitTransition(attempt.commit());
            switch (out.status()) {
                case COMMITTED -> {
                    trace(attempt.trace());
                    afterCommit(machine, attempt, event);
                    return done(event, attempt.commit().result());
                }
                case ALREADY_APPLIED -> {
                    return done(event, out.result());
                }
                case CONFLICT -> {
                    // the entity moved since it was loaded: load and decide again
                }
            }
        }
        throw ApplicationFailure.newFailure("entity " + id + " kept changing under event " + event.eventId(), "TransitionConflict");
    }

    private void afterCommit(AbstractStateMachine machine, AbstractStateMachine.Attempt attempt, EventEnvelope event) {
        List<EventEnvelope> emitted = attempt.commit().outbox();
        if (outbox != null && !emitted.isEmpty()) {
            try {
                outbox.deliverOutbox(emitted, event.eventId());
            } catch (RuntimeException e) {
                log.warn("outbox delivery of {} failed, the sweeper will deliver it: {}", event.eventId(), e.getMessage());
            }
        }
        try {
            machine.committed(attempt, event);
        } catch (RuntimeException e) {
            log.warn("post-commit hook of {} failed (the transition is committed): {}", event.eventId(), e.toString());
        }
    }

    private TransitionResult done(EventEnvelope event, TransitionResult r) {
        long now = System.currentTimeMillis();
        long e2e = event.ingestTsMillis() > 0 ? now - event.ingestTsMillis() : -1;
        long fromSource = event.sourceTsMillis() > 0 ? now - event.sourceTsMillis() : -1;
        // e2eMs: ingest read -> transition done. srcMs: producer timestamp -> transition done.
        trace(new TraceRow(event.eventId(), now, TraceRow.Stage.DONE, event.entityKey(), null,
                (r.accepted() ? "accepted" : "rejected") + " e2eMs=" + e2e + " srcMs=" + fromSource));
        return r;
    }

    private void trace(TraceRow row) {
        if (traces != null) {
            traces.add(row);
        }
    }

    private static AbstractStateMachine newMachine(Class<? extends AbstractStateMachine> impl) {
        try {
            return impl.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw ApplicationFailure.newNonRetryableFailure("cannot create " + impl.getName() + ": " + e, "UnknownMachine");
        }
    }
}
