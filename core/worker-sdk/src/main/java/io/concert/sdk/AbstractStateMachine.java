package io.concert.sdk;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionCommit;
import io.concert.common.api.TransitionResult;
import java.util.List;
import java.util.Optional;

/**
 * Base class for a state machine type: the transition table and the decision code. It is a plain object, not a
 * Temporal workflow.
 *
 * <p><b>How an event is applied.</b> The entity lives in the state store (Postgres / Aurora DSQL); there is no entity
 * workflow. The last lock of the event's chain calls the {@code ApplyTransition} activity on {@code sm-<smType>}
 * ({@link TransitionActivitiesImpl}), which, for each event:
 * <ol>
 *   <li>loads the entity's committed row (and returns the stored result if the event was applied before);
 *   <li>creates a fresh instance of this class, bound to that row, and lets it decide the event;
 *   <li>commits the decision in one database transaction: the new state row (only from the version it was decided
 *       on), the applied-event record and the outbox of emitted events; on a version conflict it loads and decides
 *       again;
 *   <li>delivers the outbox and runs the post-commit hooks.
 * </ol>
 * The entity's events are serialized by its lock (the entity key is always one of an event's lock keys), so one
 * transition runs at a time per entity.
 *
 * <p>Decisions must not perform side effects: they run before the commit and may run again (after a conflict, or a
 * retry of the activity). Take time from {@link #now()}, the transition's timestamp, not the system clock. Effects on
 * other entities are emitted events.
 */
public abstract class AbstractStateMachine {

    private String smType;
    private String instanceKey;
    private String state;
    private String data;
    private long version;
    private long now;
    private EntitySnapshotPublisher snapshots = EntitySnapshotPublisher.logging();

    /** The transition table. Must be deterministic (build it once, e.g. in a static field). */
    protected abstract StateMachineSpec spec();

    /** Computes the entity's data after an accepted transition. Default: keep the event payload as the latest data. */
    protected String applyData(String fromState, String toState, String currentData, EventEnvelope event) {
        return event.payload() != null ? event.payload() : currentData;
    }

    /**
     * The outcome of {@link #decide}: the entity's new data for an accepted transition, or the reason
     * the event is rejected (state, data and version then stay unchanged).
     */
    protected record Decision(boolean accepted, String data, String rejection) {

        public static Decision accept(String data) {
            return new Decision(true, data, null);
        }

        public static Decision reject(String reason) {
            return new Decision(false, null, reason);
        }
    }

    /**
     * Decides an event that has a transition {@code fromState -> toState}: computes the new data, or rejects the
     * event. May run more than once for one event. Default: accept with {@link #applyData}.
     */
    protected Decision decide(String fromState, String toState, String currentData, EventEnvelope event) {
        return Decision.accept(applyData(fromState, toState, currentData, event));
    }

    /**
     * The events the accepted {@link #decide} of {@code event} emitted, removed from the subclass's buffer; they are
     * committed in the transition's outbox. Default: none.
     */
    protected List<EventEnvelope> takeEmits(EventEnvelope event) {
        return List.of();
    }

    /**
     * Called once the transition of {@code result} (accepted or not) has been committed by this attempt (not for a
     * redelivered event that was applied before). Not part of the transaction; a failure here is logged and does not
     * fail the event.
     */
    protected void onPersisted(TransitionResult result, EventEnvelope event) {}

    /** Called once an accepted transition has been committed (see {@link #onPersisted} for the rules). */
    protected void onTransition(String fromState, String toState, EventEnvelope event) {}

    /** The transition's timestamp (epoch millis): one value for the whole decision. Use it instead of the clock. */
    protected long now() {
        return now;
    }

    /** The entity's current data JSON (may be {@code null}). */
    protected String currentData() {
        return data;
    }

    /** The state machine type. */
    protected String smType() {
        return smType;
    }

    /** The entity's instance key. */
    protected String instanceKey() {
        return instanceKey;
    }

    /** The entity's id, {@code smType:instanceKey} (its lock key, and the {@code sm_state} key). */
    protected String entityId() {
        return WorkflowIds.entity(smType, instanceKey);
    }

    /** The completed-entity snapshot publisher (egress; {@code KAFKA_BOOTSTRAP}). */
    protected EntitySnapshotPublisher snapshots() {
        return snapshots;
    }

    // ------------------------------------------------------------------------------------- used by the activity

    /** Binds a fresh instance to the entity as committed ({@code row} null: a new entity, in the initial state). */
    final void bind(String smType, String instanceKey, SmStateRow row, long nowMillis, EntitySnapshotPublisher snapshots) {
        this.smType = smType;
        this.instanceKey = instanceKey;
        this.state = row == null ? spec().initialState() : row.state();
        this.data = row == null ? null : row.data();
        this.version = row == null ? 0 : row.version();
        this.now = nowMillis;
        if (snapshots != null) {
            this.snapshots = snapshots;
        }
    }

    /** A decision and the transaction that commits it. */
    record Attempt(TransitionCommit commit, TraceRow trace) {}

    /** Decides {@code event} on the bound entity and builds the transaction that commits the decision. */
    final Attempt attempt(EventEnvelope event) {
        String id = entityId();
        String from = state;
        Optional<String> next = spec().next(from, event.eventType());
        Decision decision = next.isEmpty() ? null : decide(from, next.get(), data, event);
        if (decision != null && decision.accepted()) {
            String to = next.get();
            TransitionResult r = new TransitionResult(event.eventId(), from, to, true, null, version + 1);
            SmStateRow row = new SmStateRow(id, smType, to, decision.data(), version + 1, event.eventId(), now);
            return new Attempt(new TransitionCommit(id, event.eventId(), version, r, row, takeEmits(event), now),
                    new TraceRow(event.eventId(), now, TraceRow.Stage.TRANSITION, id, null,
                            from + " -> " + to + " (" + event.eventType() + ")"));
        }
        takeEmits(event); // a rejected decision emits nothing: drop whatever it buffered
        String reason = decision == null ? "no transition from " + from + " on " + event.eventType() : decision.rejection();
        TransitionResult r = new TransitionResult(event.eventId(), from, from, false, reason, version);
        return new Attempt(new TransitionCommit(id, event.eventId(), version, r, null, List.of(), now),
                new TraceRow(event.eventId(), now, TraceRow.Stage.REJECTED, id, null,
                        from + " x " + event.eventType() + (decision == null ? "" : ": " + reason)));
    }

    /** After this attempt committed: moves the instance to the committed state and runs the hooks. */
    final void committed(Attempt a, EventEnvelope event) {
        TransitionResult r = a.commit().result();
        if (r.accepted()) {
            state = r.toState();
            data = a.commit().row().data();
            version = r.version();
            onTransition(r.fromState(), r.toState(), event);
        }
        onPersisted(r, event);
    }
}
