package io.concert.sdk;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.CommitOutcome;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityLoad;
import io.concert.common.api.EntityPersistenceActivities;
import io.concert.common.api.EntitySnapshot;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionCommit;
import io.concert.common.api.TransitionRecord;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.events.EventEnqueueActivities;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * Base class for a state machine type. Subclasses provide the transition table and optionally a
 * data hook; the base class owns the Temporal plumbing.
 *
 * <p><b>DB-truth.</b> The entity lives in the state store (Postgres / Aurora DSQL), not in this workflow. Each event
 * is one database transaction, committed by {@link EntityPersistenceActivities#commitTransition}: the new state row,
 * the applied-event record (event id and result) and the outbox rows of the events the transition emitted are written
 * together, or not at all. The workflow orders the events and runs the decision code:
 *
 * <ul>
 *   <li>events are decided strictly in update acceptance order, one at a time, even when {@link #decide} yields;
 *   <li>the decision is made on a cached copy of the entity (loaded from the store when a run starts or the cache is
 *       in doubt); the commit is conditional on that version, so a stale cache is detected (CONFLICT), reloaded and
 *       the event decided again;
 *   <li>an event already applied (a redelivery, also across continue-as-new) gets the stored result back and changes
 *       nothing;
 *   <li>after commit, the outbox is delivered to the emitted events' lock chains (a regular activity); a sweeper in the
 *       worker delivers whatever a crash left behind;
 *   <li>continues-as-new after {@link #continueAsNewAfter()} events, carrying the cache.
 * </ul>
 *
 * Decisions must not perform side effects: they run before the commit and are not part of it (and may run again after
 * a conflict). Side effects belong in emitted events.
 */
public abstract class AbstractStateMachine implements EntityWorkflow {

    private static final int RECENT_LIMIT = 50;
    /** Conflicts in a row before the event fails: entity events are serialized, so even one is unusual. */
    private static final int MAX_CONFLICTS = 10;

    /** The store may be down for a while: keep trying for as long as the lock's dispatch waits (1 h). */
    private final EntityPersistenceActivities persistence = Workflow.newLocalActivityStub(
            EntityPersistenceActivities.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .setScheduleToCloseTimeout(Duration.ofHours(1))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofMillis(100))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .build())
                    .build());

    /** Outbox delivery: a regular activity, so it never holds this workflow task open while a lock waits on us. */
    private final EventEnqueueActivities outbox = Workflow.newActivityStub(EventEnqueueActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofMillis(100))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setMaximumAttempts(0)
                            .build())
                    .build());

    private String smType;
    private String instanceKey;
    /** The cached entity; {@link #cacheValid} says whether it may be decided on without loading. */
    private String state;
    private String data;
    private long version;
    private boolean cacheValid;
    /** First accepted transition (0: none yet) and latest accepted transition, workflow time. */
    private long createdAtMillis;
    private long lastAcceptedMillis;
    private long handledThisRun;
    private boolean initialized;
    /** FIFO gate for {@link #decide}: tickets handed out in update acceptance order, and the one being served. */
    private long ticketsIssued;
    private long ticketServed;
    private final Deque<TransitionRecord> recent = new ArrayDeque<>();
    /** Outbox deliveries started by this run; it may not continue-as-new before they complete. */
    private final List<Promise<Void>> deliveries = new ArrayList<>();

    /** The transition table. Must be deterministic (build it once, e.g. in a static field). */
    protected abstract StateMachineSpec spec();

    /**
     * Computes the entity's data after an accepted transition. Runs inside workflow code, so it must
     * be deterministic (no I/O, clocks or randomness). Default: keep the event payload as the latest data.
     */
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
     * Decides an event that has a transition {@code fromState -> toState}: computes the new data, or
     * rejects the event. Runs inside workflow code, so it must be deterministic; it may run more than once for one
     * event (after a version conflict). Default: accept with {@link #applyData}.
     */
    protected Decision decide(String fromState, String toState, String currentData, EventEnvelope event) {
        return Decision.accept(applyData(fromState, toState, currentData, event));
    }

    /**
     * The events the latest accepted {@link #decide} of {@code event} emitted, removed from the subclass's buffer; they
     * are committed in the transition's outbox. Default: none.
     */
    protected List<EventEnvelope> takeEmits(EventEnvelope event) {
        return List.of();
    }

    /**
     * Called after the transition of {@code result} (accepted or not) has been committed by this call (not for a
     * redelivered event that was applied before).
     * Other events may have been decided meanwhile, so read current state rather than assuming it is unchanged.
     */
    protected void onPersisted(TransitionResult result, EventEnvelope event) {}

    /**
     * Called once an accepted transition has been committed, before the update completes. Not part of the
     * transaction: work here is not undone if something fails later, and is not retried. Prefer emitted events for
     * side effects.
     */
    protected void onTransition(String fromState, String toState, EventEnvelope event) {}

    protected int continueAsNewAfter() {
        return 500;
    }

    /**
     * Whether work started by this run (e.g. an async activity) must finish before it may
     * continue-as-new: a run that ends abandons its pending activities. Evaluated in a
     * {@code Workflow.await} condition, so it must be cheap and must not mutate state. Default: false.
     */
    protected boolean hasPendingWork() {
        return false;
    }

    @Override
    public void run(EntityInit init) {
        smType = init.smType();
        instanceKey = init.instanceKey();
        // a continued run carries the cache; a fresh run loads the entity before its first decision
        cacheValid = init.state() != null;
        state = init.state() != null ? init.state() : spec().initialState();
        data = init.data();
        version = init.version();
        if (init.recent() != null) {
            init.recent().forEach(recent::addLast);
        }
        createdAtMillis = init.createdAtMillis();
        if (createdAtMillis == 0 && version > 0) {
            // Input from before createdAtMillis existed: the earliest remembered transition is the best guess.
            createdAtMillis = recent.stream().filter(TransitionRecord::accepted)
                    .mapToLong(TransitionRecord::tsMillis).min().orElse(0);
        }
        initialized = true;

        // Continue-as-new is the only way a run ends, so this is the one place that must wait for
        // pending work (deliveries, hasPendingWork) besides the update handlers.
        int limit = continueAsNewAfter();
        Workflow.await(() -> (handledThisRun >= limit || Workflow.getInfo().isContinueAsNewSuggested())
                && Workflow.isEveryHandlerFinished() && deliveries.stream().allMatch(Promise::isCompleted)
                && !hasPendingWork());
        Workflow.continueAsNew(new EntityInit(smType, instanceKey, cacheValid ? state : null, cacheValid ? data : null,
                cacheValid ? version : 0, List.copyOf(recent), createdAtMillis));
    }

    @Override
    public void validateHandle(EventEnvelope event) {
        if (event == null || event.eventType() == null || event.eventType().isBlank()) {
            throw new IllegalArgumentException("eventType is required");
        }
        String expected = Workflow.getInfo().getWorkflowId();
        if (!expected.equals(WorkflowIds.entity(event.smType(), event.instanceKey()))) {
            throw new IllegalArgumentException("event " + event.eventId() + " targets "
                    + event.entityKey() + ", not " + expected);
        }
    }

    @Override
    public TransitionResult handle(EventEnvelope event) {
        // Take a ticket before the first yield: tickets follow update acceptance order. An update can
        // be delivered in the same workflow task as the start, so also wait for run() to set up.
        long ticket = ticketsIssued++;
        Workflow.await(() -> initialized && ticketServed == ticket);

        TransitionResult result;
        List<EventEnvelope> emitted = List.of();
        boolean committedNow = false;
        try {
            handledThisRun++;
            result = cacheValid ? null : load(event);
            for (int conflicts = 0; result == null; conflicts++) {
                if (conflicts >= MAX_CONFLICTS) {
                    throw ApplicationFailure.newFailure("entity " + workflowId() + " kept changing under event "
                            + event.eventId(), "TransitionConflict");
                }
                Attempt a = attempt(event);
                CommitOutcome out = persistence.commitTransition(a.commit(), a.trace());
                switch (out.status()) {
                    case COMMITTED -> {
                        result = a.commit().result();
                        committedNow = true;
                        if (result.accepted()) {
                            adopt(a.commit().row());
                            lastAcceptedMillis = a.commit().nowMillis();
                            if (createdAtMillis == 0) {
                                createdAtMillis = lastAcceptedMillis;
                            }
                            emitted = a.commit().outbox();
                        }
                        remember(new TransitionRecord(event.eventId(), event.eventType(), result.fromState(),
                                result.toState(), result.accepted(), a.commit().nowMillis()));
                    }
                    case ALREADY_APPLIED -> {
                        // a redelivery: the store has the first answer; our cache may be behind it
                        result = out.result();
                        cacheValid = false;
                    }
                    case CONFLICT -> adopt(out.current()); // decided on a stale copy: decide again on the stored one
                }
            }
        } finally {
            ticketServed++;
        }
        if (!committedNow) {
            return result; // applied before (a redelivery): its hooks ran then, its outbox is delivered or swept
        }
        if (!emitted.isEmpty()) {
            deliveries.add(Async.procedure(outbox::deliverOutbox, emitted, event.eventId()));
            deliveries.removeIf(Promise::isCompleted);
        }
        if (result.accepted()) {
            onTransition(result.fromState(), result.toState(), event);
        }
        onPersisted(result, event);
        return result;
    }

    /** Loads the entity into the cache; returns the stored result if the event was applied before, else null. */
    private TransitionResult load(EventEnvelope event) {
        EntityLoad l = persistence.loadEntity(workflowId(), event.eventId());
        adopt(l.row());
        return l.applied();
    }

    /** Replaces the cache with the stored entity (null: none yet, the initial state). */
    private void adopt(SmStateRow row) {
        if (row == null) {
            state = spec().initialState();
            data = null;
            version = 0;
        } else {
            state = row.state();
            data = row.data();
            version = row.version();
        }
        cacheValid = true;
    }

    private record Attempt(TransitionCommit commit, TraceRow trace) {}

    /** Decides the event on the cached entity and builds the transaction that commits the decision. */
    private Attempt attempt(EventEnvelope event) {
        String from = state;
        long now = Workflow.currentTimeMillis();
        Optional<String> next = spec().next(from, event.eventType());
        Decision decision = next.isEmpty() ? null : decide(from, next.get(), data, event);
        if (decision != null && decision.accepted()) {
            String to = next.get();
            TransitionResult r = new TransitionResult(event.eventId(), from, to, true, null, version + 1);
            SmStateRow row = new SmStateRow(workflowId(), smType, to, decision.data(), version + 1, event.eventId(), now);
            return new Attempt(new TransitionCommit(workflowId(), event.eventId(), version, r, row, takeEmits(event), now),
                    new TraceRow(event.eventId(), now, TraceRow.Stage.TRANSITION, workflowId(), null,
                            from + " -> " + to + " (" + event.eventType() + ")"));
        }
        takeEmits(event); // a rejected decision emits nothing: drop whatever it buffered
        String reason = decision == null ? "no transition from " + from + " on " + event.eventType() : decision.rejection();
        TransitionResult r = new TransitionResult(event.eventId(), from, from, false, reason, version);
        return new Attempt(new TransitionCommit(workflowId(), event.eventId(), version, r, null, List.of(), now),
                new TraceRow(event.eventId(), now, TraceRow.Stage.REJECTED, workflowId(), null,
                        from + " x " + event.eventType() + (decision == null ? "" : ": " + reason)));
    }

    /** The entity's current data JSON (may be {@code null}). */
    protected String currentData() {
        return data;
    }

    /** The state machine type (valid once the workflow has started). */
    protected String smType() {
        return smType;
    }

    /** Workflow time of the entity's first accepted transition; 0 if there was none (or it is unknown). */
    protected long createdAtMillis() {
        return createdAtMillis;
    }

    /** Workflow time of the latest accepted transition in this run; 0 if none. */
    protected long lastAcceptedMillis() {
        return lastAcceptedMillis;
    }

    /** The entity's instance key (valid once the workflow has started). */
    protected String instanceKey() {
        return instanceKey;
    }

    @Override
    public EntitySnapshot snapshot() {
        return new EntitySnapshot(workflowId(), smType, instanceKey, state, data, version, handledThisRun, List.copyOf(recent));
    }

    private void remember(TransitionRecord r) {
        recent.addLast(r);
        while (recent.size() > RECENT_LIMIT) {
            recent.removeFirst();
        }
    }

    private String workflowId() {
        return Workflow.getInfo().getWorkflowId();
    }
}
