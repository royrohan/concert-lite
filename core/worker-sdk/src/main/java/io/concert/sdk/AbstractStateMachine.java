package io.concert.sdk;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EntityInit;
import io.concert.common.api.EntityPersistenceActivities;
import io.concert.common.api.EntitySnapshot;
import io.concert.common.api.EntityWorkflow;
import io.concert.common.api.SmStateRow;
import io.concert.common.api.TransitionRecord;
import io.concert.common.api.TransitionResult;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * Base class for a state machine type. Subclasses provide the transition table and optionally a
 * data hook; the base class owns the Temporal plumbing:
 *
 * <ul>
 *   <li>applies each {@code handle} update atomically: an event is decided only after the previous
 *       one has committed or been rejected, so the transition order is exactly the update acceptance
 *       order even when {@link #decide} yields (calls activities),
 *   <li>projects state to DSQL through a local activity (monotonic by version, so idempotent),
 *   <li>continues-as-new after {@link #continueAsNewAfter()} events to bound history size.
 * </ul>
 */
public abstract class AbstractStateMachine implements EntityWorkflow {

    private static final int RECENT_LIMIT = 50;

    private final EntityPersistenceActivities persistence = Workflow.newLocalActivityStub(
            EntityPersistenceActivities.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .setScheduleToCloseTimeout(Duration.ofMinutes(10))
                    .build());

    private String smType;
    private String instanceKey;
    private String state;
    private String data;
    private long version;
    /** First accepted transition (0: none yet) and latest accepted transition, workflow time. */
    private long createdAtMillis;
    private long lastAcceptedMillis;
    private long handledThisRun;
    private boolean initialized;
    /** FIFO gate for {@link #decide}: tickets handed out in update acceptance order, and the one being served. */
    private long ticketsIssued;
    private long ticketServed;
    private final Deque<TransitionRecord> recent = new ArrayDeque<>();

    /** The transition table. Must be deterministic (build it once, e.g. in a static field). */
    protected abstract StateMachineSpec spec();

    /**
     * Computes the entity's data after an accepted transition. Runs inside workflow code, so it must
     * be deterministic (no I/O, clocks or randomness); call activities from a subclass for side
     * effects. Default: keep the event payload as the latest data.
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
     * rejects the event. Runs inside workflow code, so it must be deterministic. It may yield (call
     * activities): the next event is decided only after this returns. Default: accept with
     * {@link #applyData}.
     */
    protected Decision decide(String fromState, String toState, String currentData, EventEnvelope event) {
        return Decision.accept(applyData(fromState, toState, currentData, event));
    }

    /**
     * Called after the state row of {@code result} (accepted or not) has been persisted. Other events
     * may have been decided meanwhile, so read current state rather than assuming it is unchanged.
     */
    protected void onPersisted(TransitionResult result, EventEnvelope event) {}

    /**
     * Side effects for an accepted transition (call activities from here: payments, emails, ...).
     * Runs after the in-memory state change and before the projection write; the update completes,
     * and the event's locks are released, only after it returns.
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
        // pending work (hasPendingWork) besides the update handlers.
        int limit = continueAsNewAfter();
        Workflow.await(() -> (handledThisRun >= limit || Workflow.getInfo().isContinueAsNewSuggested())
                && Workflow.isEveryHandlerFinished() && !hasPendingWork());
        Workflow.continueAsNew(new EntityInit(smType, instanceKey, state, data, version, List.copyOf(recent), createdAtMillis));
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
        // be delivered in the same workflow task as the start, so also wait for run() to load the
        // (possibly continued-as-new) state.
        long ticket = ticketsIssued++;
        Workflow.await(() -> initialized && ticketServed == ticket);

        String from;
        long now;
        TransitionResult result;
        TraceRow trace;
        boolean accepted;
        try {
            from = state;
            now = Workflow.currentTimeMillis();
            handledThisRun++;
            Optional<String> next = spec().next(from, event.eventType());
            Decision decision = next.isEmpty() ? null : decide(from, next.get(), data, event);
            accepted = decision != null && decision.accepted();
            if (next.isEmpty()) {
                result = new TransitionResult(event.eventId(), from, from, false,
                        "no transition from " + from + " on " + event.eventType(), version);
                trace = new TraceRow(event.eventId(), now, TraceRow.Stage.REJECTED, workflowId(), null,
                        from + " x " + event.eventType());
            } else if (!accepted) {
                result = new TransitionResult(event.eventId(), from, from, false, decision.rejection(), version);
                trace = new TraceRow(event.eventId(), now, TraceRow.Stage.REJECTED, workflowId(), null,
                        from + " x " + event.eventType() + ": " + decision.rejection());
            } else {
                String to = next.get();
                data = decision.data();
                state = to;
                version++;
                lastAcceptedMillis = now;
                if (createdAtMillis == 0) {
                    createdAtMillis = now;
                }
                result = new TransitionResult(event.eventId(), from, to, true, null, version);
                trace = new TraceRow(event.eventId(), now, TraceRow.Stage.TRANSITION, workflowId(), null,
                        from + " -> " + to + " (" + event.eventType() + ")");
            }
            remember(new TransitionRecord(event.eventId(), event.eventType(), from, result.toState(), result.accepted(), now));
        } finally {
            ticketServed++;
        }
        if (accepted) {
            onTransition(from, result.toState(), event);
        }

        // State is already updated; the projection write may yield without affecting ordering.
        persistence.persistState(
                new SmStateRow(workflowId(), smType, state, data, version, event.eventId(), now), trace);
        onPersisted(result, event);
        return result;
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
