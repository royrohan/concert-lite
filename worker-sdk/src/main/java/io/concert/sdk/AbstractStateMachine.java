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
 *   <li>applies each {@code handle} update atomically (no yield between reading and writing state),
 *       so the transition order is exactly the update acceptance order,
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
    private long handledThisRun;
    private boolean initialized;
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
     * Side effects for an accepted transition (call activities from here: payments, emails, ...).
     * Runs after the in-memory state change and before the projection write; the update completes,
     * and the event's locks are released, only after it returns.
     */
    protected void onTransition(String fromState, String toState, EventEnvelope event) {}

    protected int continueAsNewAfter() {
        return 500;
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
        initialized = true;

        int limit = continueAsNewAfter();
        Workflow.await(() -> (handledThisRun >= limit || Workflow.getInfo().isContinueAsNewSuggested())
                && Workflow.isEveryHandlerFinished());
        Workflow.continueAsNew(new EntityInit(smType, instanceKey, state, data, version, List.copyOf(recent)));
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
        // An update can be delivered in the same workflow task as the start; wait for run() to
        // load the (possibly continued-as-new) state first.
        Workflow.await(() -> initialized);

        String from = state;
        long now = Workflow.currentTimeMillis();
        handledThisRun++;
        Optional<String> next = spec().next(from, event.eventType());

        TransitionResult result;
        TraceRow trace;
        if (next.isEmpty()) {
            result = new TransitionResult(event.eventId(), from, from, false,
                    "no transition from " + from + " on " + event.eventType(), version);
            trace = new TraceRow(event.eventId(), now, TraceRow.Stage.REJECTED, workflowId(), null,
                    from + " x " + event.eventType());
        } else {
            String to = next.get();
            data = applyData(from, to, data, event);
            state = to;
            version++;
            result = new TransitionResult(event.eventId(), from, to, true, null, version);
            trace = new TraceRow(event.eventId(), now, TraceRow.Stage.TRANSITION, workflowId(), null,
                    from + " -> " + to + " (" + event.eventType() + ")");
            onTransition(from, to, event);
        }
        remember(new TransitionRecord(event.eventId(), event.eventType(), from, result.toState(), result.accepted(), now));

        // State is already updated; the projection write may yield without affecting ordering.
        persistence.persistState(
                new SmStateRow(workflowId(), smType, state, data, version, event.eventId(), now), trace);
        return result;
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
