package io.concert.sdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.concert.common.EventEnvelope;
import io.concert.common.api.TransitionResult;
import io.concert.model.runtime.ModelJson;
import io.concert.model.runtime.ModelObject;
import io.concert.model.runtime.ModelValidationException;
import io.concert.sdk.events.EnqueueRequest;
import io.concert.sdk.events.EventCatalog;
import io.concert.sdk.events.EventEmissions;
import io.concert.sdk.events.EventEnqueueActivities;
import io.concert.sink.EntitySnapshot;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A state machine whose entity data is a model object generated from a Pure model (see
 * {@code @LegendModel}) and whose event payloads are typed by the spec
 * ({@link StateMachineSpec.Builder#on(String, String, String, Class)}).
 *
 * <p>For each event with a transition {@code from -> to} the base class, inside the {@code handle}
 * update:
 *
 * <ol>
 *   <li>loads a fresh copy of the data: {@link #initialData} for a new entity, otherwise the stored
 *       JSON read with {@link ModelJson},
 *   <li>binds the event payload to the transition's payload type (an event without a payload, or a
 *       transition without a type, gives {@code null}); JSON that does not bind rejects the event,
 *   <li>lets {@link #completePayload} fill in defaults, then rejects the event if the payload
 *       violates its model's multiplicities (unless {@link #validatePayloads()} is off),
 *   <li>calls {@link #onTransition(String, String, ModelObject, Object, EventEnvelope)}, which mutates
 *       the data in place and may call activities, or rejects the event with {@link #reject},
 *   <li>rejects the event if the data now violates its model's multiplicities, otherwise commits:
 *       the new state, the data as {@link ModelJson} JSON and the next version are persisted.
 * </ol>
 *
 * A rejected event leaves state, data and version unchanged ({@code TransitionResult.accepted} is
 * false, its message and the {@code REJECTED} trace list the reasons). Activities already called by
 * {@code onTransition} are not undone. Everything here is deterministic ({@link ModelJson} output is),
 * so it is safe in workflow code; take time from {@code Workflow.currentTimeMillis()}.
 *
 * <p><b>Completed-entity snapshots.</b> After an accepted transition into a terminal state has been
 * persisted, the entity's final data is published through the {@link EntitySnapshotPublisher}
 * activity (Kafka topic {@code entity-snapshots}, consumed by the analytics sinks). It is a regular
 * activity on the entity's own task queue with unlimited retries, so the workflow history acts as the
 * outbox. It is started asynchronously (the update completes without waiting for Kafka) and the run
 * waits for it before continuing as new.
 *
 * <p><b>Emitting event-style events (interop).</b> {@code onTransition} may call {@link #emit},
 * {@link #emitAt} or {@link #emitEntity}. The events are buffered and, only if the transition is accepted,
 * handed to the {@link EventEnqueueActivities} activity after the state row is persisted (regular activity,
 * so the hand-off is recorded in history; the run waits for it before continuing as new). Their ids are
 * {@code <eventId>.<n>}, so replays and retries cannot duplicate them.
 *
 * @param <D> the generated class of the aggregate root
 */
public abstract class ModelStateMachine<D extends ModelObject> extends AbstractStateMachine {

    /** {@code Workflow.getVersion} change id of the snapshot publish, so runs started before it replay. */
    static final String SNAPSHOT_CHANGE_ID = "entity-snapshot-publish";

    /**
     * Retries forever with backoff 1 s .. 60 s: a snapshot must eventually reach Kafka, however long it
     * is down. 30 s start-to-close stays above the producer's own send timeout.
     */
    private final EntitySnapshotPublisher snapshots = Workflow.newActivityStub(EntitySnapshotPublisher.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2)
                            .setMaximumInterval(Duration.ofSeconds(60))
                            .setMaximumAttempts(0)
                            .build())
                    .build());

    /** Hands emitted events to their lock chains / processors; unlimited retries (never lost). */
    private final EventEnqueueActivities emitter = Workflow.newActivityStub(EventEnqueueActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofMillis(100))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setMaximumAttempts(0)
                            .build())
                    .build());

    /** Snapshot publishes and emits started by this run; it may not continue-as-new before they complete. */
    private final List<Promise<Void>> pendingSnapshots = new ArrayList<>();

    /** The event being decided (emits are only allowed then) and what it emitted so far. */
    private EventEnvelope emitParent;
    private List<EventEnvelope> emitBuffer;
    /** Emits of accepted events, waiting for their state row to be persisted. */
    private final Map<String, List<EventEnvelope>> acceptedEmits = new HashMap<>();

    /** Thrown by {@link #reject} to reject the current event from {@code onTransition}. */
    public static final class TransitionRejectedException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final List<String> reasons;

        public TransitionRejectedException(List<String> reasons) {
            super(String.join("; ", reasons));
            this.reasons = List.copyOf(reasons);
        }

        public List<String> reasons() {
            return reasons;
        }
    }

    /** The generated class of the aggregate root. */
    protected abstract Class<D> dataType();

    /** The data of a new entity, before its first transition. */
    protected abstract D initialData(String instanceKey);

    /**
     * Applies an accepted transition: mutate {@code data} in place and call activities for side
     * effects. Throw via {@link #reject} (or a {@link ModelValidationException}) to reject the event.
     *
     * @param payload the event payload as the spec's payload type for this transition, after
     *     {@link #completePayload}; {@code null} if the event has none or the transition is untyped
     */
    protected void onTransition(String fromState, String toState, D data, Object payload, EventEnvelope event) {}

    /**
     * Fills in defaults for a missing payload ({@code null}) or missing payload fields before the
     * payload is validated. Default: returns {@code payload} unchanged.
     */
    protected Object completePayload(String fromState, String eventType, Object payload, D data) {
        return payload;
    }

    /** Whether payloads that are model objects must satisfy their multiplicities. Default: true. */
    protected boolean validatePayloads() {
        return true;
    }

    /**
     * Called once an accepted transition into a terminal state (no outgoing transitions, see
     * {@link StateMachineSpec#isTerminal}) has been persisted. Default: nothing.
     */
    protected void onTerminal(String state, D data, EventEnvelope event) {}

    /** Rejects the current event; call from {@code onTransition}. */
    protected static void reject(String... reasons) {
        throw new TransitionRejectedException(List.of(reasons));
    }

    /** The current data as a fresh, mutable copy. */
    protected final D readData() {
        String json = currentData();
        return json == null ? initialData(instanceKey()) : ModelJson.read(json, dataType());
    }

    /**
     * Emits an event-style event, due now; type, domain and lock keys come from its class's
     * {@link EventCatalog} entry. Call from {@code onTransition}; applied only if the transition is accepted.
     *
     * @return the child event id, {@code <eventId>.<n>}
     */
    protected final String emit(Object event) {
        return emitAt(null, event);
    }

    /** Like {@link #emit}, applied no earlier than {@code at} ({@code null}: now). */
    protected final String emitAt(Instant at, Object event) {
        EventEnvelope parent = requireEmitParent();
        return addEmit(EventEmissions.event(parent, emitBuffer.size() + 1, event, null,
                at == null ? 0 : at.toEpochMilli(), Workflow.currentTimeMillis()));
    }

    /** Emits an entity-style event to {@code smType:instanceKey} (another state machine). */
    protected final String emitEntity(String smType, String instanceKey, String eventType, Object payload,
            List<String> extraLockKeys) {
        EventEnvelope parent = requireEmitParent();
        return addEmit(EventEmissions.entity(parent, emitBuffer.size() + 1, smType, instanceKey, eventType, payload,
                extraLockKeys, Workflow.currentTimeMillis()));
    }

    private EventEnvelope requireEmitParent() {
        if (emitParent == null) {
            throw new IllegalStateException("emit is only allowed from onTransition");
        }
        return emitParent;
    }

    private String addEmit(EventEnvelope child) {
        emitBuffer.add(child);
        return child.eventId();
    }

    @Override
    protected final Decision decide(String fromState, String toState, String currentData, EventEnvelope event) {
        emitParent = event;
        emitBuffer = new ArrayList<>();
        try {
            Decision d = decideTyped(fromState, toState, event);
            if (d.accepted() && !emitBuffer.isEmpty()) {
                acceptedEmits.put(event.eventId(), List.copyOf(emitBuffer));
            }
            return d;
        } finally {
            emitParent = null;
            emitBuffer = null;
        }
    }

    private Decision decideTyped(String fromState, String toState, EventEnvelope event) {
        D data = readData();
        Class<?> payloadType = spec().payloadType(fromState, event.eventType()).orElse(null);
        Object payload = null;
        if (payloadType != null && event.payload() != null) {
            try {
                payload = ModelJson.read(event.payload(), payloadType);
            } catch (UncheckedIOException e) {
                String detail = e.getCause() instanceof JsonProcessingException j ? j.getOriginalMessage() : e.getMessage();
                return Decision.reject("invalid " + payloadType.getSimpleName() + " payload: " + detail);
            }
        }
        payload = completePayload(fromState, event.eventType(), payload, data);
        if (validatePayloads() && payload instanceof ModelObject m) {
            List<String> errors = m.validationErrors();
            if (!errors.isEmpty()) {
                return Decision.reject("invalid payload: " + String.join("; ", errors));
            }
        }
        try {
            onTransition(fromState, toState, data, payload, event);
        } catch (TransitionRejectedException e) {
            return Decision.reject(String.join("; ", e.reasons()));
        } catch (ModelValidationException e) {
            return Decision.reject("invalid data: " + String.join("; ", e.errors()));
        }
        List<String> errors = data.validationErrors();
        if (!errors.isEmpty()) {
            return Decision.reject("invalid data: " + String.join("; ", errors));
        }
        return Decision.accept(ModelJson.write(data));
    }

    @Override
    protected void onPersisted(TransitionResult result, EventEnvelope event) {
        List<EventEnvelope> emits = acceptedEmits.remove(event.eventId());
        if (emits != null && result.accepted()) {
            pendingSnapshots.add(Async.procedure(emitter::enqueue,
                    EnqueueRequest.children(event.eventId(), emits, Workflow.currentTimeMillis())));
        }
        if (result.accepted() && spec().isTerminal(result.toState())) {
            publishSnapshot(result);
            onTerminal(result.toState(), readData(), event);
        }
    }

    /**
     * Starts the snapshot publish of a terminal transition without waiting for it. A terminal state
     * has no outgoing transitions, so state, data and version are final: this is the entity's
     * current (and last) version, which matters because the compacted topic keeps only the latest
     * record per key.
     */
    private void publishSnapshot(TransitionResult result) {
        if (Workflow.getVersion(SNAPSHOT_CHANGE_ID, Workflow.DEFAULT_VERSION, 1) == Workflow.DEFAULT_VERSION) {
            return; // the terminal transition happened before this code existed: replay it unchanged
        }
        long created = createdAtMillis();
        long completed = lastAcceptedMillis() > 0 ? lastAcceptedMillis() : Workflow.currentTimeMillis();
        EntitySnapshot snapshot = new EntitySnapshot(Workflow.getInfo().getWorkflowId(), smType(), result.toState(),
                result.version(), created > 0 ? Instant.ofEpochMilli(created) : null, Instant.ofEpochMilli(completed),
                currentData());
        pendingSnapshots.add(Async.procedure(snapshots::publish, snapshot));
    }

    @Override
    protected boolean hasPendingWork() {
        return pendingSnapshots.stream().anyMatch(p -> !p.isCompleted());
    }

    /** Typed machines use {@link #onTransition(String, String, ModelObject, Object, EventEnvelope)}. */
    @Override
    protected final void onTransition(String fromState, String toState, EventEnvelope event) {}

    /** Typed machines compute data in {@link #decide}; this is never called. */
    @Override
    protected final String applyData(String fromState, String toState, String currentData, EventEnvelope event) {
        return currentData;
    }
}
