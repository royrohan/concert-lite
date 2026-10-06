package io.concert.sdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.concert.common.EventEnvelope;
import io.concert.common.api.TransitionResult;
import io.concert.model.runtime.ModelJson;
import io.concert.model.runtime.ModelObject;
import io.concert.model.runtime.ModelValidationException;
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
import java.util.List;

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

    /** Snapshot publishes started by this run; it may not continue-as-new before they complete. */
    private final List<Promise<Void>> pendingSnapshots = new ArrayList<>();

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

    @Override
    protected final Decision decide(String fromState, String toState, String currentData, EventEnvelope event) {
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
