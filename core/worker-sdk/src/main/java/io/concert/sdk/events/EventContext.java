package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * What a handler can see and do. Reads happen immediately; {@link #save} and the {@code emit} calls are
 * buffered and applied by the processor once the handler returned successfully: state documents are written
 * (version-guarded, idempotent per event), then the lifecycle row goes DONE and the emitted events are
 * enqueued, durably and exactly once (deterministic ids {@code <eventId>.<n>}). A failed attempt applies
 * nothing.
 */
public interface EventContext {

    /** The event payload (the handler's event class). */
    Object event();

    /** The event as received: id, type, lock keys, causation. */
    EventEnvelope envelope();

    /** Handler attempt, starting at 1 (local activity retries). */
    int attempt();

    /**
     * Loads the keyed state document {@code state:<key>}. The key must be one of the event's lock keys (that
     * is what makes the read-modify-write safe); anything else is a {@link BlockingError}.
     *
     * @return empty if no document exists yet
     */
    <S> Optional<S> state(Class<S> type, String key);

    /**
     * Saves a state object under the key it was loaded with ({@link #state}), else the key rendered from its
     * class's {@link EventCatalog.StateType#keyTemplate()}.
     */
    void save(Object state);

    /** Saves a state object under {@code key} (one of the event's lock keys). */
    void save(String key, Object state);

    /**
     * Emits an event-style event, due now. Its type, domain and lock keys come from the catalog entry of its
     * class (a class without one goes to this event's domain under its simple name, with the default key).
     *
     * @return the child event id, {@code <eventId>.<n>}
     */
    String emit(Object event);

    /** Like {@link #emit} but applied no earlier than {@code at} (SCHEDULED until then, holding no locks). */
    String emitAt(Instant at, Object event);

    /**
     * Emits an entity-style event to state machine {@code smType:instanceKey} (interop with the spec-driven
     * style), serialized against the entity key plus {@code extraLockKeys}.
     *
     * @param payload a model object / POJO (serialized as JSON), a JSON string, or {@code null}
     */
    String emitEntity(String smType, String instanceKey, String eventType, Object payload, List<String> extraLockKeys);
}
