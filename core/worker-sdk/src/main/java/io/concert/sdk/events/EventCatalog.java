package io.concert.sdk.events;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Event-style types a module provides: for each event type its payload class, domain (handler application),
 * lock key templates, error policy and retries, plus keyed-state classes and their key template. Generated
 * from Pure models annotated with {@code concert::event} ({@code ./generate-concert-ecosystem}: one catalog per
 * domain, discovered with {@link java.util.ServiceLoader} through
 * {@code META-INF/services/io.concert.sdk.events.EventCatalog}, which is also how the trace UI finds them), or
 * built by hand with {@link #builder} and registered with {@link EventCatalogs#register}.
 *
 * <p>Templates use {@link io.concert.sdk.LockTemplates} syntax: literal text with {@code {field}} placeholders
 * for top-level fields of the payload (or state) JSON; {@code {id}} is the event id.
 */
public interface EventCatalog {

    /**
     * One event type.
     *
     * @param name the {@code eventType} on the wire (default: the class's simple name)
     * @param domain the handler application that applies it (task queue {@code ev-<domain>})
     * @param lockTemplates lock key templates, e.g. {@code order:{orderId}}; none means {@code <domain>:<eventId>}
     * @param onError what happens when the handler's retries are exhausted
     * @param maxAttempts handler attempts (local activity retries) before {@code onError} applies; at least 1
     */
    record EventType(String name, String domain, Class<?> payloadType, List<String> lockTemplates, OnError onError,
            int maxAttempts) {

        public static final int DEFAULT_ATTEMPTS = 3;

        public EventType {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(payloadType, "payloadType");
            lockTemplates = lockTemplates == null ? List.of() : List.copyOf(lockTemplates);
            onError = onError == null ? OnError.BLOCKING : onError;
            maxAttempts = Math.max(1, maxAttempts);
        }

        /** {@code BLOCKING}, {@value #DEFAULT_ATTEMPTS} attempts, named after the class. */
        public static EventType of(String domain, Class<?> payloadType, String... lockTemplates) {
            return new EventType(payloadType.getSimpleName(), domain, payloadType, Arrays.asList(lockTemplates),
                    OnError.BLOCKING, DEFAULT_ATTEMPTS);
        }

        public EventType named(String newName) {
            return new EventType(newName, domain, payloadType, lockTemplates, onError, maxAttempts);
        }

        public EventType onError(OnError policy) {
            return new EventType(name, domain, payloadType, lockTemplates, policy, maxAttempts);
        }

        public EventType nonBlocking() {
            return onError(OnError.NON_BLOCKING);
        }

        public EventType retries(int attempts) {
            return new EventType(name, domain, payloadType, lockTemplates, onError, attempts);
        }
    }

    /**
     * A keyed-state class: documents {@code state:<key>} in the StateStore.
     *
     * @param name the state type name (snapshots use smType {@code st_<snake name>}); default the simple name
     * @param keyTemplate renders the key from the state's own JSON, e.g. {@code order:{orderId}}; {@code null}:
     *     handlers pass the key to {@link EventContext#save(String, Object)}
     */
    record StateType(String name, Class<?> type, String keyTemplate) {

        public StateType {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }

        public static StateType of(Class<?> type, String keyTemplate) {
            return new StateType(type.getSimpleName(), type, keyTemplate);
        }
    }

    /** Name of the catalog, e.g. the module or model name. */
    String name();

    List<EventType> events();

    List<StateType> states();

    /**
     * Event types each type's handler may emit, by event type name ({@code concert::event.emits}); documentation
     * for flow diagrams, not enforced at runtime.
     */
    default Map<String, List<String>> emits() {
        return Map.of();
    }

    /** Mermaid {@code flowchart} of the event flow (event types, their domains and emits), or {@code null}. */
    default String flowMermaid() {
        return null;
    }

    /** Mermaid {@code classDiagram} of the payload and state models, or {@code null}. */
    default String modelMermaid() {
        return null;
    }

    static Builder builder(String name) {
        return new Builder(name);
    }

    /** Builds an immutable catalog. */
    final class Builder {
        private final String name;
        private final List<EventType> events = new ArrayList<>();
        private final List<StateType> states = new ArrayList<>();

        private Builder(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        public Builder event(EventType type) {
            events.add(type);
            return this;
        }

        /** Shorthand for {@code event(EventType.of(domain, payloadType, lockTemplates))}. */
        public Builder event(String domain, Class<?> payloadType, String... lockTemplates) {
            return event(EventType.of(domain, payloadType, lockTemplates));
        }

        public Builder state(StateType type) {
            states.add(type);
            return this;
        }

        public Builder state(Class<?> type, String keyTemplate) {
            return state(StateType.of(type, keyTemplate));
        }

        public EventCatalog build() {
            return new Simple(name, List.copyOf(events), List.copyOf(states));
        }
    }

    /** The catalog {@link Builder#build()} returns. */
    record Simple(String name, List<EventType> events, List<StateType> states) implements EventCatalog {}
}
