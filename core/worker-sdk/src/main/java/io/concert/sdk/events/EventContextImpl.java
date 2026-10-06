package io.concert.sdk.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.api.SmStateRow;
import io.concert.model.runtime.ModelJson;
import io.concert.sdk.LockTemplates;
import io.concert.store.StateStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** {@link EventContext} of one handler attempt; collects writes and emits for the processor. */
final class EventContextImpl implements EventContext {

    /** A loaded state document: its version and whether this event already wrote it. */
    private record Loaded(long version, boolean appliedByThisEvent) {}

    private final StateStore store;
    private final EventEnvelope envelope;
    private final Object payload;
    private final int attempt;
    private final String domain;
    private final Set<String> lockKeys;
    private final Map<String, Loaded> loaded = new LinkedHashMap<>();
    private final Map<Object, String> keyOf = new IdentityHashMap<>();
    private final Map<String, StateWrite> writes = new LinkedHashMap<>();
    private final List<EventEnvelope> emitted = new ArrayList<>();

    EventContextImpl(StateStore store, EventEnvelope envelope, Object payload, int attempt, String domain) {
        this.store = store;
        this.envelope = envelope;
        this.payload = payload;
        this.attempt = attempt;
        this.domain = domain;
        this.lockKeys = Set.copyOf(envelope.effectiveLockKeys());
    }

    @Override
    public Object event() {
        return payload;
    }

    @Override
    public EventEnvelope envelope() {
        return envelope;
    }

    @Override
    public int attempt() {
        return attempt;
    }

    @Override
    public <S> Optional<S> state(Class<S> type, String key) {
        checkKey(key);
        Optional<SmStateRow> row = load(key);
        if (row.isEmpty() || row.get().data() == null) {
            return Optional.empty();
        }
        S value = ModelJson.read(row.get().data(), type);
        keyOf.put(value, key);
        return Optional.of(value);
    }

    @Override
    public void save(Object state) {
        String key = keyOf.get(state);
        if (key == null) {
            EventCatalog.StateType st = EventCatalogs.state(state.getClass()).orElse(null);
            if (st == null || st.keyTemplate() == null) {
                throw new BlockingError("cannot derive the state key of " + state.getClass().getSimpleName()
                        + ": load it with ctx.state(type, key), pass the key to save, or declare a StateType key template");
            }
            JsonNode json = Json.read(ModelJson.write(state), JsonNode.class);
            key = LockTemplates.render(List.of(st.keyTemplate()), envelope.eventId(), json).getFirst();
        }
        save(key, state);
    }

    @Override
    public void save(String key, Object state) {
        checkKey(key);
        Loaded l = loaded.get(key);
        if (l == null) {
            load(key);
            l = loaded.get(key);
        }
        keyOf.put(state, key);
        if (l.appliedByThisEvent()) {
            return; // a re-run after this event's writes were persisted: never apply them twice
        }
        String type = EventCatalogs.state(state.getClass()).map(EventCatalog.StateType::name)
                .orElse(state.getClass().getSimpleName());
        writes.put(key, new StateWrite(key, type, ModelJson.write(state), l.version() + 1));
    }

    @Override
    public String emit(Object event) {
        return add(EventEmissions.event(envelope, emitted.size() + 1, event, domain, 0, System.currentTimeMillis()));
    }

    @Override
    public String emitAt(Instant at, Object event) {
        return add(EventEmissions.event(envelope, emitted.size() + 1, event, domain, at.toEpochMilli(),
                System.currentTimeMillis()));
    }

    @Override
    public String emitEntity(String smType, String instanceKey, String eventType, Object payload, List<String> extraLockKeys) {
        return add(EventEmissions.entity(envelope, emitted.size() + 1, smType, instanceKey, eventType, payload,
                extraLockKeys, System.currentTimeMillis()));
    }

    List<StateWrite> writes() {
        return List.copyOf(writes.values());
    }

    List<EventEnvelope> emitted() {
        return List.copyOf(emitted);
    }

    private String add(EventEnvelope child) {
        emitted.add(child);
        return child.eventId();
    }

    private Optional<SmStateRow> load(String key) {
        Optional<SmStateRow> row = store.loadState(EventRows.stateRowId(key));
        loaded.putIfAbsent(key, new Loaded(row.map(SmStateRow::version).orElse(0L),
                row.map(r -> envelope.eventId().equals(r.lastEventId())).orElse(false)));
        return row;
    }

    private void checkKey(String key) {
        if (!lockKeys.contains(key)) {
            throw new BlockingError("state key " + key + " is not one of the event's lock keys " + envelope.effectiveLockKeys());
        }
    }
}
