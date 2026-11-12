package io.concert.sdk;

import io.concert.common.EventEnvelope;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.events.EnqueueRequest;
import io.concert.sdk.events.EventEnqueueActivities;
import io.concert.store.InMemoryStateStore;
import io.concert.store.StateStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies events to machines exactly as the worker does ({@link TransitionActivitiesImpl}: load, decide, one
 * transaction), without Temporal or locks: for tests. Emitted events are captured in order; {@link #deliverEmitted()}
 * applies them in turn, as their lock chains would.
 */
public final class MachineHarness {
    private final StateStore store;
    private final Map<String, Class<? extends AbstractStateMachine>> machines = new HashMap<>();
    private final List<EventEnvelope> emitted = new ArrayList<>();
    private final List<EntitySnapshotPublisher> publishers = new ArrayList<>();
    private TransitionActivitiesImpl transitions;

    public MachineHarness() {
        this(new InMemoryStateStore());
    }

    public MachineHarness(StateStore store) {
        this.store = store;
    }

    public MachineHarness register(String smType, Class<? extends AbstractStateMachine> impl) {
        machines.put(smType, impl);
        transitions = null;
        return this;
    }

    /** Completed-entity snapshots go here (default: logged only). */
    public MachineHarness snapshots(EntitySnapshotPublisher publisher) {
        publishers.add(publisher);
        transitions = null;
        return this;
    }

    public StateStore store() {
        return store;
    }

    /** Applies one event; a redelivery of the same event id returns its stored result. */
    public TransitionResult apply(EventEnvelope event) {
        if (transitions == null) {
            EntitySnapshotPublisher pub = publishers.isEmpty() ? null : s -> publishers.forEach(p -> p.publish(s));
            transitions = new TransitionActivitiesImpl(store, machines, new Capture(), pub, null);
        }
        return transitions.applyTransition(event, Map.of());
    }

    /** Events emitted by committed transitions and not yet delivered, in emission order. */
    public List<EventEnvelope> emitted() {
        return List.copyOf(emitted);
    }

    /** Applies every captured emitted event (and what those emit, in turn); returns their results in order. */
    public List<TransitionResult> deliverEmitted() {
        List<TransitionResult> out = new ArrayList<>();
        while (!emitted.isEmpty()) {
            EventEnvelope e = emitted.removeFirst();
            if (!e.isEventStyle() && machines.containsKey(e.smType())) {
                out.add(apply(e));
            }
        }
        return out;
    }

    private final class Capture implements EventEnqueueActivities {
        @Override
        public void enqueue(EnqueueRequest request) {}

        @Override
        public void deliverOutbox(List<EventEnvelope> events, String emittedBy) {
            emitted.addAll(events);
            store.markOutboxSent(events.stream().map(EventEnvelope::eventId).toList());
        }

        @Override
        public void unblock(String lockKey, String requestId) {}
    }
}
