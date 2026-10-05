package io.concert.sdk;

import io.concert.common.TraceRow;
import io.concert.common.api.EntityPersistenceActivities;
import io.concert.common.api.SmStateRow;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;

public final class EntityPersistenceActivitiesImpl implements EntityPersistenceActivities {

    private final StateStore store;
    private final TraceWriter traces;

    public EntityPersistenceActivitiesImpl(StateStore store) {
        this(store, new TraceWriter(store));
    }

    public EntityPersistenceActivitiesImpl(StateStore store, TraceWriter traces) {
        this.store = store;
        this.traces = traces;
    }

    /**
     * The state upsert is the only synchronous write: one autocommit statement per event. The trace
     * row is best effort and goes through the batching writer instead of the state transaction.
     */
    @Override
    public void persistState(SmStateRow row, TraceRow trace) {
        store.upsertState(row, null);
        if (trace != null) {
            traces.add(trace);
        }
    }
}
