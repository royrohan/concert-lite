package io.concert.sdk;

import io.concert.common.TraceRow;
import io.concert.common.api.CommitOutcome;
import io.concert.common.api.EntityLoad;
import io.concert.common.api.EntityPersistenceActivities;
import io.concert.common.api.TransitionCommit;
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

    @Override
    public EntityLoad loadEntity(String workflowId, String eventId) {
        return store.loadEntity(workflowId, eventId);
    }

    /**
     * The transition is the only synchronous write: one transaction per event. The trace row is best effort and goes
     * through the batching writer, outside that transaction.
     */
    @Override
    public CommitOutcome commitTransition(TransitionCommit commit, TraceRow trace) {
        CommitOutcome out = store.commitTransition(commit);
        if (trace != null && out.status() == CommitOutcome.Status.COMMITTED) {
            traces.add(trace);
        }
        return out;
    }
}
