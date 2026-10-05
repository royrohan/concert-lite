package io.concert.common.api;

import io.concert.common.TraceRow;
import io.temporal.activity.ActivityInterface;

/** Local activity used by entity workflows to project state into DSQL. */
@ActivityInterface
public interface EntityPersistenceActivities {

    void persistState(SmStateRow row, TraceRow trace);
}
