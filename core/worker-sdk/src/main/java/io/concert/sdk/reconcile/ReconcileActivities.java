package io.concert.sdk.reconcile;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ReconcileActivities {

    @ActivityMethod(name = "ReconcileSmType")
    ReconcileReport reconcile(ReconcileRequest request);
}
