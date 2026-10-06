package io.concert.sdk.reconcile;

import io.concert.common.TaskQueues;
import io.temporal.api.enums.v1.ScheduleOverlapPolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.schedules.Schedule;
import io.temporal.client.schedules.ScheduleActionStartWorkflow;
import io.temporal.client.schedules.ScheduleAlreadyRunningException;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.client.schedules.ScheduleClientOptions;
import io.temporal.client.schedules.ScheduleIntervalSpec;
import io.temporal.client.schedules.ScheduleOptions;
import io.temporal.client.schedules.SchedulePolicy;
import io.temporal.client.schedules.ScheduleSpec;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Temporal Schedule {@code reconcile-<smType>} that starts {@link ReconcileWorkflow} periodically. */
public final class ReconcileSchedules {
    private static final Logger log = LoggerFactory.getLogger(ReconcileSchedules.class);

    private ReconcileSchedules() {}

    public static String scheduleId(String smType) {
        return "reconcile-" + smType;
    }

    /** Workflow id of the scheduled runs (Temporal appends the scheduled time). */
    public static String workflowId(String smType) {
        return "reconcile-" + smType;
    }

    /**
     * Creates the schedule if it does not exist yet (every replica of the worker calls this; the first
     * wins). Overlapping runs are skipped. Failures are logged, not thrown: a worker must start without it.
     */
    public static void ensure(WorkflowClient client, String smType, Duration every) {
        try {
            ScheduleClient schedules = ScheduleClient.newInstance(client.getWorkflowServiceStubs(),
                    ScheduleClientOptions.newBuilder().setNamespace(client.getOptions().getNamespace()).build());
            Schedule schedule = Schedule.newBuilder()
                    .setAction(ScheduleActionStartWorkflow.newBuilder()
                            .setWorkflowType(ReconcileWorkflow.class)
                            .setArguments(new ReconcileRequest(smType, false, ReconcileRequest.DEFAULT_CHUNK))
                            .setOptions(WorkflowOptions.newBuilder()
                                    .setWorkflowId(workflowId(smType))
                                    .setTaskQueue(TaskQueues.stateMachine(smType))
                                    .setWorkflowExecutionTimeout(Duration.ofHours(2))
                                    .build())
                            .build())
                    .setSpec(ScheduleSpec.newBuilder().setIntervals(List.of(new ScheduleIntervalSpec(every))).build())
                    .setPolicy(SchedulePolicy.newBuilder().setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP).build())
                    .build();
            schedules.createSchedule(scheduleId(smType), schedule, ScheduleOptions.newBuilder().build());
            log.info("created schedule {} (every {})", scheduleId(smType), every);
        } catch (ScheduleAlreadyRunningException e) {
            log.debug("schedule {} exists", scheduleId(smType));
        } catch (RuntimeException e) {
            log.warn("could not create schedule {}: {}", scheduleId(smType), e.toString());
        }
    }
}
