package io.concert.it;

import io.concert.common.WorkerTuning;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Isolates raw Temporal hop costs on this machine: signal -> local activity vs. update round trip. */
@Tag("bench")
class TemporalHopBench {

    @ActivityInterface
    public interface Mark {
        void mark(long sentNanos);
    }

    @WorkflowInterface
    public interface Hop {
        @WorkflowMethod
        void run();

        @SignalMethod
        void ping(long sentNanos);

        @UpdateMethod
        long pong(long sentNanos);
    }

    public static class HopImpl implements Hop {
        private final Mark mark = Workflow.newLocalActivityStub(Mark.class,
                LocalActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(5)).build());
        private final Deque<Long> pings = new ArrayDeque<>();

        @Override
        public void run() {
            while (true) {
                Workflow.await(Duration.ofSeconds(60), () -> !pings.isEmpty());
                if (pings.isEmpty()) {
                    return;
                }
                mark.mark(pings.removeFirst());
            }
        }

        @Override
        public void ping(long sentNanos) {
            pings.addLast(sentNanos);
        }

        @Override
        public long pong(long sentNanos) {
            return sentNanos;
        }
    }

    static final Histogram SIGNAL = new Histogram(10_000_000_000L, 3);
    static final ConcurrentHashMap<Long, CountDownLatch> WAITING = new ConcurrentHashMap<>();

    @Test
    void measureHops() throws Exception {
        Stack.start();
        WorkflowClient client = Stack.newClient();
        WorkerFactory factory = WorkerFactory.newInstance(client, WorkerTuning.factoryOptions());
        Worker w = factory.newWorker("hop", WorkerTuning.workerOptions(64, 4).build());
        w.registerWorkflowImplementationTypes(HopImpl.class);
        w.registerActivitiesImplementations((Mark) sent -> {
            synchronized (SIGNAL) {
                SIGNAL.recordValue((System.nanoTime() - sent) / 1000);
            }
            WAITING.get(sent).countDown();
        });
        factory.start();

        Histogram update = new Histogram(10_000_000_000L, 3);
        for (int round = 0; round < 2; round++) { // round 0 = warm-up
            SIGNAL.reset();
            update.reset();
            for (int i = 0; i < 300; i++) {
                String id = "hop-" + (i % 30);
                WorkflowStub stub = client.newUntypedWorkflowStub("Hop", WorkflowOptions.newBuilder()
                        .setWorkflowId(id).setTaskQueue("hop").build());
                long sent = System.nanoTime();
                CountDownLatch latch = new CountDownLatch(1);
                WAITING.put(sent, latch);
                stub.signalWithStart("ping", new Object[] {sent}, new Object[] {});
                latch.await(10, TimeUnit.SECONDS);

                Hop typed = client.newWorkflowStub(Hop.class, WorkflowOptions.newBuilder()
                        .setWorkflowId(id).setTaskQueue("hop")
                        .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING).build());
                long t0 = System.nanoTime();
                WorkflowClient.executeUpdateWithStart(typed::pong, t0,
                        UpdateOptions.<Long>newBuilder().setWaitForStage(WorkflowUpdateStage.COMPLETED).build(),
                        new WithStartWorkflowOperation<>(typed::run));
                update.recordValue((System.nanoTime() - t0) / 1000);
                Thread.sleep(10);
            }
        }
        System.out.printf("[hop] signalWithStart -> local activity: p50=%.1fms p99=%.1fms | updateWithStart round trip: p50=%.1fms p99=%.1fms%n",
                SIGNAL.getValueAtPercentile(50) / 1000.0, SIGNAL.getValueAtPercentile(99) / 1000.0,
                update.getValueAtPercentile(50) / 1000.0, update.getValueAtPercentile(99) / 1000.0);
        factory.shutdownNow();
    }
}
