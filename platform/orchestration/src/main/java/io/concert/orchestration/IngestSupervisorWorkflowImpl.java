package io.concert.orchestration;

import io.concert.common.api.IngestConfig;
import io.concert.common.api.IngestStatus;
import io.concert.common.api.IngestSupervisorWorkflow;
import io.concert.common.api.ShardConsumerActivities;
import io.concert.common.api.ShardInfo;
import io.concert.common.api.ShardResult;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;

/**
 * Keeps exactly one consumer activity per open shard (this replaces KCL and its lease table).
 * Child shards start only after their parents are fully consumed, preserving per-key order across
 * resharding. Before continue-as-new it cancels and drains its consumers, so two consumers never
 * read the same shard at once.
 */
public class IngestSupervisorWorkflowImpl implements IngestSupervisorWorkflow {

    static final int ITERATIONS_BEFORE_CONTINUE = 200;

    private static final Logger log = Workflow.getLogger(IngestSupervisorWorkflowImpl.class);

    private final ShardConsumerActivities lister = Workflow.newActivityStub(
            ShardConsumerActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumInterval(Duration.ofSeconds(10)).build())
                    .build());

    private final Map<String, Running> active = new LinkedHashMap<>();
    private final Set<String> consumed = new LinkedHashSet<>();
    private String stream;
    private long restarts;

    private record Running(CancellationScope scope, Promise<ShardResult> result) {}

    @Override
    public void run(IngestConfig config) {
        stream = config.stream();
        if (config.consumedShards() != null) {
            consumed.addAll(config.consumedShards());
        }
        ShardConsumerActivities consumer = Workflow.newActivityStub(
                ShardConsumerActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(config.consumeRunMinutes() + 5L))
                        .setHeartbeatTimeout(Duration.ofSeconds(config.heartbeatTimeoutSec() > 0 ? config.heartbeatTimeoutSec() : 3))
                        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
                        .setRetryOptions(RetryOptions.newBuilder()
                                .setInitialInterval(Duration.ofSeconds(1))
                                .setMaximumInterval(Duration.ofSeconds(30))
                                .build())
                        .build());

        for (int iteration = 0; iteration < ITERATIONS_BEFORE_CONTINUE; iteration++) {
            List<ShardInfo> shards = lister.listShards(stream);
            Set<String> known = new LinkedHashSet<>();
            shards.forEach(s -> known.add(s.shardId()));
            for (ShardInfo s : shards) {
                if (!consumed.contains(s.shardId()) && !active.containsKey(s.shardId()) && parentsDone(s, known)) {
                    start(consumer, config, s.shardId());
                }
            }

            Workflow.await(Duration.ofSeconds(config.shardListIntervalSec()),
                    () -> active.values().stream().anyMatch(r -> r.result().isCompleted()));
            reapCompleted();
            if (Workflow.getInfo().isContinueAsNewSuggested()) {
                break;
            }
        }

        // Drain consumers before handing over to the next run.
        active.values().forEach(r -> r.scope().cancel("continue-as-new"));
        Workflow.await(() -> active.values().stream().allMatch(r -> r.result().isCompleted()));
        reapCompleted();
        Workflow.continueAsNew(config.withConsumed(List.copyOf(consumed)));
    }

    @Override
    public IngestStatus status() {
        return new IngestStatus(stream, List.copyOf(active.keySet()), List.copyOf(consumed), restarts);
    }

    private void start(ShardConsumerActivities consumer, IngestConfig config, String shardId) {
        List<Promise<ShardResult>> holder = new ArrayList<>(1);
        CancellationScope scope = Workflow.newCancellationScope(() -> holder.add(Async.function(
                consumer::consumeShard, stream, shardId, config.initialPosition(), config.consumeRunMinutes())));
        scope.run();
        active.put(shardId, new Running(scope, holder.get(0)));
    }

    private void reapCompleted() {
        active.entrySet().removeIf(entry -> {
            Promise<ShardResult> p = entry.getValue().result();
            if (!p.isCompleted()) {
                return false;
            }
            if (p.getFailure() == null && p.get().closed()) {
                consumed.add(entry.getKey());
            } else {
                // Run budget used up, cancelled, or failed: restarted on the next iteration.
                restarts++;
                if (p.getFailure() != null) {
                    log.warn("consumer for {} ended: {}", entry.getKey(), p.getFailure().getMessage());
                }
            }
            return true;
        });
    }

    private boolean parentsDone(ShardInfo s, Set<String> known) {
        return parentDone(s.parentShardId(), known) && parentDone(s.adjacentParentShardId(), known);
    }

    private boolean parentDone(String parent, Set<String> known) {
        // A parent no longer listed has aged out of retention: nothing left to read.
        return parent == null || consumed.contains(parent) || !known.contains(parent);
    }
}
