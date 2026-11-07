package io.concert.orchestration;

import io.concert.common.Env;
import io.concert.common.Failover;
import io.concert.common.Flows;
import io.concert.common.TaskQueues;
import io.concert.common.TemporalClients;
import io.concert.common.api.IngestConfig;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Environment:
 * <pre>
 * TEMPORAL_ADDRESS=localhost:7233   TEMPORAL_NAMESPACE=default
 * KINESIS_ENDPOINT=http://localhost:4566   INGEST_STREAM=concert-<flow>   INGEST_POSITION=TRIM_HORIZON
 * STORE_JDBC_URL=jdbc:postgresql://localhost:5433/concert   (STORE_KIND=dsql for Aurora DSQL)
 * DIRECT_SM_TYPES=            comma list of types that may bypass locks (no shared keys!)
 * DEDUPE_TTL_HOURS=24
 * INGEST_CREATE_STREAM=      create the stream if missing (default: true under CONCERT_FLOW), KINESIS_SHARDS=2
 * CONCERT_FLOW=              the flow (ecosystem) this coordinator serves: stream concert-<flow>, task queue
 *                             orchestration-<flow>, lock ids lock:<flow>:<key> (io.concert.common.Flows)
 * </pre>
 */
public final class OrchestrationWorkerMain {
    private static final Logger log = LoggerFactory.getLogger(OrchestrationWorkerMain.class);

    public static void main(String[] args) throws InterruptedException {
        StateStore store = Stores.fromEnv();
        Set<String> direct = Arrays.stream(Env.get("DIRECT_SM_TYPES", "").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
        OrchestrationWorker.Settings settings = new OrchestrationWorker.Settings(
                direct,
                Env.getInt("INGEST_PARALLEL_GROUPS", 256),
                Env.getLong("INGEST_IDLE_POLL_MS", 20),
                Env.getLong("INGEST_CHECKPOINT_MS", 500),
                Env.getInt("ORCH_WF_POLLERS_MAX", 64),
                Env.getInt("ORCH_ACTIVITY_POLLERS_MAX", 8));
        String stream = Flows.stream();
        var kinesis = KinesisClients.fromEnv();
        if (Env.getBool("INGEST_CREATE_STREAM", !Flows.CURRENT.isEmpty())) {
            // a flow's stream is the flow's own: its coordinator creates it if Start flow (trace UI) did not
            if (KinesisStreams.ensure(kinesis, stream, Env.getInt("KINESIS_SHARDS", 2), 120_000)) {
                log.info("created stream {}", stream);
            }
        }
        OrchestrationWorker worker = new OrchestrationWorker(TemporalClients.fromEnv(), store, kinesis, settings).start();

        if (Env.getBool("INGEST_AUTOSTART", true)) {
            worker.ensureIngest(new IngestConfig(stream, Env.get("INGEST_POSITION", "TRIM_HORIZON"),
                    Env.getInt("INGEST_SHARD_LIST_SEC", 30), Env.getInt("INGEST_RUN_MINUTES", 30), List.of(),
                    Failover.shardHeartbeatTimeoutSeconds()));
        }

        long ttlMillis = Env.getLong("DEDUPE_TTL_HOURS", 24) * 3_600_000L;
        Thread.ofVirtual().name("dedupe-sweeper").start(() -> {
            while (true) {
                try {
                    Thread.sleep(600_000);
                    int n = store.sweepProcessed(System.currentTimeMillis() - ttlMillis, 1000);
                    if (n > 0) {
                        log.info("swept {} dedupe rows", n);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    log.warn("dedupe sweep failed: {}", e.getMessage());
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            store.close();
        }));
        log.info("orchestration worker up (flow={}, stream={}, queue={}, direct types={})", Flows.CURRENT, stream,
                TaskQueues.orchestration(), direct);
        Thread.currentThread().join();
    }
}
