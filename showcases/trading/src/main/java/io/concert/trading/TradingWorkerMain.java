package io.concert.trading;

import io.concert.common.Env;
import io.concert.common.TemporalClients;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sdk.WorkerBootstrap;
import io.concert.sink.SnapshotCodec;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import io.concert.trading.machines.TradingMachines;
import io.temporal.worker.WorkerFactory;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the workers of all four trading machines in one process (task queues {@code sm-trading_order},
 * {@code sm-trading_execution}, {@code sm-trading_fill}, {@code sm-trading_allocation}). Same
 * environment as the sample worker ({@code TEMPORAL_*}, {@code STORE_KIND}, {@code KAFKA_BOOTSTRAP},
 * ...); {@code TRADING_SM_TYPES} (comma separated) optionally restricts the types.
 */
public final class TradingWorkerMain {
    private static final Logger log = LoggerFactory.getLogger(TradingWorkerMain.class);

    private TradingWorkerMain() {}

    public static void main(String[] args) throws InterruptedException {
        StateStore store = Stores.fromEnv();
        var client = TemporalClients.fromEnv();
        TradingMachines.registerSpecs();
        // One Kafka producer for the four types (logs and drops when KAFKA_BOOTSTRAP is unset); the
        // schemaHash header is the hash of the trading model diagram.
        String schemaHash = SnapshotCodec.schemaHash(TradingMachines.MODEL_MERMAID);
        EntitySnapshotPublisher snapshots = EntitySnapshotPublisher.fromEnv(
                type -> TradingMachines.ALL.containsKey(type) ? schemaHash : null);
        String types = Env.get("TRADING_SM_TYPES", String.join(",", TradingMachines.ALL.keySet()));
        List<WorkerFactory> factories = new ArrayList<>();
        for (String type : types.split(",")) {
            TradingMachines.Machine m = TradingMachines.get(type.trim());
            factories.add(WorkerBootstrap.start(client, store, m.smType(), m.impl(), snapshots));
            log.info("worker up for sm-{}", m.smType());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            factories.forEach(WorkerFactory::shutdown);
            store.close();
        }));
        Thread.currentThread().join();
    }
}
