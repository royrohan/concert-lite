package io.concert.samples;

import io.concert.common.Env;
import io.concert.common.TemporalClients;
import io.concert.sdk.WorkerBootstrap;
import io.concert.store.JdbcStateStore;
import io.concert.store.Stores;
import io.temporal.worker.WorkerFactory;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs the workers for SM_TYPE (comma separated, e.g. {@code order} or {@code order,payment}). */
public final class SampleWorkerMain {
    private static final Logger log = LoggerFactory.getLogger(SampleWorkerMain.class);

    public static void main(String[] args) throws InterruptedException {
        JdbcStateStore store = Stores.fromEnv();
        var client = TemporalClients.fromEnv();
        List<WorkerFactory> factories = new ArrayList<>();
        for (String type : Env.get("SM_TYPE", "order").split(",")) {
            SampleMachines.Machine m = SampleMachines.get(type.trim());
            factories.add(WorkerBootstrap.start(client, store, type.trim(), m.impl(), new SimulatedWork.Impl()));
            log.info("worker up for sm-{}", type.trim());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            factories.forEach(WorkerFactory::shutdown);
            store.close();
        }));
        Thread.currentThread().join();
    }
}
