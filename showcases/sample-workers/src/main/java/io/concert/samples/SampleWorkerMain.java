package io.concert.samples;

import io.concert.common.Env;
import io.concert.common.TemporalClients;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.samples.events.DemoEvents;
import io.concert.sdk.WorkerBootstrap;
import io.concert.sdk.events.EventWorkerBootstrap;
import io.concert.sink.SnapshotCodec;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import io.temporal.worker.WorkerFactory;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the workers for SM_TYPE (comma separated, e.g. {@code order} or {@code order,payment}; {@code none}: none) and, with
 * {@code EVENT_DOMAINS=demo}, the event-style demo domain ({@link DemoEvents}).
 */
public final class SampleWorkerMain {
    private static final Logger log = LoggerFactory.getLogger(SampleWorkerMain.class);

    public static void main(String[] args) throws InterruptedException {
        StateStore store = Stores.fromEnv();
        var client = TemporalClients.fromEnv();
        // One Kafka producer for all types in this process (no-op when KAFKA_BOOTSTRAP is unset). The
        // schemaHash header is the hash of the machine's model diagram, which changes with its .pure files.
        EntitySnapshotPublisher snapshots = EntitySnapshotPublisher.fromEnv(type -> {
            SampleMachines.Machine m = SampleMachines.ALL.get(type);
            return m == null || m.modelMermaid() == null ? null : SnapshotCodec.schemaHash(m.modelMermaid());
        });
        List<WorkerFactory> factories = new ArrayList<>();
        for (String type : Env.get("SM_TYPE", "order").split(",")) {
            if (type.isBlank() || type.trim().equals("none")) {
                continue;
            }
            SampleMachines.Machine m = SampleMachines.get(type.trim());
            factories.add(WorkerBootstrap.start(client, store, type.trim(), m.impl(), new SimulatedWork.Impl(), snapshots));
            log.info("worker up for sm-{}", type.trim());
        }
        for (String domain : Env.get("EVENT_DOMAINS", "").split(",")) {
            if (domain.isBlank()) {
                continue;
            }
            if (!domain.trim().equals(DemoEvents.DOMAIN)) {
                throw new IllegalArgumentException("unknown event domain " + domain + " (available: " + DemoEvents.DOMAIN + ")");
            }
            factories.add(EventWorkerBootstrap.start(client, store, DemoEvents.handlers(),
                    EventWorkerBootstrap.Options.defaults().withPublisher(snapshots)));
            log.info("event worker up for ev-{}", domain.trim());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            factories.forEach(WorkerFactory::shutdown);
            store.close();
        }));
        Thread.currentThread().join();
    }
}
