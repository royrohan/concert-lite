package io.concert.sdk;

import io.concert.sink.EntitySnapshot;
import io.concert.sink.KafkaSnapshotProducer;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes the snapshot of an entity that reached a terminal state. {@link ModelStateMachine}
 * schedules it as a regular activity with unlimited retries, so the workflow history is the outbox: a
 * snapshot is delivered at least once even if Kafka is down for a while. Implementations must be
 * idempotent per {@code (entityId, version)} (Kafka's compaction and the sinks' version-guarded
 * upserts make a repeated send harmless).
 *
 * <p>Every typed worker registers one ({@link WorkerBootstrap} falls back to {@link #fromEnv()}).
 */
@ActivityInterface
public interface EntitySnapshotPublisher {

    @ActivityMethod(name = "PublishEntitySnapshot")
    void publish(EntitySnapshot snapshot);

    /**
     * The Kafka publisher when {@code KAFKA_BOOTSTRAP} is set, otherwise {@link #logging()}. Both are
     * process-wide singletons (one producer per worker process).
     */
    static EntitySnapshotPublisher fromEnv() {
        return fromEnv(smType -> null);
    }

    /**
     * Like {@link #fromEnv()}; {@code schemaHashes} maps an smType to the hash of its model for the
     * {@code schemaHash} record header ({@code null}: omitted). Only the first call creates the instance.
     */
    static EntitySnapshotPublisher fromEnv(Function<String, String> schemaHashes) {
        return EntitySnapshotPublishers.fromEnv(schemaHashes);
    }

    /**
     * Logs and drops snapshots: used when no Kafka is configured, so existing setups and tests run
     * unchanged. Snapshots published through it are <b>not</b> delivered anywhere.
     */
    static EntitySnapshotPublisher logging() {
        return EntitySnapshotPublishers.LOGGING;
    }

    /** Sends to Kafka through a shared {@link KafkaSnapshotProducer}. */
    record Kafka(KafkaSnapshotProducer producer) implements EntitySnapshotPublisher {

        private static final Logger log = LoggerFactory.getLogger(EntitySnapshotPublisher.class);

        @Override
        public void publish(EntitySnapshot s) {
            var md = producer.send(s);
            log.debug("published {} v{} ({}) to {}-{}@{}", s.entityId(), s.version(), s.state(), md.topic(),
                    md.partition(), md.offset());
        }
    }
}
