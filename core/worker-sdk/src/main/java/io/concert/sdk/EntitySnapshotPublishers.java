package io.concert.sdk;

import io.concert.common.Env;
import io.concert.sink.KafkaSnapshotProducer;
import io.concert.sink.SnapshotCodec;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process-wide publisher instances, see {@link EntitySnapshotPublisher#fromEnv()}. */
final class EntitySnapshotPublishers {
    private static final Logger log = LoggerFactory.getLogger(EntitySnapshotPublishers.class);

    private EntitySnapshotPublishers() {}

    static final EntitySnapshotPublisher LOGGING = s -> log.debug(
            "KAFKA_BOOTSTRAP not set: dropping snapshot of {} v{} ({})", s.entityId(), s.version(), s.state());

    private static EntitySnapshotPublisher shared;

    static synchronized EntitySnapshotPublisher fromEnv(Function<String, String> schemaHashes) {
        if (shared == null) {
            String bootstrap = Env.get("KAFKA_BOOTSTRAP", "");
            if (bootstrap.isBlank()) {
                log.info("KAFKA_BOOTSTRAP not set: entity snapshots are logged, not delivered");
                shared = LOGGING;
            } else {
                KafkaSnapshotProducer producer = new KafkaSnapshotProducer(bootstrap, schemaHashes);
                Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "snapshot-producer-close"));
                log.info("entity snapshots go to Kafka {} topic {}", bootstrap, SnapshotCodec.TOPIC);
                shared = new EntitySnapshotPublisher.Kafka(producer);
            }
        }
        return shared;
    }
}
