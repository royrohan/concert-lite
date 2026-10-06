package io.concert.sink;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Writes snapshots to {@value SnapshotCodec#TOPIC}, keyed by entity id, and waits for the broker's
 * acknowledgement. Thread-safe; share one per process.
 *
 * <p>The producer is idempotent with {@code acks=all}, so its own retries never duplicate a record.
 * A send can still be repeated by the caller (an activity retry after a lost ack), which is harmless:
 * the topic is compacted by key and sinks upsert by {@code (entityId, version)}. The time a send may
 * take ({@code max.block.ms} + {@code delivery.timeout.ms}) stays below the publish activity's 30 s
 * start-to-close timeout, so a send fails before Temporal would retry it concurrently.
 *
 * <p>The underlying {@link KafkaProducer} is created on first use: constructing it fails when the
 * bootstrap address does not resolve, and that should surface as a retried send, not a worker that
 * cannot start.
 */
public final class KafkaSnapshotProducer implements AutoCloseable {

    private final String topic;
    private final Map<String, Object> config;
    private final Function<String, String> schemaHashes;
    private volatile KafkaProducer<String, byte[]> producer;

    /**
     * @param schemaHashes smType to the hash of its model (the {@code schemaHash} header), or {@code null}
     *     when unknown (header omitted)
     */
    public KafkaSnapshotProducer(String bootstrap, String topic, Function<String, String> schemaHashes,
            Map<String, Object> overrides) {
        this.topic = topic;
        this.schemaHashes = schemaHashes;
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "entity-snapshots-" + ProcessHandle.current().pid());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 4 * 1024 * 1024);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20_000);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        p.putAll(overrides);
        Map<String, Object> c = new HashMap<>();
        p.forEach((k, v) -> c.put((String) k, v));
        this.config = Map.copyOf(c);
    }

    public KafkaSnapshotProducer(String bootstrap, Function<String, String> schemaHashes) {
        this(bootstrap, SnapshotCodec.TOPIC, schemaHashes, Map.of());
    }

    /** Sends {@code s} and blocks until it is acknowledged by all in-sync replicas. */
    public RecordMetadata send(EntitySnapshot s) {
        ProducerRecord<String, byte[]> r = new ProducerRecord<>(topic, s.entityId(), SnapshotCodec.encode(s));
        r.headers().add(SnapshotCodec.H_SM_TYPE, s.smType().getBytes(StandardCharsets.UTF_8));
        r.headers().add(SnapshotCodec.H_VERSION, Long.toString(s.version()).getBytes(StandardCharsets.UTF_8));
        String hash = schemaHashes == null ? null : schemaHashes.apply(s.smType());
        if (hash != null) {
            r.headers().add(SnapshotCodec.H_SCHEMA_HASH, hash.getBytes(StandardCharsets.UTF_8));
        }
        try {
            return producer().send(r).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing " + s.entityId(), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("publishing " + s.entityId() + " v" + s.version() + " failed: "
                    + e.getCause().getMessage(), e.getCause());
        }
    }

    private KafkaProducer<String, byte[]> producer() {
        KafkaProducer<String, byte[]> p = producer;
        if (p == null) {
            synchronized (this) {
                p = producer;
                if (p == null) {
                    p = new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer());
                    producer = p;
                }
            }
        }
        return p;
    }

    @Override
    public synchronized void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
