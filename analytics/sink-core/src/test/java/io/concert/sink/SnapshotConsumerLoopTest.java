package io.concert.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/** {@link SnapshotConsumerLoop} with an in-memory target that stores offsets (Testcontainers Kafka). */
class SnapshotConsumerLoopTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");
    static final String TOPIC = "loop-test";
    static final String POISON_TOPIC = "loop-poison";

    /** Keeps the latest version per entity and offsets "atomically" (one lock); fails the first N applies. */
    static final class MemoryTarget implements SinkTarget {
        final Map<String, Long> versions = new ConcurrentHashMap<>();
        final Map<TopicPartition, Long> offsets = new ConcurrentHashMap<>();
        final AtomicInteger failures = new AtomicInteger();
        final AtomicInteger applies = new AtomicInteger();
        /** Entity ids whose records are rejected as bad data (poison). */
        final java.util.Set<String> poison = ConcurrentHashMap.newKeySet();
        final AtomicInteger rejections = new AtomicInteger();

        @Override
        public void ensureSchema(SchemaSpec schema) {}

        @Override
        public void apply(List<SnapshotRecord> batch) {
            apply(batch, Map.of());
        }

        @Override
        public synchronized void apply(List<SnapshotRecord> batch, Map<TopicPartition, Long> nextOffsets) {
            applies.incrementAndGet();
            if (failures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("database down");
            }
            for (SnapshotRecord r : batch) {
                if (poison.contains(r.snapshot().entityId())) {
                    rejections.incrementAndGet();
                    throw new RecordException(r, "bad data in " + r.snapshot().entityId(), new NumberFormatException("x"));
                }
            }
            for (SnapshotRecord r : batch) {
                versions.merge(r.snapshot().entityId(), r.snapshot().version(), Math::max);
                offsets.merge(r.topicPartition(), r.offset() + 1, Math::max);
            }
            nextOffsets.forEach((tp, n) -> offsets.merge(tp, n, Math::max));
        }

        @Override
        public synchronized Map<TopicPartition, Long> storedOffsets() {
            return new HashMap<>(offsets);
        }

        @Override
        public void close() {}
    }

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1), new NewTopic(POISON_TOPIC, 1, (short) 1),
                    new NewTopic(POISON_TOPIC + ".dlq", 1, (short) 1))).all().get();
        }
    }

    @AfterAll
    static void stop() {
        KAFKA.stop();
    }

    private SnapshotConsumerLoop loop(SinkTarget target) {
        return loop(target, TOPIC, "loop-test");
    }

    private SnapshotConsumerLoop loop(SinkTarget target, String topic, String group) {
        SnapshotConsumerLoop l = new SnapshotConsumerLoop(new SnapshotConsumerLoop.Config(KAFKA.getBootstrapServers(), topic,
                group, Duration.ofMillis(200), Duration.ofSeconds(1), Map.of(), topic + ".dlq", 3), target);
        l.start();
        return l;
    }

    private static void await(String what, BooleanSupplier c) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!c.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            LockSupport.parkNanos(50_000_000);
        }
    }

    @Test
    void retriesFailedBatchesAndResumesFromStoredOffsets() {
        MemoryTarget target = new MemoryTarget();
        target.failures.set(2);
        try (KafkaSnapshotProducer p = new KafkaSnapshotProducer(KAFKA.getBootstrapServers(), TOPIC, t -> null, Map.of())) {
            for (int i = 0; i < 10; i++) {
                p.send(SnapshotCodecTest.order("o" + i, 3, "{}"));
            }
            SnapshotConsumerLoop l = loop(target);
            await("all applied after retries", () -> target.versions.size() == 10);
            assertTrue(target.applies.get() >= 3, "failed batches are retried");
            assertEquals(2, l.stats().failures());
            l.close();

            // Restart: the loop positions every partition at the target's stored offsets.
            int before = target.applies.get();
            p.send(SnapshotCodecTest.order("o10", 3, "{}"));
            SnapshotConsumerLoop l2 = loop(target);
            await("new record", () -> target.versions.size() == 11);
            l2.close();
            assertEquals(11, target.offsets.values().stream().mapToLong(Long::longValue).sum());
            assertTrue(target.applies.get() - before <= 2, "only the new record was read again");
        }
    }

    @Test
    void poisonRecordsGoToTheDeadLetterTopicAndTheLoopMovesOn() throws Exception {
        MemoryTarget target = new MemoryTarget();
        target.poison.add("order:bad");
        try (KafkaSnapshotProducer p = new KafkaSnapshotProducer(KAFKA.getBootstrapServers(), POISON_TOPIC, t -> null, Map.of());
                KafkaProducer<String, byte[]> raw = new KafkaProducer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()),
                        new StringSerializer(), new ByteArraySerializer())) {
            p.send(SnapshotCodecTest.order("o1", 3, "{}"));
            raw.send(new ProducerRecord<>(POISON_TOPIC, "junk", "not json".getBytes(StandardCharsets.UTF_8))).get();
            p.send(SnapshotCodecTest.order("bad", 3, "{}"));
            p.send(SnapshotCodecTest.order("o2", 3, "{}"));
            SnapshotConsumerLoop l = loop(target, POISON_TOPIC, "poison-test");
            await("valid records applied around the poison", () -> target.versions.size() == 2 && l.stats().dlq() == 2);
            assertEquals(3, target.rejections.get(), "the rejected record is tried maxAttempts times");
            assertEquals(1, l.stats().undecodable());
            // offsets advance past both poison records, in the target's own store
            assertEquals(4L, target.offsets.get(new TopicPartition(POISON_TOPIC, 0)));
            // later valid snapshots still apply
            p.send(SnapshotCodecTest.order("o3", 3, "{}"));
            await("record after the poison", () -> target.versions.size() == 3);
            l.close();
        }

        List<ConsumerRecord<String, byte[]>> dlq = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "dlq-reader", "auto.offset.reset", "earliest"), new StringDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(POISON_TOPIC + ".dlq"));
            long deadline = System.currentTimeMillis() + 20_000;
            while (dlq.size() < 2 && System.currentTimeMillis() < deadline) {
                c.poll(Duration.ofMillis(200)).forEach(dlq::add);
            }
        }
        assertEquals(2, dlq.size());
        ConsumerRecord<String, byte[]> junk = dlq.get(0);
        assertEquals("junk", junk.key());
        assertEquals("not json", new String(junk.value(), StandardCharsets.UTF_8));
        assertEquals("1", header(junk, SnapshotConsumerLoop.H_ATTEMPTS));
        assertEquals("poison-test", header(junk, SnapshotConsumerLoop.H_TARGET));
        assertEquals(POISON_TOPIC, header(junk, SnapshotConsumerLoop.H_ORIGINAL_TOPIC));
        assertEquals("0", header(junk, SnapshotConsumerLoop.H_ORIGINAL_PARTITION));
        assertEquals("1", header(junk, SnapshotConsumerLoop.H_ORIGINAL_OFFSET));
        assertTrue(header(junk, SnapshotConsumerLoop.H_ERROR).startsWith("undecodable"));
        ConsumerRecord<String, byte[]> bad = dlq.get(1);
        assertEquals("order:bad", bad.key());
        assertEquals("3", header(bad, SnapshotConsumerLoop.H_ATTEMPTS));
        assertEquals("2", header(bad, SnapshotConsumerLoop.H_ORIGINAL_OFFSET));
        assertTrue(header(bad, SnapshotConsumerLoop.H_ERROR).contains("bad data in order:bad"), header(bad, "error"));
        assertEquals("order", header(bad, SnapshotCodec.H_SM_TYPE), "original headers are kept");
    }

    private static String header(ConsumerRecord<String, byte[]> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
