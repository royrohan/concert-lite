package io.concert.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/** {@link KafkaSnapshotProducer} against a real broker (Testcontainers, needs Docker). */
class KafkaSnapshotProducerTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(SnapshotCodec.TOPIC, 6, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact", "max.message.bytes", "4194304")))).all().get();
        }
    }

    @AfterAll
    static void stop() {
        KAFKA.stop();
    }

    @Test
    void publishesKeyedSnapshotsWithHeaders() {
        List<RecordMetadata> sent = new ArrayList<>();
        try (KafkaSnapshotProducer producer = new KafkaSnapshotProducer(KAFKA.getBootstrapServers(),
                type -> type.equals("order") ? "abc123" : null)) {
            sent.add(producer.send(SnapshotCodecTest.order("o1", 3, SnapshotCodecTest.ORDER_JSON)));
            sent.add(producer.send(SnapshotCodecTest.order("o2", 4, SnapshotCodecTest.ORDER_JSON)));
            sent.add(producer.send(new EntitySnapshot("payment:p1", "payment", "REFUNDED", 3, null,
                    Instant.EPOCH, "{\"paymentId\":\"p1\"}")));
        }
        assertEquals(3, sent.size());

        List<ConsumerRecord<String, byte[]>> got = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "producer-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(SnapshotCodec.TOPIC));
            long deadline = System.currentTimeMillis() + 30_000;
            while (got.size() < 3 && System.currentTimeMillis() < deadline) {
                c.poll(Duration.ofMillis(500)).forEach(got::add);
            }
        }
        assertEquals(3, got.size());
        ConsumerRecord<String, byte[]> o1 = got.stream().filter(r -> r.key().equals("order:o1")).findFirst().orElseThrow();
        assertEquals(SnapshotCodecTest.order("o1", 3, SnapshotCodecTest.ORDER_JSON), SnapshotCodec.decode(o1.value()));
        assertEquals("order", header(o1, SnapshotCodec.H_SM_TYPE));
        assertEquals("3", header(o1, SnapshotCodec.H_VERSION));
        assertEquals("abc123", header(o1, SnapshotCodec.H_SCHEMA_HASH));
        ConsumerRecord<String, byte[]> p1 = got.stream().filter(r -> r.key().equals("payment:p1")).findFirst().orElseThrow();
        assertNull(p1.headers().lastHeader(SnapshotCodec.H_SCHEMA_HASH));
        // same key, same partition: compaction keeps the latest snapshot per entity
        assertEquals(o1.partition(), sent.get(0).partition());
    }

    private static String header(ConsumerRecord<String, byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
