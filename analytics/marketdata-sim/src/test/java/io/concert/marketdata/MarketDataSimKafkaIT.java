package io.concert.marketdata;

import io.concert.model.runtime.ModelJson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Runs the simulator for a couple of seconds against a real broker and reads every topic back. */
@Testcontainers
class MarketDataSimKafkaIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");

    @Test
    void publishesReferenceDataAndTicks() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        MarketDataSim.Config config = new MarketDataSim.Config(42, 4, "12", 6, 300, true);
        KafkaRecordSink.ensureTopics(bootstrap, MdTopics.specs(true), (short) 1);
        KafkaRecordSink.ensureTopics(bootstrap, MdTopics.specs(true), (short) 1); // idempotent
        long ticks;
        try (KafkaRecordSink sink = new KafkaRecordSink(bootstrap)) {
            MarketDataSim sim = new MarketDataSim(config, sink);
            sim.publishReference(Instant.now());
            long until = System.currentTimeMillis() + 2_000;
            sim.runRealtime(System::currentTimeMillis, () -> System.currentTimeMillis() < until);
            ticks = sim.ticksSent();
        }
        assertTrue(ticks > 300, "ticks sent: " + ticks);

        Map<String, List<ConsumerRecord<String, String>>> got = consumeAll(bootstrap,
                List.of(MdTopics.INSTRUMENTS, MdTopics.ACCOUNTS, MdTopics.VENUES, MdTopics.TICKS), 12 + 6 + 8 + ticks);
        assertEquals(12, got.get(MdTopics.INSTRUMENTS).size());
        assertEquals(6, got.get(MdTopics.ACCOUNTS).size());
        assertEquals(8, got.get(MdTopics.VENUES).size());
        assertEquals(ticks, got.get(MdTopics.TICKS).size());

        JsonNode aapl = ModelJson.mapper().readTree(got.get(MdTopics.INSTRUMENTS).stream()
                .filter(r -> r.key().equals("AAPL")).findFirst().orElseThrow().value());
        assertEquals("TECHNOLOGY", aapl.get("sector").asText());
        assertTrue(aapl.get("tickSize").isNumber());

        // Per-symbol order: the key is the symbol, so seq increases strictly within each partition.
        Map<String, Long> lastSeq = new HashMap<>();
        for (ConsumerRecord<String, String> r : got.get(MdTopics.TICKS)) {
            long seq = ModelJson.mapper().readTree(r.value()).get("seq").asLong();
            Long prev = lastSeq.put(r.key(), seq);
            assertEquals(prev == null ? 1 : prev + 1, seq, r.key());
        }

        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(p)) {
            ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, MdTopics.INSTRUMENTS);
            Config c = admin.describeConfigs(Set.of(res)).all().get().get(res);
            assertEquals("compact", c.get("cleanup.policy").value());
            assertEquals(6, admin.describeTopics(Set.of(MdTopics.TICKS)).allTopicNames().get().get(MdTopics.TICKS)
                    .partitions().size());
        }
    }

    private static Map<String, List<ConsumerRecord<String, String>>> consumeAll(String bootstrap, List<String> topics, long expected) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + System.nanoTime());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Map<String, List<ConsumerRecord<String, String>>> out = new HashMap<>();
        topics.forEach(t -> out.put(t, new java.util.ArrayList<>()));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(topics);
            long deadline = System.currentTimeMillis() + 30_000;
            long n = 0;
            while (n < expected && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    out.get(r.topic()).add(r);
                    n++;
                }
            }
        }
        return out;
    }
}
