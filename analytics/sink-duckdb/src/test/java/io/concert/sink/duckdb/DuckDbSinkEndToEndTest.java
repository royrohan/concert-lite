package io.concert.sink.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.sink.EntitySnapshot;
import io.concert.sink.KafkaSnapshotProducer;
import io.concert.sink.SnapshotCodec;
import io.concert.sink.SnapshotConsumerLoop;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.kafka.KafkaContainer;

/** Kafka (Testcontainers) -> consumer loop -> DuckDB, including restarts. Needs Docker. */
class DuckDbSinkEndToEndTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(SnapshotCodec.TOPIC, 6, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")), new NewTopic(SnapshotConsumerLoop.DEFAULT_DLQ, 1, (short) 1)))
                    .all().get();
        }
    }

    @AfterAll
    static void stop() {
        KAFKA.stop();
    }

    @TempDir
    Path dir;

    private SnapshotConsumerLoop startLoop(DuckDbTarget db) {
        SnapshotConsumerLoop loop = new SnapshotConsumerLoop(new SnapshotConsumerLoop.Config(KAFKA.getBootstrapServers(),
                SnapshotCodec.TOPIC, "sink-duckdb-test", Duration.ofMillis(200), Duration.ofSeconds(2),
                Map.of("max.poll.records", 3)), db);
        loop.start();
        return loop;
    }

    private static long count(ReadOnlyQueries q, String sql) throws SQLException {
        return ((Number) q.run(sql).rows().getFirst().getFirst()).longValue();
    }

    private static void awaitCount(ReadOnlyQueries q, String sql, long expected) throws SQLException {
        long deadline = System.currentTimeMillis() + 30_000;
        long n;
        while ((n = count(q, sql)) != expected) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(sql + ": expected " + expected + " but was " + n);
            }
            LockSupport.parkNanos(100_000_000);
        }
    }

    @Test
    void snapshotsLandOnceAcrossRestarts() throws Exception {
        String file = dir.resolve("e2e.duckdb").toString();
        try (KafkaSnapshotProducer producer = new KafkaSnapshotProducer(KAFKA.getBootstrapServers(), t -> "h")) {
            for (int i = 1; i <= 5; i++) {
                producer.send(DuckDbTargetTest.order("o" + i, 3, "A:1:2", "B:2:3"));
            }
            producer.send(DuckDbTargetTest.order("o1", 3, "A:1:2", "B:2:3")); // a republish (activity retry)

            DuckDbTarget db = new DuckDbTarget(file);
            db.ensureSchema(DuckDbTargetTest.schema());
            ReadOnlyQueries q = new ReadOnlyQueries(db, Duration.ofSeconds(10));
            SnapshotConsumerLoop loop = startLoop(db);
            awaitCount(q, "SELECT count(*) FROM orders", 5);
            awaitCount(q, "SELECT count(*) FROM order_lines", 10);
            // an undecodable record and one whose data does not fit the table (DECIMAL(38,4) overflow) go to the DLQ, the loop carries on
            try (var raw = new KafkaProducer<String, byte[]>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()),
                    new StringSerializer(), new ByteArraySerializer())) {
                raw.send(new ProducerRecord<>(SnapshotCodec.TOPIC, "junk", "not json".getBytes(StandardCharsets.UTF_8))).get();
            }
            producer.send(new EntitySnapshot("order:bad", "order", "DELIVERED", 3, null, Instant.parse("2026-10-05T10:00:00Z"),
                    "{\"orderId\":\"bad\",\"total\":{\"amount\":1e40,\"currency\":\"EUR\"}}"));
            producer.send(DuckDbTargetTest.order("o6", 3, "C:1:1"));
            awaitCount(q, "SELECT count(*) FROM orders", 6);
            SnapshotConsumerLoop l1 = loop;
            long deadline = System.currentTimeMillis() + 20_000;
            while (l1.stats().dlq() < 2 && System.currentTimeMillis() < deadline) {
                LockSupport.parkNanos(100_000_000);
            }
            assertEquals(1, loop.stats().undecodable());
            assertEquals(2, loop.stats().dlq());
            assertEquals(0, count(q, "SELECT count(*) FROM orders WHERE entity_id = 'order:bad'"));

            // HTTP API on top of the same database
            try (SinkHttpServer http = new SinkHttpServer(db, q, loop).start(0)) {
                HttpClient client = HttpClient.newHttpClient();
                String base = "http://localhost:" + http.port();
                HttpResponse<String> ok = client.send(HttpRequest.newBuilder(URI.create(base + "/query"))
                        .POST(HttpRequest.BodyPublishers.ofString("SELECT entity_id FROM orders ORDER BY 1 LIMIT 1")).build(),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(200, ok.statusCode());
                assertTrue(ok.body().contains("\"columns\":[\"entity_id\"]") && ok.body().contains("[\"order:o1\"]"), ok.body());
                HttpResponse<String> denied = client.send(HttpRequest.newBuilder(URI.create(base + "/query"))
                        .POST(HttpRequest.BodyPublishers.ofString("DELETE FROM orders")).build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(403, denied.statusCode());
                String tables = client.send(HttpRequest.newBuilder(URI.create(base + "/tables")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                assertTrue(tables.contains("\"name\":\"order_lines\",\"kind\":\"child\""), tables);
                String samples = client.send(HttpRequest.newBuilder(URI.create(base + "/samples")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                assertTrue(samples.contains("Order total by currency"), samples);
                String health = client.send(HttpRequest.newBuilder(URI.create(base + "/health")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                assertTrue(health.contains("\"undecodable\":1"), health);
                assertTrue(health.contains("\"dlq\":2"), health);
                String versions = client.send(HttpRequest.newBuilder(URI.create(base + "/versions")).POST(HttpRequest.BodyPublishers
                        .ofString("{\"smType\":\"order\",\"entityIds\":[\"order:o1\",\"order:nope\"]}")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                assertEquals("{\"smType\":\"order\",\"table\":\"orders\",\"versions\":{\"order:o1\":3}}", versions);
                String notRoot = client.send(HttpRequest.newBuilder(URI.create(base + "/versions")).POST(HttpRequest.BodyPublishers
                        .ofString("{\"smType\":\"ledger\",\"entityIds\":[\"ledger:1\"]}")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                assertTrue(notRoot.contains("\"table\":null"), notRoot);
                String metrics = client.send(HttpRequest.newBuilder(URI.create(base + "/metrics")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                assertTrue(metrics.contains("concert_sink_dlq_total{target=\"duckdb\"} 2\n"), metrics);
                assertTrue(metrics.contains("concert_sink_applied_total{target=\"duckdb\"} 6\n"), metrics);
                assertTrue(metrics.contains("concert_sink_batch_latency_seconds{target=\"duckdb\",quantile=\"0.99\"}")
                        || metrics.contains("concert_sink_batch_latency_seconds{quantile=\"0.99\",target=\"duckdb\"}"), metrics);
            }

            // stop the sink, complete more entities, restart: they appear, nothing is duplicated
            loop.close();
            q.close();
            db.close();
            producer.send(DuckDbTargetTest.order("o7", 3, "D:1:1"));
            producer.send(DuckDbTargetTest.order("o8", 3, "E:1:1", "F:1:1"));
            producer.send(DuckDbTargetTest.order("o2", 4, "G:1:1")); // a newer version replaces o2's lines

            db = new DuckDbTarget(file);
            db.ensureSchema(DuckDbTargetTest.schema());
            q = new ReadOnlyQueries(db, Duration.ofSeconds(10));
            loop = startLoop(db);
            awaitCount(q, "SELECT count(*) FROM orders", 8);
            awaitCount(q, "SELECT count(*) FROM order_lines WHERE entity_id = 'order:o2'", 1);
            assertEquals(1, count(q, "SELECT count(*) FROM order_lines WHERE entity_id = 'order:o8' AND sku = 'F'"));
            assertEquals(4, count(q, "SELECT entity_version FROM orders WHERE entity_id = 'order:o2'"));
            assertEquals(13, count(q, "SELECT count(*) FROM order_lines"));
            // only the records after the stored offsets were read again: o1..o6 were not re-applied
            assertEquals(3, db.stats().appliedEntities());
            assertEquals(0, db.stats().staleSkipped());
            Map<org.apache.kafka.common.TopicPartition, Long> offsets = db.storedOffsets();
            loop.close();
            q.close();
            db.close();

            // the offline admin path (sink stopped) deletes an entity's rows but not the offsets
            assertEquals(3, DuckDbAdmin.delete(file, "order:o8"));
            db = new DuckDbTarget(file);
            q = new ReadOnlyQueries(db, Duration.ofSeconds(10));
            assertEquals(7, count(q, "SELECT count(*) FROM orders"));
            assertEquals(offsets, db.storedOffsets());
            q.close();
            db.close();
        }
    }
}
