package io.concert.sink.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.sink.KafkaSnapshotProducer;
import io.concert.sink.SnapshotCodec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Kafka -> ClickHouse Kafka engine -> materialized views, both in Testcontainers on one network:
 * backfill of records produced before provisioning, version-guarded dedup under FINAL, child views,
 * the error table, market data, and an idempotent second provisioning run. Needs Docker.
 */
class ClickHouseKafkaIT {

    static final Network NET = Network.newNetwork();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1")
            .withNetwork(NET).withListener("kafka:19092");
    static final GenericContainer<?> CH = new GenericContainer<>("clickhouse/clickhouse-server:26.3")
            .withNetwork(NET).withEnv("CLICKHOUSE_USER", "concert").withEnv("CLICKHOUSE_PASSWORD", "concert")
            .withExposedPorts(8123).waitingFor(Wait.forHttp("/ping").forPort(8123).withStartupTimeout(Duration.ofMinutes(2)));
    static ClickHouseHttp ch;

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        CH.start();
        ch = new ClickHouseHttp("http://" + CH.getHost() + ":" + CH.getMappedPort(8123), "concert", "concert");
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(SnapshotCodec.TOPIC, 6, (short) 1).configs(Map.of("cleanup.policy", "compact")),
                    new NewTopic("ref.instruments", 1, (short) 1).configs(Map.of("cleanup.policy", "compact")),
                    new NewTopic("ref.accounts", 1, (short) 1), new NewTopic("ref.venues", 1, (short) 1),
                    new NewTopic("md.ticks", 6, (short) 1), new NewTopic("md.bars.1m", 3, (short) 1))).all().get();
        }
    }

    @AfterAll
    static void stop() {
        CH.stop();
        KAFKA.stop();
        NET.close();
    }

    static long count(String sql) {
        return ch.select(sql).get(0).elements().next().asLong();
    }

    static void await(String sql, long expected) {
        long deadline = System.currentTimeMillis() + 90_000;
        long n;
        while ((n = count(sql)) != expected) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(sql + ": expected " + expected + " but was " + n);
            }
            LockSupport.parkNanos(500_000_000L);
        }
    }

    static Provisioner.Report provision() {
        return new Provisioner(ch).apply(Fixtures.all("kafka:19092"), ClickHouseSamples.generate(Fixtures.schema(), true));
    }

    @Test
    void snapshotsAndMarketDataFlowThroughTheKafkaEngine() throws Exception {
        try (KafkaSnapshotProducer producer = new KafkaSnapshotProducer(KAFKA.getBootstrapServers(), t -> "h");
                var raw = new KafkaProducer<String, String>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()),
                        new StringSerializer(), new StringSerializer())) {
            // produced before ClickHouse knows the topic: the new consumer group starts at the earliest offset
            producer.send(Fixtures.order("o1", 3, "A:2:10.50", "B:1:4"));
            producer.send(Fixtures.order("o2", 3, "C:1:1"));
            producer.send(Fixtures.shipment("s1", 4, "P1:1.5"));

            Provisioner.Report first = provision();
            assertTrue(first.created().containsAll(List.of("orders", "mv_orders", "entity_snapshots_queue", "ticks")), first.toString());
            await("SELECT count() FROM orders FINAL", 2);
            await("SELECT count() FROM order_lines", 3);
            await("SELECT count() FROM shipment_parcels", 1);

            // duplicate, older and newer versions; an unparsable record
            producer.send(Fixtures.order("o1", 3, "A:2:10.50", "B:1:4"));
            producer.send(Fixtures.order("o1", 2, "STALE:9:9"));
            producer.send(Fixtures.order("o2", 4, "D:1:2", "E:3:1"));
            raw.send(new ProducerRecord<>(SnapshotCodec.TOPIC, "junk", "not json")).get();
            await("SELECT entity_version FROM orders FINAL WHERE entity_id = 'order:o2'", 4);
            await("SELECT count() FROM kafka_errors", 1);
            assertEquals(2, count("SELECT count() FROM orders FINAL"));
            assertEquals(3, count("SELECT entity_version FROM orders FINAL WHERE entity_id = 'order:o1'"));
            assertEquals(2, count("SELECT count() FROM order_lines WHERE entity_id = 'order:o1'"));
            assertEquals(0, count("SELECT count() FROM order_lines WHERE sku = 'STALE'"));
            assertEquals(2, count("SELECT count() FROM order_lines WHERE entity_id = 'order:o2'"));
            assertEquals(25, count("SELECT toInt64(total_amount) FROM orders FINAL WHERE entity_id = 'order:o1'"));

            // market data: ModelJson values, absent optional fields omitted
            raw.send(new ProducerRecord<>("ref.instruments", "CAT", "{\"assetClass\":\"EQUITY\",\"currency\":\"USD\",\"isin\":\"US1491231015\","
                    + "\"lotSize\":100,\"name\":\"Caterpillar\",\"primaryMic\":\"XNYS\",\"refPrice\":406.1,\"sector\":\"INDUSTRIALS\","
                    + "\"symbol\":\"CAT\",\"tickSize\":0.01}")).get();
            raw.send(new ProducerRecord<>("md.ticks", "CAT", "{\"ask\":406.12,\"askSize\":1000,\"bid\":406.08,\"bidSize\":1800,"
                    + "\"last\":406.12,\"lastSize\":700,\"mid\":406.10,\"seq\":1,\"symbol\":\"CAT\",\"tradeVenue\":\"MEMX\","
                    + "\"ts\":\"2026-10-05T14:30:00Z\",\"tsMillis\":1791210600000,\"volume\":700}")).get();
            raw.send(new ProducerRecord<>("md.ticks", "CAT", "{\"ask\":406.14,\"askSize\":900,\"bid\":406.10,\"bidSize\":1500,"
                    + "\"mid\":406.12,\"seq\":2,\"symbol\":\"CAT\",\"ts\":\"2026-10-05T14:30:00.250Z\",\"tsMillis\":1791210600250,"
                    + "\"volume\":700}")).get();
            await("SELECT count() FROM ticks", 2);
            await("SELECT count() FROM instruments FINAL", 1);
            assertEquals(1, count("SELECT count() FROM ticks WHERE trade_venue IS NULL AND last_size = 0 AND seq = 2"));
            assertEquals(1, count("SELECT count() FROM instruments FINAL WHERE sector = 'INDUSTRIALS' AND updated_at IS NULL"));
            assertEquals(0, count("SELECT count() FROM kafka_errors WHERE source != 'entity_snapshots_queue'"));

            // second run with the same models: nothing to do; samples rewritten
            Provisioner.Report second = provision();
            assertEquals(List.of(), second.created());
            assertEquals(List.of(), second.recreated());
            assertTrue(count("SELECT count() FROM _concert_samples") > 10);
            producer.send(Fixtures.order("o3", 1, "F:1:1"));
            await("SELECT count() FROM orders FINAL", 3);

            // schema evolution: a new column and a changed view -> ADD COLUMN, view recreated, target backfilled
            List<Ddl> evolved = new ArrayList<>();
            for (Ddl d : Fixtures.all("kafka:19092")) {
                if (d instanceof Ddl.Table t && t.name().equals("orders")) {
                    List<Ddl.Column> cols = new ArrayList<>(t.columns());
                    cols.add(new Ddl.Column("note", "Nullable(String)"));
                    evolved.add(new Ddl.Table(t.name(), cols, t.engine(), t.comment()));
                } else if (d instanceof Ddl.MaterializedView m && m.name().equals("mv_orders")) {
                    evolved.add(new Ddl.MaterializedView(m.name(), m.source(), m.target(),
                            m.select().replace("SELECT\n", "SELECT\n    'v2' AS note,\n"),
                            m.backfill().replace("INSERT INTO orders (", "INSERT INTO orders (note, ").replace("SELECT\n", "SELECT\n    'v2' AS note,\n")));
                } else {
                    evolved.add(d);
                }
            }
            Provisioner.Report third = new Provisioner(ch).apply(evolved, List.of());
            assertEquals(List.of("mv_orders"), third.recreated());
            assertEquals(List.of("orders"), third.backfilled());
            assertEquals(3, count("SELECT count() FROM orders FINAL WHERE note = 'v2'"));
            assertEquals(3, count("SELECT count() FROM orders FINAL"));
        }
    }
}
