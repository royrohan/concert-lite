package io.concert.sink.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.concert.sink.EntitySnapshot;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Validates the generated DDL with {@code clickhouse local}: the {@code clickhouse} binary if it is on
 * the PATH, else the {@value #IMAGE} image through Docker; skipped if neither is available. Kafka
 * queues are left out (no broker here; the Testcontainers test covers them): snapshots are inserted
 * into {@code entity_snapshots} directly, which drives the typed views like the queue would.
 */
class ClickHouseLocalDdlTest {

    static final String IMAGE = "clickhouse/clickhouse-server:26.3";

    static List<String> command() {
        if (works(List.of("clickhouse", "local", "--version"))) {
            return List.of("clickhouse", "local", "--multiquery");
        }
        if (works(List.of("docker", "image", "inspect", IMAGE))) {
            return List.of("docker", "run", "--rm", "-i", IMAGE, "clickhouse", "local", "--multiquery");
        }
        return null;
    }

    private static boolean works(List<String> cmd) {
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            return p.waitFor(60, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    static String run(List<String> cmd, String script) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        p.getOutputStream().write(script.getBytes(StandardCharsets.UTF_8));
        p.getOutputStream().close();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(120, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new AssertionError("clickhouse local failed:\n" + out);
        }
        return out;
    }

    static String insertRaw(EntitySnapshot s) {
        return "INSERT INTO entity_snapshots (entity_id, entity_version, sm_type, sm_state, created_at, completed_at, model_json,"
                + " kafka_partition, kafka_offset) VALUES (" + Sql.str(s.entityId()) + ", " + s.version() + ", " + Sql.str(s.smType())
                + ", " + Sql.str(s.state()) + ", " + Sql.str(s.createdAt().toString()) + ", " + Sql.str(s.completedAt().toString())
                + ", " + Sql.str(s.modelJson()) + ", 0, 0)";
    }

    @Test
    void generatedDdlRunsAndViewsExtractTheModel() throws Exception {
        List<String> cmd = command();
        assumeTrue(cmd != null, "neither a clickhouse binary nor Docker with " + IMAGE);
        List<String> stmts = new ArrayList<>();
        List<Ddl> ddl = Fixtures.all("kafka:9092");
        for (Ddl d : ddl) {
            switch (d) {
                case Ddl.Table t -> {
                    stmts.add(t.create());
                    stmts.add(t.addColumns()); // a second run: additive ALTER is a no-op
                }
                case Ddl.View v -> stmts.add(v.create());
                case Ddl.MaterializedView m -> stmts.add(m.create());
                // a Memory table shaped like the queue, with the Kafka engine's virtual columns, stands in for it
                case Ddl.Queue q -> stmts.add(q.body().substring(0, q.body().indexOf("\n)\nENGINE"))
                        + ",\n    _topic String, _partition UInt64, _offset UInt64, _error String, _raw_message String\n) ENGINE = Memory");
            }
        }
        // through the queue stand-in: one snapshot as the Kafka engine would deliver it, one parse error
        stmts.add("INSERT INTO entity_snapshots_queue (entityId, smType, state, version, createdAt, completedAt, model, _topic,"
                + " _partition, _offset, _error, _raw_message) VALUES ('shipment:s0', 'shipment', 'LOST', 2, NULL,"
                + " '2026-10-05T11:00:00Z', '{\"shipmentId\":\"s0\",\"carrier\":\"UPS\",\"parcels\":[]}', 'entity-snapshots', 1, 7, '', ''),"
                + " ('', '', NULL, 0, NULL, NULL, NULL, 'entity-snapshots', 2, 9, 'Cannot parse input', 'not json')");
        stmts.add(insertRaw(Fixtures.order("o1", 3, "A:2:10.50", "B:1:4")));
        stmts.add(insertRaw(Fixtures.order("o1", 2, "OLD:1:1"))); // older version: loses under FINAL
        stmts.add(insertRaw(Fixtures.order("o2", 5, "C:1:7.25")));
        stmts.add(insertRaw(Fixtures.shipment("s1", 4, "P1:1.5", "P2:0.25")));
        // a value that overflows Decimal(38, 4) reads as NULL: an exception in a view would stall the Kafka engine
        stmts.add(insertRaw(new EntitySnapshot("order:o3", "order", "DELIVERED", 1, Instant.parse("2026-10-05T10:00:00Z"),
                Instant.parse("2026-10-05T10:00:03Z"),
                "{\"orderId\":\"o3\",\"total\":{\"amount\":1e40,\"currency\":\"EUR\"},\"lines\":[]}")));
        // backfill path: re-insert from the raw table (what the provisioner does after recreating a view)
        Ddl.MaterializedView mvOrders = (Ddl.MaterializedView) ddl.stream().filter(d -> d.name().equals("mv_orders")).findFirst().orElseThrow();
        stmts.add(mvOrders.backfill());
        stmts.add("SELECT entity_id, entity_version, total_amount, total_currency, customer_name, delivered_at, length(lines)"
                + " FROM orders FINAL ORDER BY entity_id FORMAT TSV");
        stmts.add("SELECT entity_id, idx, sku, quantity, unit_price_amount, unit_price_currency FROM order_lines ORDER BY entity_id, idx FORMAT TSV");
        stmts.add("SELECT entity_id, parcel_id, weight_kg FROM shipment_parcels ORDER BY idx FORMAT TSV");
        stmts.add("SELECT entity_id, sm_state, carrier, destination_city, length(parcels) FROM shipments FINAL ORDER BY entity_id FORMAT TSV");
        stmts.add("SELECT source, kafka_offset, error, raw_message FROM kafka_errors FORMAT TSV");
        String out = run(cmd, String.join(";\n", stmts) + ";\n");
        List<String> lines = out.lines().filter(l -> !l.equals("1")).toList();
        assertEquals(List.of(
                "order:o1\t3\t25\tEUR\tAnn\t2026-10-05 10:00:03.000\t2",
                "order:o2\t5\t7.25\tEUR\tAnn\t2026-10-05 10:00:03.000\t1",
                "order:o3\t1\t\\N\tEUR\t\\N\t\\N\t0",
                "order:o1\t0\tA\t2\t10.5\tEUR",
                "order:o1\t1\tB\t1\t4\tEUR",
                "order:o2\t0\tC\t1\t7.25\tEUR",
                "shipment:s1\tP1\t1.5",
                "shipment:s1\tP2\t0.25",
                "shipment:s0\tLOST\tUPS\t\\N\t0",
                "shipment:s1\tDELIVERED\tDHL\tBerlin\t2",
                "entity_snapshots_queue\t9\tCannot parse input\tnot json"), lines);
    }
}
