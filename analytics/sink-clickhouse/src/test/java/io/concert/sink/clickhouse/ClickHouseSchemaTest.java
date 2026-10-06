package io.concert.sink.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ClickHouseSchemaTest {

    static String render(Ddl d) {
        return switch (d) {
            case Ddl.Table t -> t.create();
            case Ddl.Queue q -> q.create();
            case Ddl.MaterializedView m -> m.create();
            case Ddl.View v -> v.create();
        };
    }

    /** The whole script, also written to build/clickhouse-ddl.sql for inspection. */
    static String script(List<Ddl> ddl) {
        return ddl.stream().map(d -> render(d) + (d instanceof Ddl.MaterializedView m && m.backfill() != null
                ? ";\n-- backfill after (re)creation:\n" + m.backfill() : "")).collect(Collectors.joining(";\n\n", "", ";\n"));
    }

    final List<Ddl> ddl = Fixtures.all("kafka:9092");
    final Map<String, Ddl> byName = ddl.stream().collect(Collectors.toMap(Ddl::name, Function.identity()));

    @Test
    void writesScript() throws IOException {
        Path out = Path.of("build/clickhouse-ddl.sql");
        Files.createDirectories(out.getParent());
        Files.writeString(out, script(ddl));
    }

    @Test
    void ordersTableHasDuckDbColumnNamesAndChildArray() {
        String sql = ((Ddl.Table) byName.get("orders")).create();
        for (String col : List.of("entity_id String", "entity_version UInt64", "sm_state LowCardinality(String)",
                "created_at Nullable(DateTime64(3, 'UTC'))", "model_json String", "order_id Nullable(String)",
                "status LowCardinality(Nullable(String))", "total_amount Nullable(Decimal(38, 4))",
                "total_currency LowCardinality(Nullable(String))", "customer_customer_id Nullable(String)",
                "paid_at Nullable(DateTime64(3, 'UTC'))",
                "lines Array(Tuple(idx UInt32, sku Nullable(String), quantity Nullable(Int64), "
                        + "unit_price_amount Nullable(Decimal(38, 4)), unit_price_currency LowCardinality(Nullable(String))))")) {
            assertTrue(sql.contains("    " + col), col + " in\n" + sql);
        }
        assertTrue(sql.contains("ENGINE = ReplacingMergeTree(entity_version)\nORDER BY entity_id"), sql);
        assertTrue(sql.contains("COMMENT 'concert root order'"), sql);
    }

    @Test
    void typedViewsFilterOnSmTypeAndBackfillFromFinal() {
        Ddl.MaterializedView mv = (Ddl.MaterializedView) byName.get("mv_orders");
        assertEquals("entity_snapshots", mv.source());
        assertTrue(mv.select().endsWith("FROM entity_snapshots\nWHERE sm_type = 'order'"), mv.select());
        assertTrue(mv.select().contains("toDecimal128OrNull(trim(BOTH '\"' FROM JSONExtractRaw(model_json, 'total', 'amount')), 4) AS total_amount"));
        assertTrue(mv.select().contains("parseDateTime64BestEffortOrNull(JSONExtractString(model_json, 'paidAt'), 3, 'UTC') AS paid_at"));
        assertTrue(mv.select().contains("JSONExtractArrayRaw(model_json, 'lines')"));
        assertTrue(mv.backfill().startsWith("INSERT INTO orders (entity_id, entity_version,"), mv.backfill());
        assertTrue(mv.backfill().endsWith("FROM entity_snapshots FINAL\nWHERE sm_type = 'order'"), mv.backfill());
        assertTrue(mv.create().contains("COMMENT 'concert:" + mv.hash() + "'"));
    }

    @Test
    void childViewsArrayJoinTheRootFinal() {
        Ddl.View v = (Ddl.View) byName.get("order_lines");
        assertEquals("SELECT entity_id, entity_version, c.idx AS idx, c.sku AS sku, c.quantity AS quantity, "
                + "c.unit_price_amount AS unit_price_amount, c.unit_price_currency AS unit_price_currency\n"
                + "FROM orders FINAL\nARRAY JOIN lines AS c", v.select());
        assertTrue(byName.containsKey("payment_entries") && byName.containsKey("shipment_parcels"));
    }

    @Test
    void queueReadsTheModelAsRawJsonAndStreamsErrors() {
        String q = ((Ddl.Queue) byName.get("entity_snapshots_queue")).create();
        assertTrue(q.contains("model Nullable(String)"));
        assertTrue(q.contains("kafka_topic_list = 'entity-snapshots', kafka_group_name = 'sink-clickhouse'"), q);
        assertTrue(q.contains("kafka_handle_error_mode = 'stream'") && q.contains("input_format_json_read_objects_as_strings = 1"), q);
        Ddl.MaterializedView err = (Ddl.MaterializedView) byName.get("mv_entity_snapshots_queue_errors");
        assertEquals("kafka_errors", err.target());
        assertTrue(err.select().endsWith("WHERE length(_error) > 0"));
        // data views of a queue come before its error view (the provisioner creates them in list order)
        assertTrue(ddl.indexOf(byName.get("mv_entity_snapshots")) < ddl.indexOf(err));
    }

    @Test
    void marketDataTablesAreGeneratedFromTheTradingModels() {
        String ticks = ((Ddl.Table) byName.get("ticks")).create();
        assertTrue(ticks.contains("    ts DateTime64(3, 'UTC')") && ticks.contains("    bid Decimal(38, 6)")
                && ticks.contains("    trade_venue Nullable(String)") && ticks.contains("    last_size Int64"), ticks);
        assertTrue(ticks.contains("ORDER BY (symbol, ts)") && ticks.contains("INTERVAL 1 DAY"), ticks);
        String instruments = ((Ddl.Table) byName.get("instruments")).create();
        assertTrue(instruments.contains("    sector LowCardinality(String)") && instruments.contains("ReplacingMergeTree(kafka_offset)")
                && instruments.contains("    updated_at Nullable(DateTime64(3, 'UTC'))"), instruments);
        String q = ((Ddl.Queue) byName.get("md_ticks_queue")).create();
        assertTrue(q.contains("`tradeVenue` Nullable(String)") && q.contains("`ts` String")
                && q.contains("kafka_group_name = 'sink-clickhouse-md-ticks'"), q);
        assertTrue(((Ddl.MaterializedView) byName.get("mv_bars_1m")).select().contains("`end`"));
        for (String t : List.of("accounts", "venues", "bars_1m")) {
            assertTrue(byName.containsKey(t), t);
        }
    }

    @Test
    void hashesAreStableAndFollowTheDefinition() {
        Ddl.MaterializedView a = (Ddl.MaterializedView) Fixtures.all("kafka:9092").stream()
                .filter(d -> d.name().equals("mv_orders")).findFirst().orElseThrow();
        assertEquals(((Ddl.MaterializedView) byName.get("mv_orders")).hash(), a.hash());
        Ddl.Queue other = ClickHouseSchema.queue(ClickHouseSchema.Config.defaults("other:9092"));
        assertFalse(other.hash().equals(((Ddl.Queue) byName.get("entity_snapshots_queue")).hash()));
    }

    @Test
    void samplesUseFinalAndChildViews() {
        List<ClickHouseSamples.Sample> s = ClickHouseSamples.generate(Fixtures.schema(), true);
        assertTrue(s.stream().anyMatch(x -> x.title().equals("Order total by currency") && x.sql().contains("FROM orders FINAL")));
        assertTrue(s.stream().anyMatch(x -> x.sql().contains("FROM order_lines AS c")));
        assertTrue(s.stream().anyMatch(x -> x.title().equals("Latest quote per symbol")));
        assertFalse(ClickHouseSamples.generate(Fixtures.schema(), false).stream().anyMatch(x -> x.sql().contains("ticks")));
    }
}
