package io.concert.traceui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.concert.common.Json;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClickHouseConsoleTest {

    @Test
    void validateKeepsOneStatementWithoutFormat() throws Exception {
        assertEquals("SELECT 1", ClickHouseConsole.validate("  SELECT 1 ;; "));
        assertEquals("SELECT ';' AS s -- trailing comment", ClickHouseConsole.validate("SELECT ';' AS s -- trailing comment"));
        assertEquals(400, assertThrows(ClickHouseConsole.QueryException.class, () -> ClickHouseConsole.validate("SELECT 1; SELECT 2")).status);
        assertEquals(400, assertThrows(ClickHouseConsole.QueryException.class, () -> ClickHouseConsole.validate("SELECT 1 FORMAT CSV")).status);
        assertThrows(ClickHouseConsole.QueryException.class, () -> ClickHouseConsole.validate(" "));
    }

    @Test
    void adaptsJsonCompactToTheConsoleShape() throws Exception {
        var r = Json.MAPPER.readTree("{\"meta\":[{\"name\":\"entity_id\",\"type\":\"String\"},{\"name\":\"n\",\"type\":\"UInt64\"}],"
                + "\"data\":[[\"order:a1\",3]],\"rows\":1,\"statistics\":{\"elapsed\":0.0042}}");
        Map<String, Object> m = ClickHouseConsole.adapt(r, 99);
        assertEquals(List.of("entity_id", "n"), m.get("columns"));
        assertEquals(List.of("String", "UInt64"), m.get("types"));
        assertEquals("[[\"order:a1\",3]]", Json.write(m.get("rows")));
        assertEquals(4L, m.get("elapsedMs"));
        assertEquals(false, m.get("truncated"));
    }

    @Test
    void tableKindsComeFromTheProvisionerComments() {
        assertEquals("root", ClickHouseConsole.describe("orders", "ReplacingMergeTree", "concert root order").get("kind"));
        assertEquals("order", ClickHouseConsole.describe("orders", "ReplacingMergeTree", "concert root order").get("smType"));
        Map<String, Object> child = ClickHouseConsole.describe("order_lines", "View", "concert child order orders");
        assertEquals(List.of("child", "order", "orders"), List.of(child.get("kind"), child.get("smType"), child.get("parent")));
        assertEquals("marketdata", ClickHouseConsole.describe("ticks", "MergeTree", "marketdata md.ticks trading::marketdata::Tick").get("kind"));
        assertEquals("kafka", ClickHouseConsole.describe("md_ticks_queue", "Kafka", "concert:abc").get("kind"));
        assertEquals("system", ClickHouseConsole.describe("kafka_errors", "MergeTree", "concert errors: x").get("kind"));
    }

    @Test
    void lagTargets() {
        assertEquals("clickhouse", PipelineStatus.target("sink-clickhouse-md-ticks"));
        assertEquals("duckdb", PipelineStatus.target("sink-duckdb"));
        assertEquals("other", PipelineStatus.target("console-consumer-1"));
    }
}
