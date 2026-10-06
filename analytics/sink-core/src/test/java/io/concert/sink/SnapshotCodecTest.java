package io.concert.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotCodecTest {

    static final String ORDER_JSON = "{\"customer\":{\"customerId\":\"c1\",\"name\":\"Ann\"},\"deliveredAt\":\"2026-10-05T10:00:03Z\","
            + "\"lines\":[{\"quantity\":2,\"sku\":\"A\",\"unitPrice\":{\"amount\":10.50,\"currency\":\"EUR\"}},"
            + "{\"quantity\":1,\"sku\":\"B\",\"unitPrice\":{\"amount\":4,\"currency\":\"EUR\"}}],"
            + "\"orderId\":\"o1\",\"paymentMethod\":\"CARD\",\"status\":\"DELIVERED\",\"total\":{\"amount\":25.00,\"currency\":\"EUR\"}}";

    static EntitySnapshot order(String id, long version, String json) {
        return new EntitySnapshot("order:" + id, "order", "DELIVERED", version, Instant.parse("2026-10-05T10:00:00Z"),
                Instant.parse("2026-10-05T10:00:03Z"), json);
    }

    @Test
    void roundTripsWithTheModelEmbeddedAsObject() {
        EntitySnapshot s = order("o1", 3, ORDER_JSON);
        String wire = new String(SnapshotCodec.encode(s), StandardCharsets.UTF_8);
        assertTrue(wire.startsWith("{\"entityId\":\"order:o1\",\"smType\":\"order\",\"state\":\"DELIVERED\",\"version\":3,"
                + "\"createdAt\":\"2026-10-05T10:00:00Z\",\"completedAt\":\"2026-10-05T10:00:03Z\",\"model\":{\"customer\""), wire);
        assertEquals(s, SnapshotCodec.decode(wire.getBytes(StandardCharsets.UTF_8)));
        EntitySnapshot noModel = new EntitySnapshot("x:1", "x", "DONE", 1, null, Instant.EPOCH, null);
        assertEquals(noModel, SnapshotCodec.decode(SnapshotCodec.encode(noModel)));
        assertThrows(IllegalArgumentException.class, () -> SnapshotCodec.decode("nope".getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> SnapshotCodec.decode("{\"smType\":\"x\"}".getBytes(StandardCharsets.UTF_8)));
        assertEquals(16, SnapshotCodec.schemaHash("classDiagram").length());
    }

    @Test
    void rowsAreExtractedTyped() {
        SchemaSpec schema = SchemaMapperTest.sampleSchema();
        TableSpec orders = schema.rootFor("order").orElseThrow();
        SnapshotRows.EntityRows rows = SnapshotRows.extract(schema, orders, order("o1", 3, ORDER_JSON));
        Object[] r = rows.rootRow();
        assertEquals("order:o1", r[idx(orders, "entity_id")]);
        assertEquals(3L, r[idx(orders, "entity_version")]);
        assertEquals(new BigDecimal("25.00"), r[idx(orders, "total_amount")]);
        assertEquals("EUR", r[idx(orders, "total_currency")]);
        assertEquals("Ann", r[idx(orders, "customer_name")]);
        assertNull(r[idx(orders, "customer_email")]);
        assertEquals(OffsetDateTime.of(2026, 10, 5, 10, 0, 3, 0, ZoneOffset.UTC), r[idx(orders, "delivered_at")]);
        assertEquals(ORDER_JSON, r[idx(orders, "model_json")]);

        TableSpec lines = schema.table("order_lines").orElseThrow();
        List<Object[]> lineRows = rows.children().get(lines);
        assertEquals(2, lineRows.size());
        assertEquals(1L, lineRows.get(1)[idx(lines, "idx")]);
        assertEquals("B", lineRows.get(1)[idx(lines, "sku")]);
        assertEquals(new BigDecimal("10.50"), lineRows.get(0)[idx(lines, "unit_price_amount")]);
        assertEquals(2L, lineRows.get(0)[idx(lines, "quantity")]);
    }

    static int idx(TableSpec t, String column) {
        return t.columns().indexOf(t.column(column).orElseThrow());
    }
}
