package io.concert.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.model.pure.ModelResolver;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** {@link SchemaMapper} against the sample workers' models (showcases/sample-workers/src/main/pure). */
class SchemaMapperTest {

    static final Path MODELS = Path.of(System.getProperty("concert.modelsDir", "../../showcases/sample-workers/src/main/pure"));

    static SchemaSpec sampleSchema() {
        return new SchemaMapper(SchemaMapper.loadModels(MODELS)).map(SchemaMapper.parseRoots(SchemaMapper.DEFAULT_ROOTS));
    }

    private static Map<String, ColumnType> types(TableSpec t) {
        return t.columns().stream().collect(Collectors.toMap(ColumnSpec::name, ColumnSpec::type, (a, b) -> a,
                java.util.LinkedHashMap::new));
    }

    @Test
    void tablesPerRootAndOwnedToManyAssociation() {
        SchemaSpec s = sampleSchema();
        assertEquals(List.of("orders", "order_lines", "payments", "payment_entries", "shipments", "shipment_parcels"),
                s.tables().stream().map(TableSpec::name).toList());
        assertEquals("demo::order::Order", s.rootFor("order").orElseThrow().pureClass());
        assertEquals(List.of("order_lines"), s.childrenOf(s.rootFor("order").orElseThrow()).stream().map(TableSpec::name).toList());
        assertEquals(List.of("lines"), s.table("order_lines").orElseThrow().sourcePath());
        assertTrue(s.rootFor("ledger").isEmpty());
    }

    @Test
    void orderColumnsAreTypedAndValueClassesFlattened() {
        TableSpec orders = sampleSchema().rootFor("order").orElseThrow();
        Map<String, ColumnType> t = types(orders);
        assertEquals(List.of("entity_id", "entity_version", "sm_type", "sm_state", "created_at", "completed_at", "model_json"),
                List.copyOf(t.keySet()).subList(0, 7));
        assertEquals(ColumnType.STRING, t.get("order_id"));
        assertEquals(ColumnType.ENUM, t.get("status"));
        assertEquals(ColumnType.ENUM, t.get("payment_method"));
        assertEquals(ColumnType.TIMESTAMPTZ, t.get("paid_at"));
        assertEquals(ColumnType.STRING, t.get("customer_customer_id"));
        assertEquals(ColumnType.STRING, t.get("customer_email"));
        assertEquals(ColumnType.DECIMAL, t.get("total_amount"));
        assertEquals(ColumnType.ENUM, t.get("total_currency"));
        assertEquals(ColumnType.STRING, t.get("shipping_address_postal_code"));
        assertEquals(ColumnType.DECIMAL, t.get("refunded_amount"));
        // lines are a child table, the derived lineCount() is not stored
        assertFalse(t.containsKey("lines"));
        assertFalse(t.containsKey("line_count"));

        ColumnSpec amount = orders.column("total_amount").orElseThrow();
        assertEquals(List.of("total", "amount"), amount.path());
        assertEquals("total", amount.group());
        assertEquals(ColumnSpec.Source.MODEL, amount.source());
    }

    @Test
    void childTablesHaveKeyColumnsAndNoBackReference() {
        SchemaSpec s = sampleSchema();
        Map<String, ColumnType> lines = types(s.table("order_lines").orElseThrow());
        assertEquals(List.of("entity_id", "entity_version", "idx", "sku", "quantity", "unit_price_amount", "unit_price_currency"),
                List.copyOf(lines.keySet()));
        assertEquals(ColumnType.BIGINT, lines.get("quantity"));
        Map<String, ColumnType> entries = types(s.table("payment_entries").orElseThrow());
        assertEquals(ColumnType.ENUM, entries.get("kind"));
        assertEquals(ColumnType.TIMESTAMPTZ, entries.get("at"));
        assertEquals(ColumnType.DECIMAL, types(s.table("shipment_parcels").orElseThrow()).get("weight_kg"));
    }

    @Test
    void deeperStructuresBecomeJsonAndNamesDoNotCollide() {
        String src = """
                Class t::Root { smState: Integer[1]; tags: String[*]; when: StrictDate[0..1]; a: A[0..1]; }
                Class t::A { b: B[0..1]; }
                Class t::B { c: C[0..1]; }
                Class t::C { x: Float[1]; }
                Class t::Item { n: Integer[1]; }
                Class t::Sub { v: Boolean[1]; }
                Association t::RootItems { root: Root[1]; items: Item[*]; }
                Association t::ItemSubs { item: Item[1]; subs: Sub[*]; }
                """;
        ResolvedModel m = new ModelResolver().resolve(PureParser.parse(src, "t.pure"));
        SchemaSpec s = new SchemaMapper(m).map(Map.of("thing", "t::Root"));
        Map<String, ColumnType> root = types(s.rootFor("thing").orElseThrow());
        assertEquals("things", s.rootFor("thing").orElseThrow().name());
        assertEquals(ColumnType.BIGINT, root.get("model_sm_state")); // sm_state is a system column
        assertEquals(ColumnType.JSON, root.get("tags"));
        assertEquals(ColumnType.DATE, root.get("when"));
        assertEquals(ColumnType.JSON, root.get("a_b_c")); // third level of value classes
        Map<String, ColumnType> items = types(s.table("thing_items").orElseThrow());
        assertEquals(ColumnType.JSON, items.get("subs")); // only one level of child tables
    }

    static final Path TRADING = Path.of(System.getProperty("concert.tradingModelsDir", "../../core/trading-model/src/main/pure"));
    static final String TRADING_ROOTS = "trading_order:trading::order::Order,trading_execution:trading::order::Execution,"
            + "trading_fill:trading::order::Fill,trading_allocation:trading::order::Allocation";

    @Test
    void multipleModelDirsAndAssociationsToOtherRootsAreNotChildTables() {
        ResolvedModel m = SchemaMapper.loadModels(MODELS + "," + TRADING);
        SchemaSpec s = new SchemaMapper(m).map(SchemaMapper.parseRoots(SchemaMapper.DEFAULT_ROOTS + "," + TRADING_ROOTS));
        assertEquals(List.of("orders", "order_lines", "payments", "payment_entries", "shipments", "shipment_parcels",
                "trading_orders", "trading_executions", "trading_fills", "trading_allocations"),
                s.tables().stream().map(TableSpec::name).toList());
        Map<String, ColumnType> orders = types(s.rootFor("trading_order").orElseThrow());
        assertFalse(orders.containsKey("executions"));
        assertFalse(orders.containsKey("allocations"));
        assertEquals(ColumnType.DECIMAL, orders.get("arrival_px"));
        assertEquals(ColumnType.BIGINT, orders.get("filled_qty"));
        Map<String, ColumnType> fills = types(s.rootFor("trading_fill").orElseThrow());
        assertEquals(ColumnType.TIMESTAMPTZ, fills.get("ts"));
        assertEquals(ColumnType.ENUM, fills.get("side"));
        assertFalse(types(s.rootFor("trading_execution").orElseThrow()).containsKey("fills"));
        // Alone (no execution root configured), Order.executions is an ordinary child table again.
        SchemaSpec alone = new SchemaMapper(m).map(Map.of("trading_order", "trading::order::Order"));
        assertEquals(List.of("trading_orders", "trading_order_executions", "trading_order_allocations"),
                alone.tables().stream().map(TableSpec::name).toList());
    }

    @Test
    void rootsSpecIsParsedAndValidated() {
        assertEquals(Map.of("order", "demo::order::Order"), SchemaMapper.parseRoots(" order:demo::order::Order ,"));
        assertThrows(IllegalArgumentException.class, () -> SchemaMapper.parseRoots("demo::order::Order"));
        ResolvedModel m = SchemaMapper.loadModels(MODELS);
        assertThrows(IllegalArgumentException.class, () -> new SchemaMapper(m).map(Map.of("x", "demo::Nope")));
    }

    @Test
    void namesAreSnakeCasedAndPluralized() {
        assertEquals("shipping_address", SchemaMapper.snake("shippingAddress"));
        assertEquals("weight_kg", SchemaMapper.snake("weightKg"));
        assertEquals("http_url", SchemaMapper.snake("HTTPUrl"));
        assertEquals("entries", SchemaMapper.plural("entry"));
        assertEquals("boxes", SchemaMapper.plural("box"));
        assertEquals("days", SchemaMapper.plural("day"));
    }

    @Test
    void envListAppendsSuffixedVariablesInOrder() {
        Map<String, String> env = new java.util.HashMap<>();
        assertEquals("dflt", SchemaMapper.envList(env, "SINK_ROOTS", "dflt"));
        env.put("SINK_ROOTS", "a:x::A");
        env.put("SINK_ROOTS_ZED", "z:x::Z");
        env.put("SINK_ROOTS_INSURANCE", "claim:insurance::Claim");
        env.put("SINK_ROOTS_EMPTY", " ");
        env.put("SINK_ROOTSX", "ignored");
        assertEquals("a:x::A,claim:insurance::Claim,z:x::Z", SchemaMapper.envList(env, "SINK_ROOTS", "dflt"));
        // and it composes with ecosystem model dirs that are loaded together
        env.remove("SINK_ROOTS");
        assertEquals("dflt,claim:insurance::Claim,z:x::Z", SchemaMapper.envList(env, "SINK_ROOTS", "dflt"));
    }

    @Test
    void eventRootsGetLifecycleColumnsThenThePayloadAndStateRootsAreThePlainClass() {
        Path models = Path.of("../../examples/order-events");
        SchemaSpec s = new SchemaMapper(SchemaMapper.loadModels(models)).map(SchemaMapper.parseRoots(
                "evt_order_create_event:orders::OrderCreateEvent,evt_payment_capture_event:orders::PaymentCaptureEvent,st_order:orders::Order"));
        TableSpec created = s.rootFor("evt_order_create_event").orElseThrow();
        assertEquals("evt_order_create_events", created.name());
        Map<String, ColumnType> t = types(created);
        assertEquals(List.of("entity_id", "entity_version", "sm_type", "sm_state", "created_at", "completed_at", "model_json",
                "event_id", "event_type", "domain", "status", "outcome", "error", "attempts", "retries", "parent_event_id",
                "causation_root", "depth", "processor_key", "request_id", "scheduled_at_ms", "lock_keys", "children",
                "order_id", "customer_id", "currency", "lines", "expiry_seconds"), List.copyOf(t.keySet()));
        assertEquals(List.of("payload", "orderId"), created.column("order_id").orElseThrow().path());
        assertEquals(List.of("status"), created.column("status").orElseThrow().path());
        assertEquals(ColumnType.JSON, t.get("lines"));
        assertEquals(ColumnType.BOOLEAN, types(s.rootFor("evt_payment_capture_event").orElseThrow()).get("poison"));

        TableSpec order = s.rootFor("st_order").orElseThrow();
        assertEquals("st_orders", order.name());
        assertEquals(List.of("orderId"), order.column("order_id").orElseThrow().path());
        assertFalse(types(order).containsKey("event_id"));

        String data = "{\"eventId\":\"e1\",\"eventType\":\"OrderCreateEvent\",\"status\":\"DONE\",\"attempts\":1,\"depth\":0,"
                + "\"keys\":[\"customer:C1\",\"order:O1\"],\"payload\":{\"orderId\":\"O1\",\"customerId\":\"C1\",\"currency\":\"USD\"}}";
        Object[] row = SnapshotRows.extract(s, created, new EntitySnapshot("event:e1", "evt_order_create_event", "DONE", 7, null,
                java.time.Instant.now(), data)).rootRow();
        Map<String, Object> byName = new java.util.LinkedHashMap<>();
        for (int i = 0; i < row.length; i++) {
            byName.put(created.columns().get(i).name(), row[i]);
        }
        assertEquals("e1", byName.get("event_id"));
        assertEquals(1L, byName.get("attempts"));
        assertEquals("O1", byName.get("order_id"));
        assertEquals("[\"customer:C1\",\"order:O1\"]", byName.get("lock_keys"));
    }
}
