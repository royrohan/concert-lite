package io.concert.legend;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.legend.LegendStoreGenerator.Target;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The generated store layer's shape, without a legend-lite server (see {@link LegendLiteCompileTest} for that). */
class LegendStoreGeneratorTest {
    static final Path REPO = Path.of(System.getProperty("concert.repo", "../.."));
    static final String SAMPLES = REPO.resolve("showcases/sample-workers/src/main/pure").toString();

    private static String samples(List<Target> targets) {
        return LegendStoreMain.generate(SAMPLES, "order:demo::order::Order,payment:demo::payment::Payment", targets);
    }

    @Test
    void oneDatabaseServesBothStoresWithKeysAndJoins() {
        String pure = samples(List.of(Target.DUCKDB, Target.CLICKHOUSE));
        assertTrue(pure.contains("Database concert::store::Sink"));
        assertFalse(pure.contains("Database concert::store::DuckDb"), "one Database: the runtime picks the store");
        assertTrue(pure.contains("  Table orders_latest\n"), "roots through their one-row-per-entity view (both stores)");
        assertFalse(pure.contains("  Table orders\n"));
        assertTrue(pure.contains("  Table order_lines\n"));
        assertTrue(pure.contains("    entity_id VARCHAR(4096) PRIMARY KEY"));
        assertTrue(pure.contains("    idx BIGINT PRIMARY KEY"), "child key");
        assertTrue(pure.contains("    sm_state VARCHAR(4096)"), "system columns reachable by table access");
        assertTrue(pure.contains("    total_amount DECIMAL(38, 4)"));
        assertTrue(pure.contains("Join orders_order_lines(orders_latest.entity_id = order_lines.entity_id)"));
        assertTrue(pure.contains("Runtime concert::runtime::DuckDb\n{\n  mappings: [concert::mapping::Sink];"));
        assertTrue(pure.contains("Runtime concert::runtime::ClickHouse\n{\n  mappings: [concert::mapping::Sink];"));
        assertTrue(pure.contains("type: ClickHouse;") && pure.contains("type: DuckDB;"));
    }

    @Test
    void mappingsEmbedValueClassesAndMapEnumsAndAssociations() {
        String pure = samples(List.of(Target.DUCKDB));
        assertTrue(pure.contains("*demo::order::Order[orders]: Relational"));
        assertTrue(pure.contains("~mainTable [concert::store::Sink] orders_latest"));
        assertTrue(pure.contains("    total\n    (\n      amount: [concert::store::Sink] orders_latest.total_amount,"), pure);
        assertTrue(pure.contains("status: EnumerationMapping demo_order_OrderStatus: [concert::store::Sink] orders_latest.status"));
        assertTrue(pure.contains("demo::order::OrderStatus: EnumerationMapping demo_order_OrderStatus"));
        assertTrue(pure.contains("lines[orders, order_lines]: [concert::store::Sink] @orders_order_lines"));
        assertFalse(pure.contains(": [concert::store::Sink] orders_latest.sm_state"), "system columns are not mapped to properties");
        assertFalse(pure.contains("orders_latest.model_json"), "JSON columns are not mapped to properties");
    }

    @Test
    void eventRootsMapThePayloadAndLeaveLifecycleColumnsToTableAccess() {
        String pure = LegendStoreMain.generate(REPO.resolve("showcases/order-events/src/main/pure").toString(),
                "evt_order_create_event:orders::OrderCreateEvent", List.of(Target.DUCKDB));
        assertTrue(pure.contains("  Table evt_order_create_events_latest\n"));
        assertTrue(pure.contains("    attempts BIGINT"), "lifecycle column in the table");
        assertTrue(pure.contains("orderId: [concert::store::Sink] evt_order_create_events_latest.order_id"), pure);
        assertFalse(pure.contains("evt_order_create_events_latest.attempts"), "lifecycle columns are not mapped");
    }

    /** Two roots of one class (gen_trading_order / trading_order): the second gets its own Mapping and runtimes. */
    @Test
    void aClassSharedByTwoRootsGetsAVariantMappingPerExtraRoot() {
        String trading = REPO.resolve("core/trading-model/src/main/pure").toString();
        String pure = LegendStoreMain.generate(trading, "trading_order:trading::order::Order,gen_trading_order:trading::order::Order",
                List.of(Target.DUCKDB, Target.CLICKHOUSE));
        assertTrue(pure.contains("*trading::order::Order[trading_orders]: Relational"), "default: the first root is the class's root set");
        assertTrue(pure.contains("Mapping concert::mapping::Sink__gen_trading_orders"));
        assertTrue(pure.contains("*trading::order::Order[gen_trading_orders]: Relational\n  {\n    ~mainTable [concert::store::Sink] gen_trading_orders_latest"),
                "variant: the extra root is the class's root set");
        assertTrue(pure.contains("Runtime concert::runtime::DuckDb__gen_trading_orders\n{\n  mappings: [concert::mapping::Sink__gen_trading_orders];"));
        assertTrue(pure.contains("Runtime concert::runtime::ClickHouse__gen_trading_orders"));
    }
}
