package io.concert.sink.clickhouse;

import io.concert.model.pure.ResolvedModel;
import io.concert.sink.EntitySnapshot;
import io.concert.sink.SchemaMapper;
import io.concert.sink.SchemaSpec;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Models and snapshots shared by the tests. */
final class Fixtures {
    static final Path MODELS = Path.of(System.getProperty("concert.modelsDir", "../../showcases/sample-workers/src/main/pure"));
    static final Path TRADING_MODELS = Path.of(System.getProperty("concert.tradingModelsDir", "../../core/trading-model/src/main/pure"));

    private Fixtures() {}

    static SchemaSpec schema() {
        return new SchemaMapper(SchemaMapper.loadModels(MODELS)).map(SchemaMapper.parseRoots(SchemaMapper.DEFAULT_ROOTS));
    }

    static ResolvedModel trading() {
        return SchemaMapper.loadModels(TRADING_MODELS);
    }

    static final String TRADING_ROOTS = "trading_order:trading::order::Order,trading_execution:trading::order::Execution,"
            + "trading_fill:trading::order::Fill,trading_allocation:trading::order::Allocation";

    /** Sample and trading roots over both model directories (as in compose). */
    static SchemaSpec tradingSchema() {
        return new SchemaMapper(SchemaMapper.loadModels(MODELS + "," + TRADING_MODELS))
                .map(SchemaMapper.parseRoots(SchemaMapper.DEFAULT_ROOTS + "," + TRADING_ROOTS));
    }

    /** Everything the provisioner applies with the trading roots, market data and the trading views. */
    static List<Ddl> allWithTrading(String brokers) {
        SchemaSpec schema = tradingSchema();
        List<Ddl> ddl = new ArrayList<>(ClickHouseSchema.common());
        ddl.addAll(ClickHouseSchema.entities(schema, ClickHouseSchema.Config.defaults(brokers)));
        ddl.addAll(MarketDataSchema.ddl(trading(), brokers));
        ddl.addAll(TradingAnalytics.views(schema, true));
        return ddl;
    }

    /** Everything the provisioner applies, with the given broker list. */
    static List<Ddl> all(String brokers) {
        List<Ddl> ddl = new ArrayList<>(ClickHouseSchema.common());
        ddl.addAll(ClickHouseSchema.entities(schema(), ClickHouseSchema.Config.defaults(brokers)));
        ddl.addAll(MarketDataSchema.ddl(trading(), brokers));
        return ddl;
    }

    /** An order snapshot with the given lines, each {@code sku:qty:price} in EUR. */
    static EntitySnapshot order(String id, long version, String... lines) {
        StringBuilder ls = new StringBuilder();
        double total = 0;
        for (String l : lines) {
            String[] p = l.split(":");
            ls.append(ls.isEmpty() ? "" : ",").append("{\"quantity\":").append(p[1]).append(",\"sku\":\"").append(p[0])
                    .append("\",\"unitPrice\":{\"amount\":").append(p[2]).append(",\"currency\":\"EUR\"}}");
            total += Integer.parseInt(p[1]) * Double.parseDouble(p[2]);
        }
        String json = "{\"customer\":{\"customerId\":\"c1\",\"name\":\"Ann\"},\"deliveredAt\":\"2026-10-05T10:00:03Z\",\"lines\":["
                + ls + "],\"orderId\":\"" + id + "\",\"paymentMethod\":\"CARD\",\"status\":\"DELIVERED\",\"total\":{\"amount\":"
                + String.format("%.2f", total) + ",\"currency\":\"EUR\"}}";
        return new EntitySnapshot("order:" + id, "order", "DELIVERED", version, Instant.parse("2026-10-05T10:00:00Z"),
                Instant.parse("2026-10-05T10:00:03Z"), json);
    }

    static EntitySnapshot shipment(String id, long version, String... parcels) {
        StringBuilder ps = new StringBuilder();
        for (String p : parcels) {
            String[] f = p.split(":");
            ps.append(ps.isEmpty() ? "" : ",").append("{\"parcelId\":\"").append(f[0]).append("\",\"weightKg\":").append(f[1]).append('}');
        }
        String json = "{\"shipmentId\":\"" + id + "\",\"status\":\"DELIVERED\",\"carrier\":\"DHL\",\"parcels\":[" + ps
                + "],\"destination\":{\"line1\":\"1 Main St\",\"city\":\"Berlin\",\"postalCode\":\"10115\",\"country\":\"DE\"}}";
        return new EntitySnapshot("shipment:" + id, "shipment", "DELIVERED", version, Instant.parse("2026-10-05T10:00:00Z"),
                Instant.parse("2026-10-05T10:01:00Z"), json);
    }
}
