package io.concert.sink.clickhouse;

import io.concert.model.pure.ResolvedModel;
import io.concert.sink.SchemaMapper;
import io.concert.sink.SchemaSpec;
import io.concert.sink.SnapshotCodec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-shot ClickHouse provisioner: generates the Kafka engine tables, materialized views and typed
 * tables from the Pure models and applies them, then exits. ClickHouse does the consuming.
 *
 * <pre>
 * CLICKHOUSE_URL          http://localhost:8123   CLICKHOUSE_USER / CLICKHOUSE_PASSWORD  concert / concert
 * KAFKA_BROKERS           kafka:9092 (as ClickHouse reaches Kafka)
 * SNAPSHOT_TOPIC          entity-snapshots        CLICKHOUSE_GROUP   sink-clickhouse
 * MODELS_DIR              /models (comma-separated dirs allowed)  SINK_ROOTS  order:demo::order::Order,...
 * MODELS_DIR_*, SINK_ROOTS_*   appended to MODELS_DIR / SINK_ROOTS (generated ecosystems' compose.yml)
 * MARKETDATA_MODELS_DIR   /models-trading (trading-model's .pure files); empty or missing: no market data
 * WAIT_SECONDS            120 (how long to wait for ClickHouse)
 * </pre>
 */
public final class ClickHouseProvisionerMain {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseProvisionerMain.class);

    private ClickHouseProvisionerMain() {}

    public static void main(String[] args) {
        ClickHouseHttp ch = new ClickHouseHttp(env("CLICKHOUSE_URL", "http://localhost:8123"),
                env("CLICKHOUSE_USER", "concert"), env("CLICKHOUSE_PASSWORD", "concert"));
        long deadline = System.currentTimeMillis() + Long.parseLong(env("WAIT_SECONDS", "120")) * 1000;
        while (!ch.ping()) {
            if (System.currentTimeMillis() > deadline) {
                log.error("ClickHouse not reachable at {}", env("CLICKHOUSE_URL", "http://localhost:8123"));
                System.exit(1);
            }
            LockSupport.parkNanos(1_000_000_000L);
        }
        String brokers = env("KAFKA_BROKERS", "kafka:9092");
        SchemaSpec schema = new SchemaMapper(SchemaMapper.loadModels(
                SchemaMapper.envList(System.getenv(), "MODELS_DIR", env("MODELS_DIR", "/models"))))
                .map(SchemaMapper.parseRoots(SchemaMapper.envList(System.getenv(), "SINK_ROOTS",
                        env("SINK_ROOTS", SchemaMapper.DEFAULT_ROOTS))));
        List<Ddl> ddl = new ArrayList<>(ClickHouseSchema.common());
        ddl.addAll(ClickHouseSchema.entities(schema, new ClickHouseSchema.Config(brokers,
                env("SNAPSHOT_TOPIC", SnapshotCodec.TOPIC), env("CLICKHOUSE_GROUP", ClickHouseSchema.DEFAULT_GROUP))));
        String md = env("MARKETDATA_MODELS_DIR", "/models-trading");
        boolean marketData = !md.isEmpty() && Files.isDirectory(Path.of(md));
        if (marketData) {
            ResolvedModel trading = SchemaMapper.loadModels(Path.of(md));
            ddl.addAll(MarketDataSchema.ddl(trading, brokers));
        } else {
            log.info("no market data models at '{}': skipping ref.* / md.* tables", md);
        }
        ddl.addAll(TradingAnalytics.views(schema, marketData)); // trading_* ⨝ ticks / bars / instruments
        List<ClickHouseSamples.Sample> samples = new ArrayList<>(TradingAnalytics.samples(schema, marketData));
        samples.addAll(ClickHouseSamples.generate(schema, marketData));
        Provisioner.Report r = new Provisioner(ch).apply(ddl, samples);
        log.info("ClickHouse ready: {} objects ({} created, {} recreated, backfilled {})", ddl.size(), r.created().size(),
                r.recreated().size(), r.backfilled());
    }

    static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v == null ? System.getProperty(name, dflt) : v;
    }
}
