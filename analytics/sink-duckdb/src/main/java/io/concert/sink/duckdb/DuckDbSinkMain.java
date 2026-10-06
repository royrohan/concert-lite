package io.concert.sink.duckdb;

import io.concert.sink.SchemaMapper;
import io.concert.sink.SchemaSpec;
import io.concert.sink.SnapshotCodec;
import io.concert.sink.SnapshotConsumerLoop;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DuckDB sink: consumes {@code entity-snapshots} into a DuckDB file and serves read-only SQL.
 *
 * <pre>
 * KAFKA_BOOTSTRAP  localhost:29092          SNAPSHOT_TOPIC  entity-snapshots
 * SINK_GROUP_ID    sink-duckdb              DUCKDB_PATH     /data/concert.duckdb
 * MODELS_DIR       /models (the .pure files; several dirs comma-separated, e.g. /models,/models-trading)
 * SINK_ROOTS       order:demo::order::Order,...  (smType:pure::Class; root table = plural(snake(smType)))
 * MODELS_DIR_*, SINK_ROOTS_*   appended to MODELS_DIR / SINK_ROOTS (e.g. MODELS_DIR_INSURANCE from an ecosystem's compose.yml)
 * SINK_HTTP_PORT   8090                     QUERY_TIMEOUT_SECONDS 10
 * SINK_DLQ_TOPIC   entity-snapshots.dlq     SINK_MAX_ATTEMPTS 3 (a record the database rejects, then the DLQ)
 * </pre>
 */
public final class DuckDbSinkMain {
    private static final Logger log = LoggerFactory.getLogger(DuckDbSinkMain.class);

    private DuckDbSinkMain() {}

    public static void main(String[] args) throws Exception {
        String models = SchemaMapper.envList(System.getenv(), "MODELS_DIR", env("MODELS_DIR", "/models"));
        SchemaSpec schema = new SchemaMapper(SchemaMapper.loadModels(models))
                .map(SchemaMapper.parseRoots(SchemaMapper.envList(System.getenv(), "SINK_ROOTS",
                        env("SINK_ROOTS", SchemaMapper.DEFAULT_ROOTS))));
        Path dbFile = Path.of(env("DUCKDB_PATH", "/data/concert.duckdb"));
        if (dbFile.getParent() != null) {
            Files.createDirectories(dbFile.getParent());
        }
        DuckDbTarget db = new DuckDbTarget(dbFile.toString());
        db.ensureSchema(schema);

        SnapshotConsumerLoop loop = new SnapshotConsumerLoop(new SnapshotConsumerLoop.Config(
                env("KAFKA_BOOTSTRAP", "localhost:29092"), env("SNAPSHOT_TOPIC", SnapshotCodec.TOPIC),
                env("SINK_GROUP_ID", "sink-duckdb"), Duration.ofMillis(500), Duration.ofSeconds(30), Map.of(),
                env("SINK_DLQ_TOPIC", SnapshotConsumerLoop.DEFAULT_DLQ),
                Integer.parseInt(env("SINK_MAX_ATTEMPTS", "3"))), db);
        ReadOnlyQueries queries = new ReadOnlyQueries(db, Duration.ofSeconds(Long.parseLong(env("QUERY_TIMEOUT_SECONDS", "10"))));
        int port = Integer.parseInt(env("SINK_HTTP_PORT", "8090"));
        SinkHttpServer http = new SinkHttpServer(db, queries, loop).start(port);
        loop.start();
        log.info("DuckDB sink: {} <- {} (models {}), HTTP on :{}", dbFile, env("KAFKA_BOOTSTRAP", "localhost:29092"), models, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            loop.close(); // finishes the batch in progress
            http.close();
            queries.close();
            db.close();
        }, "sink-shutdown"));
    }

    static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? System.getProperty(name, dflt) : v;
    }
}
