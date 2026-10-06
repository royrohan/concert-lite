package io.concert.store.spanner;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GoogleSQL DDL, applied idempotently (IF NOT EXISTS). */
final class SpannerSchema {
    private SpannerSchema() {}

    private static final Logger log = LoggerFactory.getLogger(SpannerSchema.class);

    static final List<String> DDL = List.of(
            "CREATE TABLE IF NOT EXISTS processed_event ("
                    + " event_id STRING(MAX) NOT NULL, received_at TIMESTAMP NOT NULL, status STRING(16) NOT NULL"
                    + ") PRIMARY KEY (event_id)",
            "CREATE INDEX IF NOT EXISTS processed_event_by_received ON processed_event (received_at)",
            "CREATE TABLE IF NOT EXISTS shard_checkpoint ("
                    + " stream STRING(MAX) NOT NULL, shard_id STRING(MAX) NOT NULL, seq_no STRING(MAX) NOT NULL,"
                    + " updated_at TIMESTAMP NOT NULL) PRIMARY KEY (stream, shard_id)",
            "CREATE TABLE IF NOT EXISTS sm_state ("
                    + " workflow_id STRING(MAX) NOT NULL, sm_type STRING(MAX) NOT NULL, state STRING(MAX) NOT NULL,"
                    + " data STRING(MAX), version INT64 NOT NULL, last_event_id STRING(MAX), updated_at TIMESTAMP NOT NULL"
                    + ") PRIMARY KEY (workflow_id)",
            // Entity data in Spanner's native JSON type. A STRING column can't be altered to JSON in
            // place, so the original `data` column stays for rows written before this existed.
            "ALTER TABLE sm_state ADD COLUMN IF NOT EXISTS data_json JSON",
            "CREATE TABLE IF NOT EXISTS event_trace ("
                    + " event_id STRING(MAX) NOT NULL, trace_id STRING(36) NOT NULL, ts TIMESTAMP NOT NULL,"
                    + " stage STRING(16) NOT NULL, workflow_id STRING(MAX), lock_key STRING(MAX), detail STRING(MAX)"
                    + ") PRIMARY KEY (event_id, trace_id)",
            "CREATE INDEX IF NOT EXISTS event_trace_by_workflow ON event_trace (workflow_id, ts DESC)",
            "CREATE INDEX IF NOT EXISTS event_trace_by_stage ON event_trace (stage, ts DESC)");

    static void ensure(Spanner spanner, DatabaseId db, boolean emulator) {
        try {
            if (emulator) {
                createEmulatorInstance(spanner, db.getInstanceId());
            }
            try {
                spanner.getDatabaseAdminClient()
                        .createDatabase(db.getInstanceId().getInstance(), db.getDatabase(), DDL).get();
                log.info("created Spanner database {}", db);
            } catch (ExecutionException e) {
                if (!alreadyExists(e)) {
                    throw e;
                }
                spanner.getDatabaseAdminClient()
                        .updateDatabaseDdl(db.getInstanceId().getInstance(), db.getDatabase(), DDL, null).get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Spanner schema setup failed: " + e.getCause().getMessage(), e.getCause());
        }
    }

    private static void createEmulatorInstance(Spanner spanner, InstanceId instance) throws InterruptedException, ExecutionException {
        try {
            spanner.getInstanceAdminClient().createInstance(InstanceInfo.newBuilder(instance)
                    .setInstanceConfigId(InstanceConfigId.of(instance.getProject(), "emulator-config"))
                    .setDisplayName(instance.getInstance())
                    .setNodeCount(1)
                    .build()).get();
        } catch (ExecutionException e) {
            if (!alreadyExists(e)) {
                throw e;
            }
        }
    }

    private static boolean alreadyExists(ExecutionException e) {
        return e.getCause() instanceof SpannerException se && se.getErrorCode() == ErrorCode.ALREADY_EXISTS;
    }
}
