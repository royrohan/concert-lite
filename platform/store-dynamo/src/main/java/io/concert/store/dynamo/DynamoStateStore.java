package io.concert.store.dynamo;

import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import com.fasterxml.jackson.databind.JsonNode;
import io.concert.store.JsonData;
import io.concert.store.StateStore;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * DynamoDB backend. Tables (prefix {@code DYNAMO_TABLE_PREFIX}):
 *
 * <ul>
 *   <li>{@code processed_event}: pk event_id; native TTL on {@code expires_at} replaces the sweep.
 *       Claim = conditional put per id ({@code attribute_not_exists}), run in parallel; on conflict
 *       the old item comes back on the same call, so no extra read.
 *   <li>{@code shard_checkpoint}: pk {@code stream#shard}.
 *   <li>{@code sm_state}: pk workflow_id; conditional put {@code version < :v} makes it monotonic.
 *       Entity data is a native map attribute ({@code M}), so nested fields are queryable; JSON that
 *       is not an object (a string or array) is kept as {@code S}.
 *   <li>{@code event_trace}: pk event_id, sk {@code <ts>#<stage>#<uuid>}; sparse GSIs
 *       {@code by_workflow} (entity history) and {@code by_recent} (hour bucket, terminal stages).
 * </ul>
 */
public final class DynamoStateStore implements StateStore {
    private static final Logger log = LoggerFactory.getLogger(DynamoStateStore.class);

    private static final Set<TraceRow.Stage> TERMINAL =
            Set.of(TraceRow.Stage.DONE, TraceRow.Stage.DROPPED, TraceRow.Stage.FAILED, TraceRow.Stage.REJECTED);
    private static final long HOUR = 3_600_000L;

    private final DynamoDbClient ddb;
    private final String processed;
    private final String checkpoints;
    private final String states;
    private final String traces;
    private final long ttlHours;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public DynamoStateStore(DynamoDbClient ddb, String tablePrefix, long ttlHours) {
        this.ddb = ddb;
        this.processed = tablePrefix + "processed_event";
        this.checkpoints = tablePrefix + "shard_checkpoint";
        this.states = tablePrefix + "sm_state";
        this.traces = tablePrefix + "event_trace";
        this.ttlHours = ttlHours;
    }

    @Override
    public String kind() {
        return "dynamo";
    }

    // ---------------------------------------------------------------- dedupe

    @Override
    public Set<String> claimForDispatch(Collection<String> eventIds, long nowMillis) {
        Set<String> claimed = ConcurrentHashMap.newKeySet();
        parallel(new LinkedHashSet<>(eventIds), id -> {
            try {
                ddb.putItem(r -> r.tableName(processed)
                        .item(dedupeItem(id, "RECEIVED", nowMillis))
                        .conditionExpression("attribute_not_exists(event_id)")
                        .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD));
                claimed.add(id);
            } catch (ConditionalCheckFailedException seen) {
                // Seen before: re-dispatch only if a previous attempt never got past RECEIVED.
                Map<String, AttributeValue> old = seen.hasItem() ? seen.item() : null;
                String status = old != null ? s(old, "status") : processedStatus(id).orElse(null);
                if ("RECEIVED".equals(status)) {
                    claimed.add(id);
                }
            }
        });
        return claimed;
    }

    @Override
    public void markDispatched(Collection<String> eventIds) {
        long now = System.currentTimeMillis();
        batchPut(processed, new LinkedHashSet<>(eventIds).stream().map(id -> dedupeItem(id, "DISPATCHED", now)).toList());
    }

    private Map<String, AttributeValue> dedupeItem(String id, String status, long nowMillis) {
        return Map.of(
                "event_id", str(id),
                "status", str(status),
                "received_at", num(nowMillis),
                "expires_at", num(nowMillis / 1000 + ttlHours * 3600));
    }

    @Override
    public Optional<String> processedStatus(String eventId) {
        var item = ddb.getItem(r -> r.tableName(processed).key(Map.of("event_id", str(eventId))).consistentRead(true)).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(s(item, "status"));
    }

    @Override
    public int sweepProcessed(long olderThanMillis, int batchSize) {
        return 0; // native TTL on expires_at
    }

    // ---------------------------------------------------------------- checkpoints

    @Override
    public Optional<String> loadCheckpoint(String stream, String shardId) {
        var item = ddb.getItem(r -> r.tableName(checkpoints).key(Map.of("id", str(stream + "#" + shardId))).consistentRead(true)).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(s(item, "seq_no"));
    }

    @Override
    public void saveCheckpoint(String stream, String shardId, String sequenceNumber) {
        ddb.putItem(r -> r.tableName(checkpoints).item(Map.of(
                "id", str(stream + "#" + shardId),
                "seq_no", str(sequenceNumber),
                "updated_at", num(System.currentTimeMillis()))));
    }

    // ---------------------------------------------------------------- entity state

    @Override
    public void upsertState(SmStateRow row, TraceRow trace) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("workflow_id", str(row.workflowId()));
        item.put("sm_type", str(row.smType()));
        item.put("state", str(row.state()));
        AttributeValue data = dataToAttribute(JsonData.normalize(row.data()));
        if (data != null) {
            item.put("data", data);
        }
        item.put("version", num(row.version()));
        putIfNotNull(item, "last_event_id", row.lastEventId());
        item.put("updated_at", num(row.updatedAtMillis()));
        try {
            ddb.putItem(r -> r.tableName(states).item(item)
                    .conditionExpression("attribute_not_exists(workflow_id) OR version < :v")
                    .expressionAttributeValues(Map.of(":v", num(row.version()))));
        } catch (ConditionalCheckFailedException olderVersion) {
            // A newer version is already stored: this write is a late retry, nothing to do.
        }
        if (trace != null) {
            appendTrace(List.of(trace));
        }
    }

    @Override
    public Optional<SmStateRow> loadState(String workflowId) {
        var item = ddb.getItem(r -> r.tableName(states).key(Map.of("workflow_id", str(workflowId))).consistentRead(true)).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(stateRow(item));
    }

    @Override
    public List<SmStateRow> scanStates(String workflowIdPrefix) {
        List<SmStateRow> out = new ArrayList<>();
        scanPrefix(states, "workflow_id", workflowIdPrefix, null, item -> out.add(stateRow(item)));
        return out;
    }

    @Override
    public List<SmStateRow> scanStatesWhere(String workflowIdPrefix, String jsonPath, String value) {
        Map<String, String> names = new HashMap<>(Map.of("#k", "workflow_id", "#d", "data"));
        StringBuilder path = new StringBuilder("#d");
        String[] parts = jsonPath.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            names.put("#p" + i, parts[i]);
            path.append(".#p").append(i);
        }
        // The JSON scalar may be stored as a string, number or boolean attribute.
        Map<String, AttributeValue> values = new HashMap<>(Map.of(":p", str(workflowIdPrefix), ":s", str(value)));
        List<String> matches = new ArrayList<>(List.of(path + " = :s"));
        if (value.matches("-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?")) {
            values.put(":n", AttributeValue.fromN(value));
            matches.add(path + " = :n");
        }
        if (value.equals("true") || value.equals("false")) {
            values.put(":b", AttributeValue.fromBool(Boolean.parseBoolean(value)));
            matches.add(path + " = :b");
        }
        String filter = "begins_with(#k, :p) AND (" + String.join(" OR ", matches) + ")";
        List<SmStateRow> out = new ArrayList<>();
        Map<String, AttributeValue> start = null;
        do {
            ScanResponse resp = ddb.scan(ScanRequest.builder().tableName(states).filterExpression(filter)
                    .expressionAttributeNames(names).expressionAttributeValues(values)
                    .consistentRead(true).exclusiveStartKey(start).build());
            resp.items().forEach(item -> out.add(stateRow(item)));
            start = resp.hasLastEvaluatedKey() && !resp.lastEvaluatedKey().isEmpty() ? resp.lastEvaluatedKey() : null;
        } while (start != null);
        return out;
    }

    private static SmStateRow stateRow(Map<String, AttributeValue> i) {
        return new SmStateRow(s(i, "workflow_id"), s(i, "sm_type"), s(i, "state"), dataFromAttribute(i.get("data")),
                n(i, "version"), s(i, "last_event_id"), n(i, "updated_at"));
    }

    private static AttributeValue dataToAttribute(String json) {
        if (json == null) {
            return null;
        }
        JsonNode node = JsonData.parse(json);
        if (node != null && node.isObject()) {
            return AttributeValue.fromM(EnhancedDocument.fromJson(json).toMap());
        }
        return str(json);
    }

    private static String dataFromAttribute(AttributeValue v) {
        if (v == null) {
            return null;
        }
        if (v.hasM()) {
            return EnhancedDocument.fromAttributeValueMap(v.m()).toJson();
        }
        return v.s(); // scalar/array JSON, or data written before it became a map
    }

    // ---------------------------------------------------------------- traces

    @Override
    public void appendTrace(List<TraceRow> rows) {
        List<Map<String, AttributeValue>> items = new ArrayList<>(rows.size());
        for (TraceRow t : rows) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("event_id", str(t.eventId()));
            item.put("sk", str(String.format("%013d#%s#%s", t.tsMillis(), t.stage(), UUID.randomUUID().toString().substring(0, 8))));
            item.put("ts", num(t.tsMillis()));
            item.put("stage", str(t.stage().name()));
            putIfNotNull(item, "workflow_id", t.workflowId());
            putIfNotNull(item, "lock_key", t.lockKey());
            putIfNotNull(item, "detail", t.detail());
            if (TERMINAL.contains(t.stage())) {
                item.put("recent_bucket", str(bucket(t.tsMillis())));
            }
            items.add(item);
        }
        batchPut(traces, items);
    }

    @Override
    public List<TraceRow> traceForEvent(String eventId) {
        List<TraceRow> out = new ArrayList<>();
        query(QueryRequest.builder().tableName(traces)
                .keyConditionExpression("event_id = :e")
                .expressionAttributeValues(Map.of(":e", str(eventId)))
                .scanIndexForward(true), 1000, item -> out.add(traceRow(item)));
        return out;
    }

    @Override
    public List<TraceRow> traceForWorkflow(String workflowId, int limit) {
        List<TraceRow> out = new ArrayList<>();
        query(QueryRequest.builder().tableName(traces).indexName("by_workflow")
                .keyConditionExpression("workflow_id = :w")
                .expressionAttributeValues(Map.of(":w", str(workflowId)))
                .scanIndexForward(false), limit, item -> out.add(traceRow(item)));
        return out;
    }

    @Override
    public List<TraceRow> recentEvents(int limit) {
        List<TraceRow> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        // Newest hour bucket first (one ahead, for clock skew), walking back up to two days.
        for (long t = now + HOUR; out.size() < limit && t > now - 48 * HOUR; t -= HOUR) {
            int remaining = limit - out.size();
            query(QueryRequest.builder().tableName(traces).indexName("by_recent")
                    .keyConditionExpression("recent_bucket = :b")
                    .expressionAttributeValues(Map.of(":b", str(bucket(t))))
                    .scanIndexForward(false), remaining, item -> out.add(traceRow(item)));
        }
        return out;
    }

    @Override
    public List<TraceRow> scanTraces(String eventIdPrefix, TraceRow.Stage stage) {
        List<TraceRow> out = new ArrayList<>();
        scanPrefix(traces, "event_id", eventIdPrefix, stage.name(), item -> out.add(traceRow(item)));
        return out;
    }

    @Override
    public Map<String, Long> processedStatusCounts(String eventIdPrefix) {
        Map<String, Long> out = new TreeMap<>();
        scanPrefix(processed, "event_id", eventIdPrefix, null, item -> out.merge(s(item, "status"), 1L, Long::sum));
        return out;
    }

    private static TraceRow traceRow(Map<String, AttributeValue> i) {
        return new TraceRow(s(i, "event_id"), n(i, "ts"), TraceRow.Stage.valueOf(s(i, "stage")),
                s(i, "workflow_id"), s(i, "lock_key"), s(i, "detail"));
    }

    private static String bucket(long tsMillis) {
        return "r#" + tsMillis / HOUR;
    }

    // ---------------------------------------------------------------- plumbing

    /** BatchWriteItem in chunks of 25, chunks in parallel, unprocessed items retried with backoff. */
    private void batchPut(String table, List<Map<String, AttributeValue>> items) {
        List<List<Map<String, AttributeValue>>> chunks = new ArrayList<>();
        for (int i = 0; i < items.size(); i += 25) {
            chunks.add(items.subList(i, Math.min(items.size(), i + 25)));
        }
        parallel(chunks, chunk -> {
            Map<String, List<WriteRequest>> pending = Map.of(table, chunk.stream()
                    .map(it -> WriteRequest.builder().putRequest(PutRequest.builder().item(it).build()).build()).toList());
            for (int attempt = 0; !pending.isEmpty(); attempt++) {
                Map<String, List<WriteRequest>> req = pending;
                pending = ddb.batchWriteItem(r -> r.requestItems(req)).unprocessedItems();
                if (!pending.isEmpty()) {
                    sleep(Math.min(1000, 20L << Math.min(attempt, 6)));
                }
            }
        });
    }

    private void query(QueryRequest.Builder req, int limit, Consumer<Map<String, AttributeValue>> each) {
        Map<String, AttributeValue> start = null;
        int seen = 0;
        do {
            QueryResponse resp = ddb.query(req.exclusiveStartKey(start).limit(Math.min(1000, limit - seen)).build());
            for (var item : resp.items()) {
                each.accept(item);
                if (++seen >= limit) {
                    return;
                }
            }
            start = resp.hasLastEvaluatedKey() && !resp.lastEvaluatedKey().isEmpty() ? resp.lastEvaluatedKey() : null;
        } while (start != null);
    }

    private void scanPrefix(String table, String keyAttr, String prefix, String stage, Consumer<Map<String, AttributeValue>> each) {
        Map<String, String> names = new HashMap<>(Map.of("#k", keyAttr));
        Map<String, AttributeValue> values = new HashMap<>(Map.of(":p", str(prefix)));
        String filter = "begins_with(#k, :p)";
        if (stage != null) {
            filter += " AND #s = :s";
            names.put("#s", "stage");
            values.put(":s", str(stage));
        }
        Map<String, AttributeValue> start = null;
        do {
            ScanResponse resp = ddb.scan(ScanRequest.builder().tableName(table).filterExpression(filter)
                    .expressionAttributeNames(names).expressionAttributeValues(values)
                    .consistentRead(true).exclusiveStartKey(start).build());
            resp.items().forEach(each);
            start = resp.hasLastEvaluatedKey() && !resp.lastEvaluatedKey().isEmpty() ? resp.lastEvaluatedKey() : null;
        } while (start != null);
    }

    private <T> void parallel(Collection<T> items, Consumer<T> task) {
        if (items.size() == 1) {
            task.accept(items.iterator().next());
            return;
        }
        List<Future<?>> fs = new ArrayList<>(items.size());
        for (T item : items) {
            fs.add(pool.submit(() -> task.accept(item)));
        }
        for (Future<?> f : fs) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (ExecutionException e) {
                throw e.getCause() instanceof RuntimeException re ? re : new IllegalStateException(e.getCause());
            }
        }
    }

    // ---------------------------------------------------------------- schema

    /** Creates missing tables (on-demand billing) and enables TTL on dedupe rows. Idempotent. */
    public void createTables() {
        create(processed, List.of(key("event_id", KeyType.HASH)), List.of(attr("event_id")), List.of());
        create(checkpoints, List.of(key("id", KeyType.HASH)), List.of(attr("id")), List.of());
        create(states, List.of(key("workflow_id", KeyType.HASH)), List.of(attr("workflow_id")), List.of());
        create(traces, List.of(key("event_id", KeyType.HASH), key("sk", KeyType.RANGE)),
                List.of(attr("event_id"), attr("sk"), attr("workflow_id"), attr("recent_bucket")),
                List.of(gsi("by_workflow", "workflow_id"), gsi("by_recent", "recent_bucket")));
        try {
            ddb.updateTimeToLive(r -> r.tableName(processed)
                    .timeToLiveSpecification(t -> t.attributeName("expires_at").enabled(true)));
        } catch (RuntimeException e) {
            // Already enabled, or not supported by the emulator.
            log.debug("TTL on {}: {}", processed, e.getMessage());
        }
    }

    private void create(String table, List<KeySchemaElement> keys, List<AttributeDefinition> attrs, List<GlobalSecondaryIndex> gsis) {
        try {
            ddb.describeTable(r -> r.tableName(table));
            return;
        } catch (ResourceNotFoundException missing) {
            // create below
        }
        try {
            ddb.createTable(r -> {
                r.tableName(table).keySchema(keys).attributeDefinitions(attrs).billingMode(BillingMode.PAY_PER_REQUEST);
                if (!gsis.isEmpty()) {
                    r.globalSecondaryIndexes(gsis);
                }
            });
        } catch (ResourceInUseException createdConcurrently) {
            // another process won the race
        }
        ddb.waiter().waitUntilTableExists(r -> r.tableName(table));
        log.info("created DynamoDB table {}", table);
    }

    private static GlobalSecondaryIndex gsi(String name, String hashAttr) {
        return GlobalSecondaryIndex.builder().indexName(name)
                .keySchema(key(hashAttr, KeyType.HASH), key("sk", KeyType.RANGE))
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build();
    }

    private static KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }

    private static AttributeDefinition attr(String name) {
        return AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build();
    }

    private static AttributeValue str(String v) {
        return AttributeValue.fromS(v);
    }

    private static AttributeValue num(long v) {
        return AttributeValue.fromN(Long.toString(v));
    }

    private static void putIfNotNull(Map<String, AttributeValue> item, String name, String v) {
        if (v != null) {
            item.put(name, str(v));
        }
    }

    private static String s(Map<String, AttributeValue> item, String name) {
        AttributeValue v = item.get(name);
        return v == null ? null : v.s();
    }

    private static long n(Map<String, AttributeValue> item, String name) {
        AttributeValue v = item.get(name);
        return v == null ? 0 : Long.parseLong(v.n());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        pool.close();
        ddb.close();
    }
}
