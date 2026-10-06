package io.concert.store;

import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** Same semantics as {@link JdbcStateStore}, for unit tests and demos without a database. */
public final class InMemoryStateStore implements StateStore {

    private record Processed(long receivedAt, String status) {}

    private final Map<String, Processed> processed = new ConcurrentHashMap<>();
    private final Map<String, String> checkpoints = new ConcurrentHashMap<>();
    private final Map<String, SmStateRow> states = new ConcurrentHashMap<>();
    private final List<TraceRow> traces = new CopyOnWriteArrayList<>();

    @Override
    public String kind() {
        return "memory";
    }

    @Override
    public List<TraceRow> recentEvents(int limit) {
        Set<TraceRow.Stage> terminal = Set.of(TraceRow.Stage.DONE, TraceRow.Stage.DROPPED, TraceRow.Stage.FAILED, TraceRow.Stage.REJECTED);
        return traces.stream().filter(t -> terminal.contains(t.stage()))
                .sorted(Comparator.comparingLong(TraceRow::tsMillis).reversed()).limit(limit).toList();
    }

    @Override
    public List<SmStateRow> scanStates(String workflowIdPrefix) {
        return states.values().stream().filter(r -> r.workflowId().startsWith(workflowIdPrefix)).toList();
    }

    @Override
    public List<SmStateRow> scanStatesWhere(String workflowIdPrefix, String jsonPath, String value) {
        return scanStates(workflowIdPrefix).stream()
                .filter(r -> value.equals(JsonData.valueAt(r.data(), jsonPath))).toList();
    }

    @Override
    public Map<String, Long> processedStatusCounts(String eventIdPrefix) {
        Map<String, Long> out = new java.util.TreeMap<>();
        processed.forEach((id, p) -> {
            if (id.startsWith(eventIdPrefix)) {
                out.merge(p.status(), 1L, Long::sum);
            }
        });
        return out;
    }

    @Override
    public List<TraceRow> scanTraces(String eventIdPrefix, TraceRow.Stage stage) {
        return traces.stream().filter(t -> t.stage() == stage && t.eventId().startsWith(eventIdPrefix)).toList();
    }

    @Override
    public synchronized Set<String> claimForDispatch(Collection<String> eventIds, long nowMillis) {
        Set<String> out = new HashSet<>();
        for (String id : eventIds) {
            Processed p = processed.putIfAbsent(id, new Processed(nowMillis, "RECEIVED"));
            if (p == null || p.status().equals("RECEIVED")) {
                out.add(id);
            }
        }
        return out;
    }

    @Override
    public void markDispatched(Collection<String> eventIds) {
        eventIds.forEach(id -> processed.computeIfPresent(id, (k, p) -> new Processed(p.receivedAt(), "DISPATCHED")));
    }

    @Override
    public Optional<String> processedStatus(String eventId) {
        return Optional.ofNullable(processed.get(eventId)).map(Processed::status);
    }

    @Override
    public Optional<String> loadCheckpoint(String stream, String shardId) {
        return Optional.ofNullable(checkpoints.get(stream + "/" + shardId));
    }

    @Override
    public void saveCheckpoint(String stream, String shardId, String sequenceNumber) {
        checkpoints.put(stream + "/" + shardId, sequenceNumber);
    }

    @Override
    public void upsertState(SmStateRow row, TraceRow trace) {
        SmStateRow normalized = new SmStateRow(row.workflowId(), row.smType(), row.state(), JsonData.normalize(row.data()),
                row.version(), row.lastEventId(), row.updatedAtMillis());
        states.merge(row.workflowId(), normalized, (old, neu) -> neu.version() > old.version() ? neu : old);
        if (trace != null) {
            traces.add(trace);
        }
    }

    @Override
    public Optional<SmStateRow> loadState(String workflowId) {
        return Optional.ofNullable(states.get(workflowId));
    }

    @Override
    public void appendTrace(List<TraceRow> rows) {
        traces.addAll(rows);
    }

    @Override
    public List<TraceRow> traceForEvent(String eventId) {
        return traces.stream().filter(t -> t.eventId().equals(eventId))
                .sorted(Comparator.comparingLong(TraceRow::tsMillis)).toList();
    }

    @Override
    public List<TraceRow> traceForWorkflow(String workflowId, int limit) {
        List<TraceRow> out = new ArrayList<>(traces.stream().filter(t -> workflowId.equals(t.workflowId()))
                .sorted(Comparator.comparingLong(TraceRow::tsMillis).reversed()).toList());
        return out.subList(0, Math.min(limit, out.size()));
    }

    @Override
    public int sweepProcessed(long olderThanMillis, int batchSize) {
        int before = processed.size();
        processed.values().removeIf(p -> p.receivedAt() < olderThanMillis);
        return before - processed.size();
    }

    @Override
    public void close() {}
}
