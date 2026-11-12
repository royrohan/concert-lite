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
    private record Applied(io.concert.common.api.TransitionResult result, long appliedAt) {}
    private record Outbox(String workflowId, io.concert.common.EventEnvelope event, long createdAt, long sentAt) {}
    /** DB-truth: applied events by {@code workflowId + '\0' + eventId}, and outbox rows by event id. */
    private final Map<String, Applied> applied = new ConcurrentHashMap<>();
    private final Map<String, Outbox> outbox = new java.util.concurrent.ConcurrentSkipListMap<>();

    @Override
    public io.concert.common.api.EntityLoad loadEntity(String workflowId, String eventId) {
        Applied a = eventId == null ? null : applied.get(workflowId + "\0" + eventId);
        return new io.concert.common.api.EntityLoad(states.get(workflowId), a == null ? null : a.result());
    }

    @Override
    public synchronized io.concert.common.api.CommitOutcome commitTransition(io.concert.common.api.TransitionCommit t) {
        String key = t.workflowId() + "\0" + t.eventId();
        Applied prior = applied.get(key);
        if (prior != null) {
            return io.concert.common.api.CommitOutcome.alreadyApplied(prior.result());
        }
        SmStateRow current = states.get(t.workflowId());
        long version = current == null ? 0 : current.version();
        if (version != t.expectedVersion()) {
            return io.concert.common.api.CommitOutcome.conflict(current);
        }
        applied.put(key, new Applied(t.result(), t.nowMillis()));
        if (t.result().accepted()) {
            states.put(t.workflowId(), t.row());
            t.outbox().forEach(e -> outbox.putIfAbsent(e.eventId(), new Outbox(t.workflowId(), e, t.nowMillis(), 0)));
        }
        return io.concert.common.api.CommitOutcome.committed(t.result());
    }

    @Override
    public List<io.concert.common.EventEnvelope> pendingOutbox(String workflowIdPrefix, long createdBeforeMillis, int limit) {
        return outbox.values().stream()
                .filter(o -> o.sentAt() == 0 && o.createdAt() < createdBeforeMillis && o.workflowId().startsWith(workflowIdPrefix))
                .sorted(Comparator.comparingLong(Outbox::createdAt)).limit(limit).map(Outbox::event).toList();
    }

    @Override
    public void markOutboxSent(Collection<String> eventIds) {
        long now = System.currentTimeMillis();
        eventIds.forEach(id -> outbox.computeIfPresent(id, (k, o) -> o.sentAt() > 0 ? o
                : new Outbox(o.workflowId(), o.event(), o.createdAt(), now)));
    }

    @Override
    public int sweepEntityLog(long outboxSentBeforeMillis, long appliedBeforeMillis, int batchSize) {
        int before = outbox.size() + applied.size();
        outbox.values().removeIf(o -> o.sentAt() > 0 && o.sentAt() < outboxSentBeforeMillis);
        applied.values().removeIf(a -> a.appliedAt() < appliedBeforeMillis);
        return before - outbox.size() - applied.size();
    }

    /** For tests: whether an outbox event has been marked sent. */
    public boolean outboxSent(String eventId) {
        Outbox o = outbox.get(eventId);
        return o != null && o.sentAt() > 0;
    }

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
    public List<TraceRow> tracesSince(long sinceMillis, int limit) {
        return traces.stream().filter(t -> t.tsMillis() >= sinceMillis)
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
    public long purgeFlow(Set<String> smTypes, List<String> workflowIdPrefixes, String stream) {
        int before = states.size() + traces.size() + checkpoints.size();
        states.values().removeIf(r -> smTypes.contains(r.smType()));
        traces.removeIf(t -> t.workflowId() != null && workflowIdPrefixes.stream().anyMatch(t.workflowId()::startsWith));
        checkpoints.keySet().removeIf(k -> k.startsWith(stream + "/"));
        return before - (states.size() + traces.size() + checkpoints.size());
    }

    @Override
    public void close() {}
}
