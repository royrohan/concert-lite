package io.concert.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * A single business event as read from a source (Kinesis). It targets exactly one state machine
 * instance ({@code smType:instanceKey}) and is serialized against every key in
 * {@link #effectiveLockKeys()}.
 *
 * @param eventId globally unique id; drives dedupe and Temporal update / workflow ids
 * @param smType state machine type, e.g. {@code order}; selects the task queue {@code sm-<type>}
 * @param instanceKey entity id within the type, e.g. {@code 123}
 * @param eventType the transition trigger, e.g. {@code pay}
 * @param lockKeys extra serialization keys, e.g. {@code account:9}; may be empty
 * @param payload opaque JSON passed to the state machine
 * @param sourceTsMillis producer timestamp
 * @param ingestTsMillis when the ingest activity read the record (0 until ingested)
 */
public record EventEnvelope(
        String eventId,
        String smType,
        String instanceKey,
        String eventType,
        List<String> lockKeys,
        String payload,
        long sourceTsMillis,
        long ingestTsMillis) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(smType, "smType");
        Objects.requireNonNull(instanceKey, "instanceKey");
        Objects.requireNonNull(eventType, "eventType");
        lockKeys = lockKeys == null ? List.of() : List.copyOf(lockKeys);
    }

    /** The lock key (and entity workflow id) of the target instance: {@code smType:instanceKey}. */
    @JsonIgnore
    public String entityKey() {
        return WorkflowIds.entityKey(smType, instanceKey);
    }

    /**
     * All keys this event is serialized against, distinct and in global acquisition order. The
     * target entity key is always included, so events on the same entity never overlap.
     */
    @JsonIgnore
    public List<String> effectiveLockKeys() {
        TreeSet<String> keys = new TreeSet<>(lockKeys);
        keys.add(entityKey());
        return List.copyOf(keys);
    }

    @JsonIgnore
    public boolean isSingleKey() {
        return effectiveLockKeys().size() == 1;
    }

    public EventEnvelope withIngestTs(long ts) {
        return new EventEnvelope(eventId, smType, instanceKey, eventType, lockKeys, payload, sourceTsMillis, ts);
    }
}
