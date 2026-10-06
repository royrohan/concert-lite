package io.concert.sink;

import java.time.Instant;
import java.util.Objects;

/**
 * The final state of an entity that reached a terminal state, as published to the
 * {@value SnapshotCodec#TOPIC} topic (key = {@code entityId}). On the wire it is
 * {@code {entityId, smType, state, version, createdAt, completedAt, model}} with the model embedded as
 * a JSON object (see {@link SnapshotCodec}).
 *
 * @param entityId the entity workflow id, {@code smType:instanceKey}; unique across types, so it is
 *     the Kafka key and the sinks' primary key
 * @param version the entity version of the terminal transition; sinks keep the highest version
 * @param createdAt the entity's first accepted transition, {@code null} if unknown
 * @param completedAt the workflow time of the terminal transition
 * @param modelJson the entity data as a JSON document ({@code ModelJson} output), or {@code null}
 */
public record EntitySnapshot(
        String entityId, String smType, String state, long version, Instant createdAt, Instant completedAt,
        String modelJson) {

    public EntitySnapshot {
        Objects.requireNonNull(entityId, "entityId");
        Objects.requireNonNull(smType, "smType");
    }
}
