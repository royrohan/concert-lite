package io.concert.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * A single business event as read from a source (Kinesis). It comes in one of two styles:
 *
 * <ul>
 *   <li><b>entity</b> (the default): it targets exactly one state machine instance
 *       ({@code smType:instanceKey}) and is serialized against every key in {@link #effectiveLockKeys()},
 *       which always includes the entity key;
 *   <li><b>event</b> ({@code style = "event"}): it is applied by the handler for its {@code eventType} in the
 *       handler application ("domain") {@code smType}. {@code instanceKey} is the event id, the lock keys are
 *       exactly the declared ones (default {@code <domain>:<eventId>}) and the event is applied by the
 *       processor workflow of its first sorted key, {@code evproc:<domain>:<firstKey>}.
 * </ul>
 *
 * The event-style fields are optional on the wire: a record without them is an entity-style event, so
 * existing producers and histories are unaffected.
 *
 * @param eventId globally unique id; drives dedupe and Temporal update / workflow ids
 * @param smType state machine type, e.g. {@code order} (selects task queue {@code sm-<type>}); for event style
 *     the domain (task queue {@code ev-<domain>})
 * @param instanceKey entity id within the type, e.g. {@code 123}; for event style the event id (filled in when
 *     missing)
 * @param eventType the transition trigger, e.g. {@code pay}; for event style the event type name
 * @param lockKeys extra serialization keys, e.g. {@code account:9}; may be empty
 * @param payload opaque JSON passed to the state machine / handler
 * @param sourceTsMillis producer timestamp
 * @param ingestTsMillis when the ingest activity read the record (0 until ingested)
 * @param style {@value #STYLE_ENTITY} (default, also for {@code null}) or {@value #STYLE_EVENT}
 * @param scheduledAtMillis event style: apply no earlier than this (epoch millis); 0 = now
 * @param parentEventId the event whose handler (or entity transition) emitted this one, if any
 * @param causationRoot the first event of the causation chain this one belongs to, if emitted
 */
public record EventEnvelope(
        String eventId,
        String smType,
        String instanceKey,
        String eventType,
        List<String> lockKeys,
        String payload,
        long sourceTsMillis,
        long ingestTsMillis,
        String style,
        @JsonInclude(JsonInclude.Include.NON_DEFAULT) long scheduledAtMillis,
        @JsonInclude(JsonInclude.Include.NON_NULL) String parentEventId,
        @JsonInclude(JsonInclude.Include.NON_NULL) String causationRoot) {

    public static final String STYLE_ENTITY = "entity";
    public static final String STYLE_EVENT = "event";

    @JsonCreator
    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(smType, "smType");
        Objects.requireNonNull(eventType, "eventType");
        style = style == null || style.isBlank() ? STYLE_ENTITY : style.trim().toLowerCase(java.util.Locale.ROOT);
        if (!style.equals(STYLE_ENTITY) && !style.equals(STYLE_EVENT)) {
            throw new IllegalArgumentException("style must be " + STYLE_ENTITY + " or " + STYLE_EVENT + ": " + style);
        }
        if (instanceKey == null && style.equals(STYLE_EVENT)) {
            instanceKey = eventId;
        }
        Objects.requireNonNull(instanceKey, "instanceKey");
        lockKeys = lockKeys == null ? List.of() : List.copyOf(lockKeys);
    }

    /** An entity-style event (the original eight fields). */
    public EventEnvelope(
            String eventId, String smType, String instanceKey, String eventType, List<String> lockKeys, String payload,
            long sourceTsMillis, long ingestTsMillis) {
        this(eventId, smType, instanceKey, eventType, lockKeys, payload, sourceTsMillis, ingestTsMillis,
                STYLE_ENTITY, 0, null, null);
    }

    /** An event-style event for handler application {@code domain}, due now. */
    public static EventEnvelope event(
            String eventId, String domain, String eventType, List<String> lockKeys, String payload, long sourceTsMillis) {
        return new EventEnvelope(eventId, domain, eventId, eventType, lockKeys, payload, sourceTsMillis, 0,
                STYLE_EVENT, 0, null, null);
    }

    @JsonIgnore
    public boolean isEventStyle() {
        return STYLE_EVENT.equals(style);
    }

    /** Event style: the handler application (= {@code smType}). */
    @JsonIgnore
    public String domain() {
        return smType;
    }

    /** The lock key (and entity workflow id) of the target instance: {@code smType:instanceKey}. */
    @JsonIgnore
    public String entityKey() {
        return WorkflowIds.entityKey(smType, instanceKey);
    }

    /**
     * All keys this event is serialized against, distinct and in global acquisition order. For entity style
     * the target entity key is always included, so events on the same entity never overlap. For event style
     * they are the declared keys only (an added key could sort first and break first-key ordering), or
     * {@code <domain>:<eventId>} when none are declared.
     */
    @JsonIgnore
    public List<String> effectiveLockKeys() {
        TreeSet<String> keys = new TreeSet<>(lockKeys);
        if (isEventStyle()) {
            if (keys.isEmpty()) {
                keys.add(WorkflowIds.entityKey(smType, eventId));
            }
        } else {
            keys.add(entityKey());
        }
        return List.copyOf(keys);
    }

    /** Event style: the key whose processor applies the event (the first sorted lock key). */
    @JsonIgnore
    public String processorKey() {
        return effectiveLockKeys().get(0);
    }

    /**
     * The workflow trace rows point at: the entity workflow, or for event style the processor workflow
     * {@code evproc:<domain>:<firstKey>}.
     */
    @JsonIgnore
    public String traceWorkflowId() {
        return isEventStyle() ? WorkflowIds.processor(smType, processorKey()) : entityKey();
    }

    @JsonIgnore
    public boolean isSingleKey() {
        return effectiveLockKeys().size() == 1;
    }

    public EventEnvelope withIngestTs(long ts) {
        return new EventEnvelope(eventId, smType, instanceKey, eventType, lockKeys, payload, sourceTsMillis, ts,
                style, scheduledAtMillis, parentEventId, causationRoot);
    }

    public EventEnvelope withScheduledAt(long atMillis) {
        return new EventEnvelope(eventId, smType, instanceKey, eventType, lockKeys, payload, sourceTsMillis,
                ingestTsMillis, style, atMillis, parentEventId, causationRoot);
    }

    /** A copy emitted by {@code parent}: parent id set, causation root inherited (or the parent itself). */
    public EventEnvelope withParent(EventEnvelope parent) {
        String root = parent.causationRoot() != null ? parent.causationRoot() : parent.eventId();
        return new EventEnvelope(eventId, smType, instanceKey, eventType, lockKeys, payload, sourceTsMillis,
                ingestTsMillis, style, scheduledAtMillis, parent.eventId(), root);
    }
}
