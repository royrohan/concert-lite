package io.concert.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

/**
 * The Kafka encoding of {@link EntitySnapshot}: UTF-8 JSON
 * {@code {"entityId","smType","state","version","createdAt","completedAt","model"}} with ISO-8601
 * instants and the model embedded as an object (not a string), so tools that understand JSON
 * (Redpanda Console, ClickHouse, Deephaven) see its structure. Headers {@value #H_SM_TYPE},
 * {@value #H_VERSION} and {@value #H_SCHEMA_HASH} let consumers route or filter without parsing.
 */
public final class SnapshotCodec {

    public static final String TOPIC = "entity-snapshots";
    public static final String H_SM_TYPE = "smType";
    public static final String H_VERSION = "version";
    public static final String H_SCHEMA_HASH = "schemaHash";

    /**
     * Reads decimals exactly (keeping their scale, e.g. {@code 25.00}) and writes them plainly, so the
     * model passes through unchanged.
     */
    static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .build();

    private SnapshotCodec() {}

    public static byte[] encode(EntitySnapshot s) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("entityId", s.entityId());
        n.put("smType", s.smType());
        n.put("state", s.state());
        n.put("version", s.version());
        n.put("createdAt", s.createdAt() == null ? null : s.createdAt().toString());
        n.put("completedAt", s.completedAt() == null ? null : s.completedAt().toString());
        try {
            n.set("model", s.modelJson() == null ? null : MAPPER.readTree(s.modelJson()));
            return MAPPER.writeValueAsBytes(n);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("snapshot of " + s.entityId() + " has invalid model JSON", e);
        }
    }

    /** @throws IllegalArgumentException if {@code bytes} is not a snapshot */
    public static EntitySnapshot decode(byte[] bytes) {
        JsonNode n;
        try {
            n = MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new IllegalArgumentException("not JSON: " + e.getMessage(), e);
        }
        if (n == null || !n.isObject() || !n.hasNonNull("entityId") || !n.hasNonNull("smType")) {
            throw new IllegalArgumentException("not an entity snapshot: entityId and smType are required");
        }
        JsonNode model = n.get("model");
        try {
            return new EntitySnapshot(n.get("entityId").asText(), n.get("smType").asText(), text(n, "state"),
                    n.path("version").asLong(), instant(n, "createdAt"), instant(n, "completedAt"),
                    model == null || model.isNull() ? null : MAPPER.writeValueAsString(model));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A short stable hash (first 16 hex chars of SHA-256) of a model description, for the schemaHash header. */
    public static String schemaHash(String modelSource) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(modelSource.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static Instant instant(JsonNode n, String field) {
        String v = text(n, field);
        return v == null ? null : Instant.parse(v);
    }
}
