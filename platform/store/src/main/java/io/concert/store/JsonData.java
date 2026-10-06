package io.concert.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;

/**
 * Entity data is stored in each backend's native JSON type, so it must be a JSON document. Handlers
 * written before typed models may store arbitrary text: {@link #normalize} turns such a value into a
 * JSON string literal so every backend accepts it.
 */
public final class JsonData {
    private JsonData() {}

    public static String normalize(String data) {
        if (data == null) {
            return null;
        }
        return parse(data) != null ? data : Json.write(data);
    }

    /** Parsed JSON, or null if the text is not valid JSON. */
    public static JsonNode parse(String data) {
        try {
            return data == null ? null : Json.MAPPER.readTree(data);
        } catch (JsonProcessingException notJson) {
            return null;
        }
    }

    /** The scalar at a dotted path as text, or null; used by stores without native path queries. */
    public static String valueAt(String data, String jsonPath) {
        JsonNode node = parse(data);
        if (node == null) {
            return null;
        }
        for (String part : jsonPath.split("\\.")) {
            node = node.get(part);
            if (node == null) {
                return null;
            }
        }
        return node.isValueNode() ? node.asText() : null;
    }
}
