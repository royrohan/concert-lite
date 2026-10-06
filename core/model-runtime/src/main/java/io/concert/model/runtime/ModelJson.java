package io.concert.model.runtime;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.UncheckedIOException;

/**
 * The JSON form of model objects. Output is deterministic (the same object always yields
 * byte-identical JSON) so it can be used inside Temporal workflow code: properties are bound to
 * fields and sorted by name, map entries are sorted by key, dates are ISO-8601 strings and decimals
 * are written plainly. {@code BigDecimal} fields are read exactly from the JSON text; other floating
 * point values are doubles.
 */
public final class ModelJson {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .visibility(PropertyAccessor.ALL, Visibility.NONE)
            .visibility(PropertyAccessor.FIELD, Visibility.ANY)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .disable(MapperFeature.SORT_CREATOR_PROPERTIES_FIRST)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .defaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
            .addModule(new JavaTimeModule())
            .build();

    private ModelJson() {}

    /** A copy of the configured mapper; changes to it do not affect {@link #write} or {@link #read}. */
    public static ObjectMapper mapper() {
        return MAPPER.copy();
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Parses {@code json} and, if it is a {@link ModelObject}, {@link ModelObject#relink() relinks} the result. */
    public static <T> T read(String json, Class<T> type) {
        try {
            return relinked(MAPPER.readValue(json, type));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts a JSON-shaped value (a {@code JsonNode}, a {@code Map} or another bean) to
     * {@code type}, relinking the result if it is a {@link ModelObject}.
     */
    public static <T> T convert(Object fromJsonTree, Class<T> type) {
        return relinked(MAPPER.convertValue(fromJsonTree, type));
    }

    private static <T> T relinked(T value) {
        if (value instanceof ModelObject m) {
            m.relink();
        }
        return value;
    }
}
