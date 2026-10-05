package io.concert.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class Json {
    private Json() {}

    public static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static String write(Object o) {
        try {
            return MAPPER.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> T read(String s, Class<T> type) {
        try {
            return MAPPER.readValue(s, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Bad JSON for " + type.getSimpleName() + ": " + e.getOriginalMessage(), e);
        }
    }

    public static <T> T read(byte[] b, Class<T> type) {
        try {
            return MAPPER.readValue(b, type);
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Bad JSON for " + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }
}
