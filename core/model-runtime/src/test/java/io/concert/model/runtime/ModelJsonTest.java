package io.concert.model.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.SerializationFeature;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ModelJsonTest {

    /** Hand-written stand-in for a generated class: fields only, declared out of alphabetical order. */
    static final class Sample implements ModelObject {
        private String zeta = "z";
        private Long alpha = 1L;
        private BigDecimal amount = new BigDecimal("1E+3");
        private BigDecimal price = new BigDecimal("12.50");
        private Double ratio = 0.25;
        private Instant at = Instant.parse("2024-05-01T10:15:30.120Z");
        private LocalDate on = LocalDate.of(2024, 5, 1);
        private String missing;
        private Map<String, Long> counts = new HashMap<>();
        private List<String> tags = new ArrayList<>();
        private transient int relinks;

        String getMissing() {
            return missing;
        }

        @Override
        public void collectValidationErrors(String path, List<String> errors) {
            if (zeta == null) {
                errors.add(path + ".zeta: required [1]");
            }
        }

        @Override
        public void relink() {
            relinks++;
        }
    }

    private static Sample sample() {
        Sample s = new Sample();
        for (String k : List.of("pear", "apple", "fig", "banana", "cherry")) {
            s.counts.put(k, (long) k.length());
        }
        s.tags.add("b");
        s.tags.add("a");
        return s;
    }

    @Test
    void outputIsDeterministicAndSorted() {
        String json = ModelJson.write(sample());
        assertEquals(json, ModelJson.write(sample()));
        assertEquals("{\"alpha\":1,\"amount\":1000,\"at\":\"2024-05-01T10:15:30.120Z\","
                        + "\"counts\":{\"apple\":5,\"banana\":6,\"cherry\":6,\"fig\":3,\"pear\":4},"
                        + "\"on\":\"2024-05-01\",\"price\":12.50,\"ratio\":0.25,\"tags\":[\"b\",\"a\"],\"zeta\":\"z\"}",
                json);
    }

    @Test
    void readBindsFieldsExactlyAndRelinks() {
        Sample s = ModelJson.read("{\"price\":0.10000000000000000001,\"on\":\"2025-01-31\",\"unknown\":true,"
                + "\"at\":\"2025-01-31T00:00:00Z\",\"zeta\":null}", Sample.class);
        assertEquals(new BigDecimal("0.10000000000000000001"), s.price);
        assertEquals(LocalDate.of(2025, 1, 31), s.on);
        assertEquals(Instant.parse("2025-01-31T00:00:00Z"), s.at);
        assertNull(s.getMissing());
        assertEquals(1, s.relinks);
        assertEquals(List.of("Sample.zeta: required [1]"), s.validationErrors());
        ModelValidationException e = assertThrows(ModelValidationException.class, s::validate);
        assertEquals(List.of("Sample.zeta: required [1]"), e.errors());
    }

    @Test
    void convertRelinksModelObjects() {
        Sample s = ModelJson.convert(Map.of("zeta", "q", "alpha", 7), Sample.class);
        assertEquals("q", s.zeta);
        assertEquals(7L, s.alpha);
        assertEquals(1, s.relinks);
    }

    @Test
    void mapperIsACopy() {
        ModelJson.mapper().enable(SerializationFeature.INDENT_OUTPUT);
        assertTrue(ModelJson.write(sample()).startsWith("{\"alpha\":1,"));
    }
}
