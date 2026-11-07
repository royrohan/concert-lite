package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Rendering of declared emissions: source = entity data overlaid with the triggering payload, then the field templates. */
class ChainEmissionsTest {

    private static final StateMachineSpec.Emission E = new StateMachineSpec.Emission(Set.of("FILLED"), "execution", "route",
            "{orderId}-E1", Map.of("execId", "{orderId}-E1", "quantity", "{qty}", "venue", "XNYS"), List.of("order:{orderId}"));

    @Test
    void payloadCopiesBothSourcesAndRendersTemplates() {
        var source = ChainEmissions.source("{\"orderId\":\"O-1\",\"qty\":5,\"status\":\"FILLED\"}", "{\"ts\":\"2026-01-01T00:00:00Z\",\"qty\":7}");
        var p = ChainEmissions.payload(E, source);
        assertEquals("O-1-E1", p.get("execId").asText());
        assertTrue(p.get("quantity").isInt(), "a single placeholder keeps the value's type");
        assertEquals(7, p.get("quantity").asInt(), "the triggering payload overrides the entity's data");
        assertEquals("XNYS", p.get("venue").asText());
        assertEquals("2026-01-01T00:00:00Z", p.get("ts").asText());
        assertEquals("O-1-E1", ChainEmissions.render(E.keyTemplate(), source, E));
    }

    @Test
    void missingValuesAreErrors() {
        var source = ChainEmissions.source("{\"qty\":5}", null);
        assertThrows(IllegalArgumentException.class, () -> ChainEmissions.payload(E, source));
    }

    @Test
    void specEmitsOnEnteringAStateOnly() {
        StateMachineSpec spec = StateMachineSpec.startingAt("NEW").on("NEW", "fill", "FILLED").on("FILLED", "fill", "FILLED")
                .emit(Set.of("FILLED"), "execution", "route", "{orderId}", Map.of(), List.of()).build();
        assertEquals(1, spec.emissionsOf("NEW", "FILLED").size());
        assertEquals(0, spec.emissionsOf("FILLED", "FILLED").size(), "staying in the state doesn't emit again");
        assertThrows(IllegalArgumentException.class, () -> StateMachineSpec.startingAt("NEW").on("NEW", "a", "B")
                .emit(Set.of("NOPE"), "x", "y", "{k}", Map.of(), List.of()).build());
    }
}
