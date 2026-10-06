package io.concert.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.Json;
import java.util.List;
import org.junit.jupiter.api.Test;

class LockTemplatesTest {

    @Test
    void rendersPayloadFieldsAndTheInstanceKey() throws Exception {
        var payload = Json.MAPPER.readTree("{\"policyId\":\"P-7\",\"n\":3,\"nested\":{\"x\":1}}");
        assertEquals(List.of("policy:P-7", "claim:C-1", "mix:P-7/3"),
                LockTemplates.render(List.of("policy:{policyId}", "claim:{id}", "mix:{policyId}/{ n }"), "C-1", payload));
        assertEquals(List.of("policyId", "id"), LockTemplates.placeholders("a:{policyId}-{id}"));
        assertThrows(IllegalArgumentException.class, () -> LockTemplates.render(List.of("x:{missing}"), "k", payload));
        assertThrows(IllegalArgumentException.class, () -> LockTemplates.render(List.of("x:{nested}"), "k", payload));
        assertThrows(IllegalArgumentException.class, () -> LockTemplates.render(List.of("x:{policyId}"), "k", null));
        assertEquals(List.of(), LockTemplates.render(List.of(), "k", null));
    }

    @Test
    void declaredTerminalStatesOverrideTheDerivedOnes() {
        StateMachineSpec derived = StateMachineSpec.startingAt("A").on("A", "go", "B").on("B", "back", "A").on("B", "end", "C").build();
        assertEquals(List.of("C"), List.copyOf(derived.terminalStates()));
        StateMachineSpec declared = StateMachineSpec.startingAt("A").on("A", "go", "B").on("B", "back", "A").on("B", "end", "C")
                .terminal("B", "C").build();
        assertTrue(declared.isTerminal("B"));
        assertTrue(declared.isTerminal("C"));
        assertFalse(declared.isTerminal("A"));
        assertThrows(IllegalArgumentException.class, () -> StateMachineSpec.startingAt("A").on("A", "go", "B").terminal("Z").build());
    }
}
