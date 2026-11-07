package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Diagrams of the emitted models: states with payload classes, and the classes doing the transitions. */
class MachineDiagramsTest {

    @SuppressWarnings("unchecked")
    private static Map<String, Object> machine(String smType) {
        List<Map<String, Object>> machines = (List<Map<String, Object>>) MachineDiagrams.all().get("machines");
        return machines.stream().filter(m -> m.get("smType").equals(smType)).findFirst().orElseThrow();
    }

    @Test
    @SuppressWarnings("unchecked")
    void generatedMachineDrawsItsTransitionClasses() {
        Map<String, Object> m = machine("gen_trading_order");
        assertEquals("trading-gen", m.get("catalog"));
        assertTrue(((String) m.get("root")).endsWith(".trading.order.Order"), String.valueOf(m.get("root")));

        String states = (String) m.get("stateMermaid");
        assertTrue(states.startsWith("stateDiagram-v2\n    [*] --> PENDING_NEW\n"), states);
        assertTrue(states.contains("WORKING --> PARTIALLY_FILLED : fill (FillCommand)"), states);
        assertTrue(states.contains("FILLED --> [*]"), states);

        String classes = (String) m.get("transitionMermaid");
        assertTrue(classes.startsWith("classDiagram\n  class Order {\n    <<gen_trading_order>>\n"), classes);
        // Pure types and multiplicities from the model diagram, not Java field types
        assertTrue(classes.contains("[1]"), classes);
        assertTrue(classes.contains("  class FillCommand {"), classes);
        assertTrue(classes.contains("  FillCommand ..> Order : fill / complete_fill"), classes);
        assertTrue(((String) m.get("modelMermaid")).startsWith("classDiagram"));

        List<String> javaClasses = (List<String>) m.get("classes");
        assertEquals(m.get("root"), javaClasses.getFirst());
        assertTrue(javaClasses.stream().anyMatch(c -> c.endsWith(".FillCommand")), javaClasses.toString());
    }

    @Test
    void everyMachineHasDiagramsAndEventCatalogsAreListed() {
        Map<String, Object> all = MachineDiagrams.all();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> machines = (List<Map<String, Object>>) all.get("machines");
        assertFalse(machines.isEmpty());
        for (Map<String, Object> m : machines) {
            assertNotNull(m.get("stateMermaid"), String.valueOf(m.get("smType")));
            assertTrue(((String) m.get("transitionMermaid")).startsWith("classDiagram\n"), String.valueOf(m.get("smType")));
        }
        assertNotNull(all.get("eventCatalogs"));
    }

    @Test
    void classBlocksAreReadFromAModelDiagram() {
        Map<String, String> blocks = MachineDiagrams.classBlocks("""
                classDiagram
                  class Order {
                    +orderId : String[1]
                  }
                  class Side {
                    <<enumeration>>
                    BUY
                  }
                  Order <|-- Sub
                """);
        assertEquals("    +orderId : String[1]\n", blocks.get("Order"));
        assertEquals("    <<enumeration>>\n    BUY\n", blocks.get("Side"));
    }
}
