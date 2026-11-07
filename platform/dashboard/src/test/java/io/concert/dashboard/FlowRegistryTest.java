package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Flows are ecosystems, each with its own services, stream, topic, queues and data scope. */
class FlowRegistryTest {

    private final FlowRegistry registry = FlowRegistry.load(DashboardMain.repo());

    @Test
    void ecosystemsAreTheFlows() {
        List<String> ids = registry.all().stream().map(FlowRegistry.Flow::id).toList();
        assertTrue(ids.containsAll(List.of("samples", "trading", "insurance", "trading-gen", "order-events")), ids.toString());
        assertEquals("samples", FlowRegistry.ecosystemOf("demo"));
        assertEquals("order-events", FlowRegistry.ecosystemOf("order-events/orders"));
    }

    @Test
    void aFlowOwnsItsInfrastructure() {
        FlowRegistry.Flow f = registry.get("insurance");
        assertEquals(List.of("insurance-coordinator", "insurance-worker"), f.services());
        assertEquals("showcases/insurance/compose.yml", f.composeFile());
        assertEquals("concert-insurance", f.stream());
        assertEquals("snapshots.insurance", f.topic());
        assertEquals("orchestration-insurance", f.orchestrationQueue());
        assertTrue(f.taskQueues().containsAll(List.of("sm-claim", "sm-policy", "sm-payout", "orchestration-insurance")));
        assertTrue(f.workflowPrefixes().containsAll(List.of("claim:", "lock:insurance:")));
        assertEquals(f, registry.ofMachineFlow("claim"));
        assertEquals("concert-insurance", registry.streamOf("claim"));
    }

    @Test
    void builtInAndEventFlows() {
        FlowRegistry.Flow samples = registry.get("samples");
        assertEquals(List.of("samples-coordinator", "worker"), samples.services());
        assertNull(samples.composeFile());
        assertTrue(samples.machineFlows().containsAll(List.of("order", "payment")), samples.machineFlows().toString());

        FlowRegistry.Flow orders = registry.get("order-events");
        assertTrue(orders.machineFlows().containsAll(List.of("ev:orders", "ev:inventory")), orders.machineFlows().toString());
        assertTrue(orders.smTypes().stream().anyMatch(t -> t.startsWith("evt_")), orders.smTypes().toString());
        assertTrue(orders.smTypes().stream().anyMatch(t -> t.startsWith("st_")), orders.smTypes().toString());
        assertTrue(orders.services().contains("order-events-event-worker"), orders.services().toString());
        assertTrue(registry.composeFiles().contains("showcases/order-events/compose.yml"));
    }
}
