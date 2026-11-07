package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The Performance page's sample configurations. */
class PerfOpsTest {

    @Test
    void presetsAreSane() {
        assertEquals(PerfOps.PRESETS.size(), new HashSet<>(PerfOps.PRESETS.stream().map(PerfOps.Preset::id).toList()).size());
        for (PerfOps.Preset p : PerfOps.PRESETS) {
            assertTrue(p.coordinators() >= 1 && p.workers() >= 1 && p.rate() > 0, p.id());
            // compose only passes these through, and the coordinator and workers read them
            assertTrue(DockerCompose.TUNING_ENV.containsAll(p.tuning().keySet()), p.id() + " " + p.tuning());
            p.tuning().values().forEach(v -> assertTrue(v.matches("\\d+"), p.id() + " " + v));
        }
        assertTrue(PerfOps.PRESETS.stream().anyMatch(p -> p.id().equals("balanced")), "the default preset");
    }

    @Test
    void analyticsServicesAreNotFlowServices() {
        FlowRegistry registry = FlowRegistry.load(DashboardMain.repo());
        for (FlowRegistry.Flow f : registry.all()) {
            assertTrue(f.services().stream().noneMatch(PerfOps.ANALYTICS::contains), f.id());
        }
        assertTrue(PerfOps.ANALYTICS.containsAll(List.of("sink-duckdb", "clickhouse", "deephaven")));
    }
}
