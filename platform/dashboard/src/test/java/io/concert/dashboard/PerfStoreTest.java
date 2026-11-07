package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs, samples and timelines round-trip through the DuckDB file, with per-run summaries. */
class PerfStoreTest {

    @TempDir
    Path tmp;

    private static PerfStore.Sample sample(long ts, int t, boolean loading, double completedPerSec, long completed, double p95) {
        return new PerfStore.Sample(ts, t, loading, 5.0, 20.0, 19.0, completedPerSec, completed, 0L, 1L, 300.0, p95, p95 * 2,
                4000.0, 9000.0, 3L, 4, true);
    }

    @Test
    void summarisesAndComparesRuns() throws Exception {
        Path file = tmp.resolve("perf/perf-runs.duckdb");
        try (PerfStore s = new PerfStore(file)) {
            s.startRun("r1", "trading-gen-rs", "rust", "rs_trading_order", "balanced", 2, 2, 5, 300, "serial", true, 20, false,
                    "{}", 1_000_000);
            s.addSample("r1", sample(1_010_000, 10, true, 4.0, 40, 1000));
            s.addSample("r1", sample(1_020_000, 20, true, 5.0, 50, 2000));
            s.addSample("r1", sample(1_030_000, 30, false, 0.5, 5, 500));
            s.addMark("r1", 1_015_000, "CHAOS KILL", "worker-2", "SIGKILL");
            s.addMark("r1", 1_016_000, "START", "worker-2", "after chaos");
            s.endRun("r1", 1_040_000, "completed");
            s.startRun("r2", "trading-gen", "java", "gen_trading_order", "balanced", 2, 2, 5, 300, "direct", false, 20, false,
                    "{\"DIRECT_SM_TYPES\":\"gen_trading_order\"}", 2_000_000);
            s.addSample("r2", sample(2_010_000, 10, true, 6.0, 60, 800));
        }
        // reopened: data persisted, and a run left "running" is now "interrupted"
        try (PerfStore s = new PerfStore(file)) {
            List<Map<String, Object>> runs = s.runs(10);
            assertEquals(List.of("r2", "r1"), runs.stream().map(r -> r.get("run_id")).toList(), "newest first");
            Map<String, Object> r1 = runs.get(1);
            assertEquals("completed", r1.get("status"));
            assertEquals(4.5, ((Number) r1.get("avg_completed_per_sec")).doubleValue(), 1e-9, "loading samples only");
            assertEquals(5.0, ((Number) r1.get("peak_completed_per_sec")).doubleValue(), 1e-9);
            assertEquals(95L, ((Number) r1.get("completed")).longValue(), "consecutive windows add up");
            assertEquals(1500.0, ((Number) r1.get("latency_p95")).doubleValue(), 1e-9);
            assertEquals(1L, ((Number) r1.get("disruptions")).longValue(), "chaos and manual kills/stops, not restarts");
            assertEquals(1_000_000L, ((Number) r1.get("started_at")).longValue());
            assertEquals("interrupted", runs.get(0).get("status"));
            assertEquals("direct", runs.get(0).get("lock_mode"));

            List<Map<String, Object>> cmp = s.compare(List.of("r1", "r2"));
            assertEquals(3, ((List<?>) cmp.get(0).get("series")).size());
            assertEquals(2, ((List<?>) cmp.get(0).get("marks")).size());
            assertEquals("r2", cmp.get(1).get("run_id"));
            assertThrows(IllegalArgumentException.class, () -> s.compare(List.of("nope")));

            assertTrue(s.delete("r2"));
            assertEquals(1, s.runs(10).size());
        }
    }
}
