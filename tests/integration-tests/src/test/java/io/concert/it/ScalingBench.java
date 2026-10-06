package io.concert.it;

import io.concert.common.EventEnvelope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Does throughput scale with workers? Each worker gets a fixed capacity
 * ({@value #SLOTS} concurrent transitions x {@value #WORK_MS} ms of simulated side-effect work
 * = ~{@value #CAPACITY}/s), so the worker tier is the intended bottleneck; the run then shows
 * whether adding workers / types moves throughput, and where it saturates next.
 *
 * <p>Key mixes: <b>disjoint</b> (each entity locks only itself) vs <b>shared</b> (every event also
 * locks one of {@value #ACCOUNTS} account keys shared across all three types).
 *
 * <p>Run: {@code ./gradlew :integration-tests:test -Pbench --tests '*ScalingBench'}
 */
@Tag("bench")
class ScalingBench {

    static final int SLOTS = 4;
    static final int WORK_MS = 40;
    static final int CAPACITY = SLOTS * 1000 / WORK_MS;
    static final int ACCOUNTS = 20;
    static final int EVENTS = Integer.getInteger("bench.events", 3000);

    static final Map<String, String[]> FLOWS = Map.of(
            "order", new String[] {"pay", "ship", "deliver"},
            "payment", new String[] {"authorize", "capture", "refund"},
            "shipment", new String[] {"pick", "dispatch", "deliver"});

    record Scenario(String name, List<String> types, int workersPerType, boolean sharedKeys) {}

    @Test
    void workerScalingMatrix() {
        List<Scenario> scenarios = List.of(
                new Scenario("1 type  x 1 worker ", List.of("order"), 1, false),
                new Scenario("3 types x 1 worker ", List.of("order", "payment", "shipment"), 1, false),
                new Scenario("3 types x 2 workers", List.of("order", "payment", "shipment"), 2, false),
                new Scenario("3 types x 1 worker  shared keys", List.of("order", "payment", "shipment"), 1, true),
                new Scenario("3 types x 2 workers shared keys", List.of("order", "payment", "shipment"), 2, true));
        List<String> report = new ArrayList<>();
        String only = System.getProperty("bench.only", "");
        for (int idx = 0; idx < scenarios.size(); idx++) {
            if (!only.isEmpty() && !only.contains(String.valueOf(idx))) {
                continue;
            }
            Scenario s = scenarios.get(idx);
            report.add(String.format("%-34s workers=%d ideal=%5d/s  %s%n%40s%s", s.name(), s.types().size() * s.workersPerType(),
                    s.types().size() * s.workersPerType() * CAPACITY, run(s), "", lastBreakdown));
        }
        System.out.println("\n[scaling] per-worker capacity ~" + CAPACITY + "/s, " + EVENTS + " events per scenario");
        report.forEach(r -> System.out.println("[scaling] " + r));
    }

    private String lastBreakdown = "";

    private String run(Scenario s) {
        try (Harness h = new Harness(4)) {
            h.startCoordinator();
            for (String type : s.types()) {
                for (int w = 0; w < s.workersPerType(); w++) {
                    h.startWorker(type, SLOTS);
                }
            }
            h.startIngest();
            String prefix = "sc" + System.nanoTime() + "-";

            // Warm-up: create the entity and lock workflows so we measure steady state, not cold starts.
            int instances = EVENTS / 3 / s.types().size();
            List<EventEnvelope> events = new ArrayList<>();
            for (int step = 0; step < 3; step++) {
                for (String type : s.types()) {
                    String transition = FLOWS.get(type)[step];
                    for (int i = 0; i < instances; i++) {
                        List<String> keys = s.sharedKeys() ? List.of("account:" + (i % ACCOUNTS)) : List.of();
                        events.add(Harness.event(prefix + type + "-" + i + "-" + step, type, type.charAt(0) + "" + i,
                                transition, keys, "{\"workMs\":" + WORK_MS + "}"));
                    }
                }
            }
            h.publish(events);
            Harness.await(Duration.ofMinutes(10), () -> {
                long done = h.countTrace(prefix, "DONE");
                System.out.printf("[progress] %s received=%d lockWait=%d granted=%d done=%d%n", s.name(),
                        h.countTrace(prefix, "RECEIVED"), h.countTrace(prefix, "LOCK_WAIT"),
                        h.countTrace(prefix, "LOCK_GRANTED"), done);
                return done == events.size();
            });
            lastBreakdown = h.breakdown(prefix);
            return h.stats(prefix).format();
        }
    }
}
