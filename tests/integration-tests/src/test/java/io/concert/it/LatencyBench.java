package io.concert.it;

import io.concert.common.EventEnvelope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Steady-rate latency sweep. Events spread over {@value #KEYS} warm keys, so there is no per-key
 * queueing: this measures the platform's own per-event cost, broken down per pipeline section.
 *
 * <p>Run: {@code ./gradlew :integration-tests:test -Pbench --tests '*LatencyBench' -Dbench.rates=100,500,1000}
 */
@Tag("bench")
class LatencyBench {

    static final int KEYS = 2000;
    static final int SECONDS = Integer.getInteger("bench.seconds", 15);
    static final int[] RATES = Arrays.stream(System.getProperty("bench.rates", "100,250,500,1000").split(","))
            .mapToInt(Integer::parseInt).toArray();

    private static List<String> extraKeys(int k, boolean multiKey) {
        return multiKey ? List.of("account:" + (k % (KEYS / 2))) : List.of();
    }

    private static void runAtRate(Harness h, String prefix, int rate, int total, boolean multiKey) throws InterruptedException {
        int ticksPerSec = 20;
        int perTick = Math.max(1, rate / ticksPerSec);
        long intervalNanos = 1_000_000_000L / ticksPerSec;
        int sent = 0;
        long next = System.nanoTime();
        while (sent < total) {
            List<EventEnvelope> tick = new ArrayList<>(perTick);
            for (int i = 0; i < perTick && sent < total; i++, sent++) {
                int k = sent % KEYS;
                tick.add(Harness.event(prefix + sent, "ledger", "K" + k, "append", extraKeys(k, multiKey), "{\"seq\":" + sent + "}"));
            }
            h.publish(tick);
            next += intervalNanos;
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                Thread.sleep(sleep / 1_000_000, (int) (sleep % 1_000_000));
            }
        }
        int expected = total;
        Harness.await(Duration.ofSeconds(SECONDS * 6L + 120), () -> h.countTrace(prefix, "DONE") == expected);
    }

    @Test
    void steadyRateSweep() throws Exception {
        List<String> report = new ArrayList<>();
        try (Harness h = new Harness(4)) {
            h.startCoordinator();
            h.startWorker("ledger");
            h.startIngest();

            // Warm every entity, lock and account key once, plus JIT and connection pools.
            runAtRate(h, "warm1-", 500, KEYS, false);
            runAtRate(h, "warmN-", 500, KEYS, true);

            for (int rate : RATES) {
                for (boolean multi : new boolean[] {false, true}) {
                    String prefix = (multi ? "N" : "S") + rate + "-";
                    runAtRate(h, prefix, rate, rate * SECONDS, multi);
                    Harness.Stats st = h.stats(prefix);
                    report.add(String.format("%-10s @%5d/s offered  achieved=%4.0f/s  e2e(ingest->done) p50=%3dms p99=%4dms  producer->done p50=%3dms p99=%4dms%n             %s",
                            multi ? "multi-key" : "single-key", rate, st.throughputPerSec(),
                            st.e2e().getValueAtPercentile(50), st.e2e().getValueAtPercentile(99),
                            st.fromSource().getValueAtPercentile(50), st.fromSource().getValueAtPercentile(99),
                            h.breakdown(prefix)));
                    System.out.println("[latency-progress] " + report.get(report.size() - 1));
                }
            }
        }
        System.out.println();
        report.forEach(r -> System.out.println("[latency] " + r));
    }
}
