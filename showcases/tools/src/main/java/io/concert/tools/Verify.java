package io.concert.tools;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import io.concert.common.TraceRow;
import io.concert.common.api.SmStateRow;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.HdrHistogram.Histogram;

/**
 * Checks a chaos run against its manifest, through the configured {@link StateStore} (any STORE_KIND):
 *
 * <ul>
 *   <li><b>no loss</b>: every ledger's applied count == events sent to it,
 *   <li><b>no double apply</b>: never more than sent,
 *   <li><b>order</b>: no event applied after a higher seq on the same ledger,
 *   <li><b>dedupe state</b>: every event reached DISPATCHED.
 * </ul>
 *
 * Then prints throughput per 5 s bucket with the chaos actions marked, and producer->done latency.
 */
final class Verify {
    private Verify() {}

    private static final Pattern SRC = Pattern.compile("srcMs=(-?\\d+)");
    private static final DateTimeFormatter HMS = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    record ChaosEvent(long ts, String action, String target) {}

    static int run(Map<String, String> o) throws Exception {
        System.setProperty("STORE_INIT_SCHEMA", "false");
        Manifest m = Json.read(Files.readString(Path.of(o.get("manifest"))), Manifest.class);
        List<ChaosEvent> chaos = readChaos(o.get("chaos-log"));
        int waitSeconds = Integer.parseInt(o.getOrDefault("wait-seconds", "300"));

        try (StateStore store = Stores.fromEnv()) {
            long deadline = System.currentTimeMillis() + waitSeconds * 1000L;
            Map<String, long[]> state;
            long applied;
            while (true) {
                state = ledgerState(store, m.runId());
                applied = state.values().stream().mapToLong(v -> v[0]).sum();
                if (applied >= m.total() || System.currentTimeMillis() > deadline) {
                    break;
                }
                System.out.printf("[verify] draining: applied %d/%d%n", applied, m.total());
                Thread.sleep(5000);
            }

            int lostKeys = 0, overKeys = 0, oooKeys = 0;
            long lost = 0, over = 0, ooo = 0;
            for (int k = 0; k < m.keys(); k++) {
                long[] s = state.getOrDefault("ledger:" + m.runId() + "-K" + k, new long[] {0, 0});
                long expected = m.perKey()[k];
                if (s[0] < expected) {
                    lostKeys++;
                    lost += expected - s[0];
                }
                if (s[0] > expected) {
                    overKeys++;
                    over += s[0] - expected;
                }
                if (s[1] > 0) {
                    oooKeys++;
                    ooo += s[1];
                }
            }
            Map<String, Long> dedupe = store.processedStatusCounts(m.runId() + "-");
            long dropped = store.scanTraces(m.runId() + "-", TraceRow.Stage.DROPPED).size();
            long failed = store.scanTraces(m.runId() + "-", TraceRow.Stage.FAILED).size();

            System.out.println();
            System.out.println("================ chaos verification: " + m.runId() + " (store: " + store.kind() + ") ================");
            System.out.printf("sent %d events to %d ledgers (%d single-key, %d multi-key) at %.0f/s%n", m.total(), m.keys(),
                    (m.keys() + 1) / 2, m.keys() / 2, m.total() * 1000.0 / Math.max(1, m.finishedAt() - m.startedAt()));
            System.out.printf("chaos actions: %d (%s)%n", chaos.stream().filter(c -> !c.action().equals("START")).count(),
                    summarize(chaos));
            System.out.println();
            check("no lost events", lost == 0, lost + " missing across " + lostKeys + " ledgers");
            check("no double-applied events", over == 0, over + " extra across " + overKeys + " ledgers");
            check("per-key order kept", ooo == 0, ooo + " out-of-order applies across " + oooKeys + " ledgers");
            check("dedupe state settled", dedupe.getOrDefault("RECEIVED", 0L) == 0, "status counts " + dedupe);
            System.out.printf("  info  replays absorbed by dedupe (DROPPED rows): %d, FAILED rows: %d%n", dropped, failed);
            System.out.println();
            Map<String, TraceRow> done = firstDone(store, m.runId());
            latency(done);
            timeline(done, m, chaos);
            boolean pass = lost == 0 && over == 0 && ooo == 0;
            System.out.println(pass ? "RESULT: PASS" : "RESULT: FAIL");
            return pass ? 0 : 1;
        }
    }

    private static void check(String what, boolean ok, String detail) {
        System.out.printf("  %s  %s%s%n", ok ? "PASS" : "FAIL", what, ok ? "" : " — " + detail);
    }

    /** workflow_id -> [count, outOfOrder] from the ledger projections. */
    private static Map<String, long[]> ledgerState(StateStore store, String runId) {
        Map<String, long[]> out = new HashMap<>();
        for (SmStateRow r : store.scanStates("ledger:" + runId + "-K")) {
            JsonNode d = Json.read(r.data(), JsonNode.class);
            out.put(r.workflowId(), new long[] {d.path("count").asLong(), d.path("outOfOrder").asLong()});
        }
        return out;
    }

    /** First DONE row per event (a replayed event may have more than one). */
    private static Map<String, TraceRow> firstDone(StateStore store, String runId) {
        Map<String, TraceRow> out = new HashMap<>();
        for (TraceRow t : store.scanTraces(runId + "-", TraceRow.Stage.DONE)) {
            out.merge(t.eventId(), t, (a, b) -> a.tsMillis() <= b.tsMillis() ? a : b);
        }
        return out;
    }

    private static void latency(Map<String, TraceRow> done) {
        Histogram h = new Histogram(TimeUnit.HOURS.toMillis(1), 3);
        for (TraceRow t : done.values()) {
            Matcher mt = SRC.matcher(String.valueOf(t.detail()));
            if (mt.find()) {
                h.recordValue(Math.max(0, Long.parseLong(mt.group(1))));
            }
        }
        if (h.getTotalCount() == 0) {
            System.out.println("producer->done latency: no trace rows (TRACE_SAMPLE=0?)\n");
            return;
        }
        System.out.printf("producer->done latency under chaos: p50=%dms p90=%dms p99=%dms max=%dms (%d traced events)%n%n",
                h.getValueAtPercentile(50), h.getValueAtPercentile(90), h.getValueAtPercentile(99), h.getMaxValue(),
                h.getTotalCount());
    }

    /** Completed events per 5 s, with chaos actions marked in the bucket they happened in. */
    private static void timeline(Map<String, TraceRow> done, Manifest m, List<ChaosEvent> chaos) {
        TreeMap<Long, Long> buckets = new TreeMap<>();
        done.values().forEach(t -> buckets.merge(t.tsMillis() / 5000 * 5000, 1L, Long::sum));
        if (buckets.isEmpty()) {
            return;
        }
        long first = Math.min(buckets.firstKey(), m.startedAt() / 5000 * 5000);
        long last = Math.max(buckets.lastKey(), chaos.isEmpty() ? 0 : chaos.get(chaos.size() - 1).ts() / 5000 * 5000);
        long max = buckets.values().stream().mapToLong(Long::longValue).max().orElse(1);
        System.out.println("completed events per 5s (each # = " + Math.max(1, max / 40) + " events)");
        for (long b = first; b <= last; b += 5000) {
            long n = buckets.getOrDefault(b, 0L);
            long bucket = b;
            StringBuilder marks = new StringBuilder();
            for (ChaosEvent ce : chaos) {
                if (ce.ts() >= bucket && ce.ts() < bucket + 5000) {
                    marks.append(" <- ").append(ce.action()).append(' ').append(ce.target());
                }
            }
            System.out.printf("%s %5d/s %-40s%s%n", HMS.format(Instant.ofEpochMilli(b)), n / 5,
                    "#".repeat((int) (n / Math.max(1, max / 40))), marks);
        }
        System.out.println();
    }

    private static List<ChaosEvent> readChaos(String path) throws Exception {
        List<ChaosEvent> out = new ArrayList<>();
        if (path == null || !Files.exists(Path.of(path))) {
            return out;
        }
        for (String line : Files.readAllLines(Path.of(path))) {
            String[] p = line.trim().split("\\s+", 3);
            if (p.length >= 2 && p[0].matches("\\d+")) {
                out.add(new ChaosEvent(Long.parseLong(p[0]), p[1], p.length > 2 ? p[2] : ""));
            }
        }
        return out;
    }

    private static String summarize(List<ChaosEvent> chaos) {
        Map<String, Integer> byAction = new TreeMap<>();
        chaos.forEach(c -> byAction.merge(c.action(), 1, Integer::sum));
        return byAction.toString();
    }
}
