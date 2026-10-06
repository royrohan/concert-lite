package io.concert.tools;

import io.concert.common.Env;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.orchestration.KinesisClients;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

/**
 * Publishes ledger events at a fixed rate. Every ledger gets a strictly increasing {@code seq}, so
 * the end state is checkable: count == sent and no out-of-order arrivals.
 *
 * <ul>
 *   <li>even ledgers: single-key events;
 *   <li>odd ledgers: multi-key, always with the same account key, so every event of a ledger has the
 *       same first lock key (the platform guarantees order on the first key).
 * </ul>
 *
 * With {@code --style event} it publishes event-style {@code Tick}s of the DemoEvents domain ({@code demo}) instead:
 * the same keys, seqs and account keys ({@code evc:<runId>-K<i>}, odd keys also {@code account:...}), every
 * {@code --chain-every}-th Tick of a key (default 5) with {@code chain = true}, which emits a child event.
 *
 * <p>A key appears at most once per publish call and calls are sequential, so per-key publish order is
 * unambiguous even with PutRecords.
 */
final class LoadGen {
    private LoadGen() {}

    static int run(Map<String, String> o) throws Exception {
        String runId = o.getOrDefault("run-id", "chaos" + System.currentTimeMillis() % 100000);
        int rate = Integer.parseInt(o.getOrDefault("rate", "100"));
        int seconds = Integer.parseInt(o.getOrDefault("seconds", "120"));
        int keys = Integer.parseInt(o.getOrDefault("keys", "200"));
        int accounts = Integer.parseInt(o.getOrDefault("accounts", "20"));
        Path out = Path.of(o.getOrDefault("out", "build/chaos/" + runId + ".json"));
        String stream = Env.get("INGEST_STREAM", "concert-events");
        boolean eventStyle = "event".equals(o.getOrDefault("style", "entity"));
        int chainEvery = Integer.parseInt(o.getOrDefault("chain-every", "5"));

        int ticksPerSec = 10;
        int perTick = Math.min(keys, Math.max(1, rate / ticksPerSec));
        long total = (long) rate * seconds;
        long[] perKey = new long[keys];
        long[] chained = new long[keys];
        long started = System.currentTimeMillis();
        long next = System.nanoTime();
        long sent = 0;
        int cursor = 0;
        try (KinesisClient kinesis = KinesisClients.fromEnv()) {
            while (sent < total) {
                List<PutRecordsRequestEntry> batch = new ArrayList<>(perTick);
                for (int i = 0; i < perTick && sent < total; i++, sent++) {
                    int k = cursor;
                    cursor = (cursor + 1) % keys;
                    List<String> extra = k % 2 == 1 ? List.of("account:" + runId + "-A" + (k / 2) % accounts) : List.of();
                    EventEnvelope e;
                    if (eventStyle) {
                        long seq = perKey[k]++;
                        boolean chain = chainEvery > 0 && seq % chainEvery == chainEvery - 1;
                        if (chain) {
                            chained[k]++;
                        }
                        List<String> locks = new ArrayList<>(extra);
                        locks.add("evc:" + runId + "-K" + k);
                        e = EventEnvelope.event(runId + "-" + sent, "demo", "Tick", locks,
                                "{\"key\":\"" + runId + "-K" + k + "\",\"seq\":" + seq + ",\"chain\":" + chain + "}", System.currentTimeMillis());
                    } else {
                        e = new EventEnvelope(runId + "-" + sent, "ledger", runId + "-K" + k, "append", extra,
                                "{\"seq\":" + perKey[k]++ + "}", System.currentTimeMillis(), 0);
                    }
                    batch.add(PutRecordsRequestEntry.builder()
                            .partitionKey(e.effectiveLockKeys().get(0))
                            .data(SdkBytes.fromUtf8String(Json.write(e)))
                            .build());
                }
                publish(kinesis, stream, batch);
                if (sent % (rate * 10L) < perTick) {
                    System.out.printf("[loadgen] %s sent %d/%d%n", runId, sent, total);
                }
                next += 1_000_000_000L / ticksPerSec;
                long sleep = next - System.nanoTime();
                if (sleep > 0) {
                    Thread.sleep(sleep / 1_000_000, (int) (sleep % 1_000_000));
                }
            }
        }
        Manifest m = new Manifest(runId, keys, accounts, total, perKey, started, System.currentTimeMillis(),
                eventStyle ? "event" : "entity", chained);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, Json.write(m));
        System.out.printf("[loadgen] done: %d events over %d keys, manifest %s%n", total, keys, out);
        return 0;
    }

    private static void publish(KinesisClient kinesis, String stream, List<PutRecordsRequestEntry> batch) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            PutRecordsResponse r = kinesis.putRecords(b -> b.streamName(stream).records(batch));
            if (r.failedRecordCount() == null || r.failedRecordCount() == 0) {
                return;
            }
            // Retrying the whole batch keeps per-key order; dedupe absorbs the re-sent successes.
            if (attempt == 5) {
                throw new IllegalStateException(r.failedRecordCount() + " records failed after retries");
            }
            Thread.sleep(100L * attempt);
        }
    }
}
