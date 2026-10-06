package io.concert.trading;

import io.concert.common.Env;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.marketdata.PriceModel;
import io.concert.orchestration.KinesisClients;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

/**
 * Publishes generated trading flows ({@link TradingFlowGenerator}) to the Kinesis ingest stream.
 *
 * <pre>
 * TradingLoadGen [--orders 100] [--rate 50] [--seed 42] [--vol-mult 4] [--symbols all|N|AAPL,MSFT]
 *                [--accounts all|N|ACC-001,...] [--concurrency 20] [--run-id t12345] [--dry-run [--show 40]]
 * </pre>
 *
 * Events are published at their scheduled timestamps ({@code --rate} events/s), so fill prices match
 * the {@code md.ticks} quotes of a {@code marketdata-sim} running with the same seed and vol multiplier.
 * The partition key is the event's first sorted lock key, as in the {@code tools} load generator, and
 * a batch holds at most one record per partition key while batches are sent one after another, so
 * per-key publish order is unambiguous. Env: INGEST_STREAM (default {@code concert-events}),
 * KINESIS_ENDPOINT, AWS_REGION; {@code --orders}, {@code --rate}, {@code --seed} and {@code --vol-mult}
 * fall back to ORDERS, RATE, SEED and VOL_MULT (the compose service; SEED / VOL_MULT as for
 * {@code marketdata-sim}).
 *
 * <p>Time: without {@code --dry-run}, event {@code i} is stamped {@code now + 0.5 s + i / rate} and sent
 * at that wall-clock instant, so payload timestamps (fill {@code ts}, order {@code createdAt}, ...) are
 * live market time, the same clock {@code marketdata-sim} stamps {@code md.ticks} with: as-of joins of
 * fills against live ticks find the quote each fill was priced from.
 *
 * <p>Runs: {@code --run-id} (default {@code t<millis % 100000>}, {@code dry} for {@code --dry-run})
 * prefixes every id and is mixed into the order-flow RNG seed, so repeated runs send different orders
 * (sides, sizes, symbols, venues, fills, cancels, allocations) while prices still come from the shared
 * {@code SEED} / {@code VOL_MULT} price model. Passing the same {@code --run-id} and seed replays a run.
 */
public final class TradingLoadGen {
    private TradingLoadGen() {}

    private static final int MAX_BATCH = 500;

    public static void main(String[] args) throws Exception {
        Map<String, String> o = parse(args);
        envDefault(o, "orders", "ORDERS");
        envDefault(o, "rate", "RATE");
        envDefault(o, "seed", "SEED");
        envDefault(o, "vol-mult", "VOL_MULT");
        boolean dryRun = o.containsKey("dry-run");
        String runId = o.getOrDefault("run-id", dryRun ? "dry" : "t" + System.currentTimeMillis() % 100000);
        long start = dryRun ? java.time.Instant.parse("2026-10-05T14:30:00Z").toEpochMilli() : System.currentTimeMillis() + 500;
        TradingFlowGenerator.Config config = new TradingFlowGenerator.Config(
                Long.parseLong(o.getOrDefault("seed", String.valueOf(PriceModel.DEFAULT_SEED))),
                Double.parseDouble(o.getOrDefault("vol-mult", String.valueOf(PriceModel.DEFAULT_VOL_MULTIPLIER))),
                runId,
                Integer.parseInt(o.getOrDefault("orders", "100")),
                o.getOrDefault("symbols", "all"),
                o.getOrDefault("accounts", "all"),
                Integer.parseInt(o.getOrDefault("concurrency", "20")),
                start,
                Double.parseDouble(o.getOrDefault("rate", "50")),
                0.03, 0.10, 0.40);
        TradingFlowGenerator gen = new TradingFlowGenerator(config);
        if (dryRun) {
            dryRun(gen, Integer.parseInt(o.getOrDefault("show", "40")));
        } else {
            publish(gen, config, Env.get("INGEST_STREAM", "concert-events"));
        }
    }

    private static void dryRun(TradingFlowGenerator gen, int show) {
        Map<String, Integer> counts = new TreeMap<>();
        int n = 0;
        while (gen.hasNext()) {
            EventEnvelope e = gen.next();
            counts.merge(e.smType() + "." + e.eventType(), 1, Integer::sum);
            if (n++ < show) {
                System.out.printf("%-22s %s%n", TradingEvents.partitionKey(e), Json.write(e));
            }
        }
        System.out.printf("[dry-run] %d events: %s%n", n, counts);
    }

    private static void publish(TradingFlowGenerator gen, TradingFlowGenerator.Config config, String stream) throws InterruptedException {
        long sent = 0;
        EventEnvelope carry = null;
        try (KinesisClient kinesis = KinesisClients.fromEnv()) {
            System.out.printf("[trading-loadgen] %s: %d orders at %.0f events/s to %s%n", config.runId(), config.orders(),
                    config.eventsPerSecond(), stream);
            while (carry != null || gen.hasNext()) {
                EventEnvelope first = carry != null ? carry : gen.next();
                carry = null;
                long wait = first.sourceTsMillis() - System.currentTimeMillis();
                if (wait > 0) {
                    Thread.sleep(wait);
                }
                // Everything already due, up to the first repeated partition key.
                List<PutRecordsRequestEntry> batch = new ArrayList<>();
                Set<String> keys = new HashSet<>();
                EventEnvelope e = first;
                while (true) {
                    String key = TradingEvents.partitionKey(e);
                    if (!keys.add(key)) {
                        carry = e;
                        break;
                    }
                    batch.add(PutRecordsRequestEntry.builder().partitionKey(key).data(SdkBytes.fromUtf8String(Json.write(e))).build());
                    if (batch.size() == MAX_BATCH || !gen.hasNext()) {
                        break;
                    }
                    e = gen.next();
                    if (e.sourceTsMillis() > System.currentTimeMillis()) {
                        carry = e;
                        break;
                    }
                }
                put(kinesis, stream, batch);
                long before = sent;
                sent += batch.size();
                if (sent / 1000 != before / 1000) {
                    System.out.printf("[trading-loadgen] %s sent %d events%n", config.runId(), sent);
                }
            }
        }
        System.out.printf("[trading-loadgen] done: %s, %d events for %d orders%n", config.runId(), sent, config.orders());
    }

    private static void put(KinesisClient kinesis, String stream, List<PutRecordsRequestEntry> batch) throws InterruptedException {
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

    /** Fills option {@code key} from env var {@code envKey} when not given on the command line. */
    private static void envDefault(Map<String, String> o, String key, String envKey) {
        String v = System.getenv(envKey);
        if (!o.containsKey(key) && v != null && !v.isBlank()) {
            o.put(key, v.trim());
        }
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> o = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument " + args[i]);
            }
            String k = args[i].substring(2);
            boolean hasValue = i + 1 < args.length && !args[i + 1].startsWith("--");
            o.put(k, hasValue ? args[++i] : "true");
        }
        return o;
    }
}
