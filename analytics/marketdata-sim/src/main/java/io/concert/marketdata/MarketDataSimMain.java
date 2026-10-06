package io.concert.marketdata;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Market data simulator: reference data and ticks straight to Kafka (no concert, no Temporal).
 *
 * <pre>
 * MarketDataSimMain [--tick-rate 200] [--seed 42] [--symbols all|N|AAPL,MSFT] [--accounts 20]
 *                   [--vol-mult 4] [--duration-sec 0] [--no-bars] [--dry-run [--dry-run-seconds 65]]
 * </pre>
 *
 * Each option falls back to an env var (TICK_RATE, SEED, SYMBOLS, ACCOUNTS, VOL_MULT, DURATION_SEC,
 * BARS=false); KAFKA_BOOTSTRAP (default {@code localhost:9092}) and TOPIC_REPLICATION (default 1) are
 * env only. {@code --dry-run} prints a sample of each topic to stdout on a virtual clock instead of
 * connecting to Kafka. Use the same seed and vol-mult as the trading generator so fills line up with
 * ticks.
 */
public final class MarketDataSimMain {
    private MarketDataSimMain() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> o = parse(args);
        MarketDataSim.Config config = new MarketDataSim.Config(
                Long.parseLong(opt(o, "seed", "SEED", String.valueOf(PriceModel.DEFAULT_SEED))),
                Double.parseDouble(opt(o, "vol-mult", "VOL_MULT", String.valueOf(PriceModel.DEFAULT_VOL_MULTIPLIER))),
                opt(o, "symbols", "SYMBOLS", "all"),
                Integer.parseInt(opt(o, "accounts", "ACCOUNTS", String.valueOf(MarketUniverse.DEFAULT_ACCOUNTS))),
                Double.parseDouble(opt(o, "tick-rate", "TICK_RATE", "200")),
                !o.containsKey("no-bars") && Boolean.parseBoolean(env("BARS", "true")));
        if (o.containsKey("dry-run")) {
            dryRun(config, Integer.parseInt(o.getOrDefault("dry-run-seconds", "65")));
            return;
        }
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        long durationSec = Long.parseLong(opt(o, "duration-sec", "DURATION_SEC", "0"));
        KafkaRecordSink.ensureTopics(bootstrap, MdTopics.specs(config.bars()),
                Short.parseShort(env("TOPIC_REPLICATION", "1")));
        try (KafkaRecordSink sink = new KafkaRecordSink(bootstrap)) {
            MarketDataSim sim = new MarketDataSim(config, sink);
            sim.publishReference(Instant.now());
            System.out.printf("[marketdata-sim] %s: %d instruments, %d accounts, %d venues; %.0f ticks/s (seed %d)%n",
                    bootstrap, sim.instruments().size(), sim.universe().accounts().size(), sim.universe().venues().size(),
                    config.tickRate(), config.seed());
            long deadline = durationSec > 0 ? System.currentTimeMillis() + durationSec * 1000 : Long.MAX_VALUE;
            long[] nextLog = {System.currentTimeMillis() + 10_000};
            sim.runRealtime(System::currentTimeMillis, () -> {
                long now = System.currentTimeMillis();
                if (now >= nextLog[0]) {
                    nextLog[0] = now + 10_000;
                    System.out.printf("[marketdata-sim] %d ticks, %d bars%n", sim.ticksSent(), sim.barsSent());
                }
                return now < deadline && !Thread.currentThread().isInterrupted();
            });
            System.out.printf("[marketdata-sim] done: %d ticks, %d bars%n", sim.ticksSent(), sim.barsSent());
        }
    }

    private static void dryRun(MarketDataSim.Config config, int seconds) {
        Map<String, Integer> counts = new HashMap<>();
        RecordSink stdout = (topic, key, value) -> {
            int n = counts.merge(topic, 1, Integer::sum);
            if (n <= (topic.equals(MdTopics.TICKS) ? 8 : 2)) {
                System.out.printf("%-16s %-8s %s%n", topic, key, value);
            }
        };
        MarketDataSim sim = new MarketDataSim(config, stdout);
        long start = Instant.parse("2026-10-05T14:30:00Z").toEpochMilli();
        sim.publishReference(Instant.ofEpochMilli(start));
        sim.runVirtual(start, (long) (seconds * config.tickRate()));
        System.out.printf("[dry-run] %ds virtual: %s%n", seconds, new java.util.TreeMap<>(counts));
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

    private static String opt(Map<String, String> o, String key, String envKey, String def) {
        return o.getOrDefault(key, env(envKey, def));
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
