package io.concert.eco.payments;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Env;
import io.concert.common.EventEnvelope;
import io.concert.common.Flows;
import io.concert.common.Json;
import io.concert.common.api.SmStateRow;
import io.concert.orchestration.KinesisClients;
import io.concert.store.StateStore;
import io.concert.store.Stores;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * Sends payments through the deployed flow, waits until each has an outcome, then checks every payment against both
 * external systems:
 * <ul>
 *   <li>SETTLED: exactly one live charge in the ledger, a settlement record in Kafka, no reversal;
 *   <li>DECLINED: nothing committed in the ledger, no settlement record (only charges over the limit may be declined);
 *   <li>FAILED: no live charge left (refunded), and a reversal for any settlement record that was written;
 *   <li>every payment has an outcome (none stuck in NEW or SETTLING);
 *   <li>at most one ledger commit per payment.
 * </ul>
 * Kafka records may repeat (at-least-once); they are counted, and the check is on distinct idempotency keys.
 *
 * <pre>
 * payments-chaos --scenario NAME [--count 40] [--rate 4] [--decline-every 10] [--timeout 240] [--expect-violations]
 * </pre>
 * Environment: {@code KINESIS_ENDPOINT}, {@code LEDGER_URL}, {@code KAFKA_BOOTSTRAP}, {@code STORE_*}. The last line is
 * {@code RESULT key=value ...} for scripts; the exit code is 0 when the verdict is as expected.
 */
public final class PaymentsChaosCheck {
    private static final Set<String> TERMINAL = Set.of("SETTLED", "DECLINED", "FAILED", "CANCELLED");

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--expect-violations")) {
                a.put("expect-violations", "true");
            } else if (args[i].startsWith("--") && i + 1 < args.length) {
                a.put(args[i].substring(2), args[++i]);
            }
        }
        String scenario = a.getOrDefault("scenario", "manual");
        int count = Integer.parseInt(a.getOrDefault("count", "40"));
        double rate = Double.parseDouble(a.getOrDefault("rate", "4"));
        int declineEvery = Integer.parseInt(a.getOrDefault("decline-every", "10"));
        int timeoutSec = Integer.parseInt(a.getOrDefault("timeout", "240"));
        boolean expectViolations = a.containsKey("expect-violations");
        String prefix = "PAY-" + scenario + "-" + Long.toString(System.currentTimeMillis() % 1_000_000, 36) + "-";

        System.out.printf("== %s: %d payments (%s*), %.1f/s, every %d-th over the limit%n", scenario, count, prefix, rate, declineEvery);
        Map<String, Boolean> overLimit = send(prefix, count, rate, declineEvery);
        Map<String, JsonNode> payments = await(prefix, count, timeoutSec);
        Map<String, JsonNode> ledger = ledger(prefix);
        Map<String, List<String>> kafka = kafka(prefix); // payment id -> record types in order (with repeats)
        boolean ok = verify(scenario, overLimit, payments, ledger, kafka, expectViolations);
        System.exit(ok ? 0 : 1);
    }

    private static Map<String, Boolean> send(String prefix, int count, double rate, int declineEvery) throws InterruptedException {
        Map<String, Boolean> overLimit = new LinkedHashMap<>();
        long gapMs = (long) (1000 / rate);
        try (KinesisClient kinesis = KinesisClients.fromEnv()) {
            for (int i = 1; i <= count; i++) {
                String id = prefix + i;
                boolean over = declineEvery > 0 && i % declineEvery == 0;
                BigDecimal amount = over ? new BigDecimal("25000.00") : BigDecimal.valueOf(1000 + i * 37L, 2);
                String payload = Json.write(Map.of("accountId", "ACC-" + (i % 7), "amount", amount, "currency", "EUR",
                        "memo", "chaos " + prefix + i));
                EventEnvelope e = PaymentsEvents.envelope("settlement", "settle", id, payload);
                kinesis.putRecord(b -> b.streamName(Flows.stream("payments")).partitionKey(e.effectiveLockKeys().getFirst())
                        .data(SdkBytes.fromUtf8String(Json.write(e))));
                overLimit.put(id, over);
                Thread.sleep(gapMs);
            }
        }
        return overLimit;
    }

    private static Map<String, JsonNode> await(String prefix, int count, int timeoutSec) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        Map<String, JsonNode> out = new LinkedHashMap<>();
        try (StateStore store = Stores.fromEnv()) {
            long lastPrint = 0;
            while (true) {
                out.clear();
                for (SmStateRow row : store.scanStates("settlement:" + prefix)) {
                    // a scan row may come without data: read it on its own then
                    String data = row.data() != null ? row.data()
                            : store.loadState(row.workflowId()).map(SmStateRow::data).orElse(null);
                    JsonNode d = Json.MAPPER.readTree(data == null ? "{}" : data);
                    ((com.fasterxml.jackson.databind.node.ObjectNode) d).put("_state", row.state());
                    out.put(row.workflowId().substring("settlement:".length()), d);
                }
                long done = out.values().stream().filter(d -> TERMINAL.contains(d.path("_state").asText())).count();
                if (done == count || System.currentTimeMillis() > deadline) {
                    return out;
                }
                if (System.currentTimeMillis() - lastPrint > 10_000) {
                    lastPrint = System.currentTimeMillis();
                    System.out.printf("   %d/%d with an outcome: %s%n", done, count, out.values().stream()
                            .collect(Collectors.groupingBy(d -> d.path("_state").asText(), java.util.TreeMap::new, Collectors.counting())));
                }
                Thread.sleep(1000);
            }
        }
    }

    private static Map<String, JsonNode> ledger(String prefix) throws Exception {
        String url = Env.get("LEDGER_URL", "http://localhost:8097") + "/charges?prefix=" + prefix;
        HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).build(),
                HttpResponse.BodyHandlers.ofString());
        Map<String, JsonNode> out = new HashMap<>();
        Json.MAPPER.readTree(r.body()).forEach(n -> out.put(n.path("paymentId").asText(), n));
        return out;
    }

    private static Map<String, List<String>> kafka(String prefix) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.get("KAFKA_BOOTSTRAP", "localhost:29092"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        Map<String, List<String>> out = new HashMap<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> parts = c.partitionsFor(SettlementMachine.TOPIC, Duration.ofSeconds(10)).stream()
                    .map(i -> new TopicPartition(i.topic(), i.partition())).toList();
            c.assign(parts);
            c.seekToBeginning(parts);
            Map<TopicPartition, Long> end = c.endOffsets(parts);
            long deadline = System.currentTimeMillis() + 30_000;
            while (parts.stream().anyMatch(tp -> c.position(tp) < end.get(tp)) && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> rec : c.poll(Duration.ofMillis(500))) {
                    if (rec.key() != null && rec.key().startsWith(prefix)) {
                        Header type = rec.headers().lastHeader("type");
                        out.computeIfAbsent(rec.key(), k -> new ArrayList<>())
                                .add(type == null ? "?" : new String(type.value(), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return out;
    }

    private static boolean verify(String scenario, Map<String, Boolean> overLimit, Map<String, JsonNode> payments,
            Map<String, JsonNode> ledger, Map<String, List<String>> kafka, boolean expectViolations) {
        List<String> violations = new ArrayList<>();
        Map<String, Integer> states = new java.util.TreeMap<>();
        long dupCommits = 0, replays = 0, faults = 0, ledgerRetries = 0, kafkaRetries = 0, kafkaRecords = 0, kafkaDistinct = 0;
        for (var e : overLimit.entrySet()) {
            String id = e.getKey();
            JsonNode p = payments.get(id);
            String state = p == null ? "MISSING" : p.path("_state").asText();
            states.merge(state, 1, Integer::sum);
            JsonNode l = ledger.get(id);
            long commits = l == null ? 0 : l.path("chargeCommits").asLong();
            long live = 0;
            if (l != null) {
                for (JsonNode en : l.path("entries")) {
                    if (en.path("status").asText().equals("CHARGED")) {
                        live++;
                    }
                }
                replays += l.path("replays").asLong();
                faults += l.path("faults").asLong();
            }
            dupCommits += Math.max(0, commits - 1);
            List<String> recs = kafka.getOrDefault(id, List.of());
            kafkaRecords += recs.size();
            Set<String> types = new HashSet<>(recs);
            kafkaDistinct += types.size();
            if (p != null) {
                ledgerRetries += Math.max(0, p.path("ledgerAttempts").asInt() - 1);
                kafkaRetries += Math.max(0, p.path("kafkaAttempts").asInt() - 1);
            }
            if (commits > 1) {
                violations.add(id + ": charged " + commits + " times");
            }
            switch (state) {
                case "SETTLED" -> {
                    if (live != 1) violations.add(id + ": SETTLED but " + live + " live charges in the ledger");
                    if (!types.contains("SETTLED")) violations.add(id + ": SETTLED but no settlement record in Kafka");
                    if (types.contains("REVERSED")) violations.add(id + ": SETTLED but reversed in Kafka");
                    if (e.getValue()) violations.add(id + ": over the limit but SETTLED");
                }
                case "DECLINED" -> {
                    if (commits > 0) violations.add(id + ": DECLINED but charged");
                    if (types.contains("SETTLED")) violations.add(id + ": DECLINED but a settlement record exists");
                    if (!e.getValue()) violations.add(id + ": within the limit but DECLINED");
                }
                case "FAILED" -> {
                    if (live > 0) violations.add(id + ": FAILED but a charge was not refunded");
                    if (types.contains("SETTLED") && !types.contains("REVERSED")) {
                        violations.add(id + ": FAILED with a settlement record and no reversal");
                    }
                }
                default -> violations.add(id + ": no outcome (" + state + ")");
            }
        }
        System.out.println();
        System.out.printf("   outcomes           %s%n", states);
        System.out.printf("   ledger             %d duplicate commits, %d repeats answered from the idempotency store, %d faults injected%n",
                dupCommits, replays, faults);
        System.out.printf("   kafka              %d records for %d distinct (payment, type): %d repeats, dropped by key on read%n",
                kafkaRecords, kafkaDistinct, kafkaRecords - kafkaDistinct);
        System.out.printf("   temporal retries   %d ledger, %d kafka (attempts beyond the first, from the payments' data)%n",
                ledgerRetries, kafkaRetries);
        violations.stream().limit(15).forEach(v -> System.out.println("   VIOLATION " + v));
        if (violations.size() > 15) {
            System.out.println("   ... " + (violations.size() - 15) + " more");
        }
        boolean clean = violations.isEmpty();
        String verdict = clean == !expectViolations ? "PASS" : "FAIL";
        System.out.printf("   %s%s%n", verdict, expectViolations
                ? (clean ? ": expected violations (control run) but found none" : ": violations found, as expected for this control run")
                : (clean ? ": every payment is consistent with the ledger and Kafka" : ": " + violations.size() + " violations"));
        System.out.printf("RESULT scenario=%s payments=%d settled=%d declined=%d failed=%d stuck=%d dupCommits=%d kafkaRecords=%d "
                + "kafkaDistinct=%d replays=%d faults=%d ledgerRetries=%d kafkaRetries=%d violations=%d verdict=%s%n",
                scenario, overLimit.size(), states.getOrDefault("SETTLED", 0), states.getOrDefault("DECLINED", 0),
                states.getOrDefault("FAILED", 0), overLimit.size() - states.getOrDefault("SETTLED", 0) - states.getOrDefault("DECLINED", 0)
                        - states.getOrDefault("FAILED", 0), dupCommits, kafkaRecords, kafkaDistinct, replays, faults,
                ledgerRetries, kafkaRetries, violations.size(), verdict);
        return verdict.equals("PASS");
    }
}
