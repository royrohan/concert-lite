package io.concert.eco.payments.external;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.concert.common.Env;
import io.concert.common.Json;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sample ledger REST service (an "external system" for the payments worker), with fault injection.
 *
 * <pre>
 * POST /charges   {paymentId, accountId, amount, currency}  header Idempotency-Key   201 {ledgerRef, status} | 422 {error}
 * POST /refunds   {paymentId}                               header Idempotency-Key   200 {ledgerRef, status}
 * GET  /charges?prefix=PAY-   what was committed, per payment (the chaos verifier reads it)
 * GET  /chaos, POST /chaos {mode, rate, idempotency, kafkaAckLossRate}
 * GET  /health
 * </pre>
 *
 * Charges above 10 000 are declined (422, nothing committed). With {@code idempotency} on (the default) a key is committed
 * once and every repeat gets the stored answer with {@code Idempotent-Replayed: true}; off, every request commits again
 * (the control experiment: what retries do without idempotency keys).
 *
 * <p>Chaos modes, applied to {@code rate} of the requests:
 * <ul>
 *   <li>{@code commit-then-500}: commits, then answers 500;
 *   <li>{@code commit-then-timeout}: commits, then answers after 8 s (the client gives up after 4 s);
 *   <li>{@code commit-then-drop}: commits, then closes the connection without an answer;
 *   <li>{@code fail-before-commit}: answers 503 without committing.
 * </ul>
 * State is in memory: restarting the service forgets it.
 */
public final class LedgerService {
    static final BigDecimal LIMIT = new BigDecimal("10000");

    /** A committed entry: a charge (CHARGED, then REFUNDED) or a declined charge. */
    static final class Entry {
        final String key, paymentId, ledgerRef;
        volatile String status;
        Entry(String key, String paymentId, String ledgerRef, String status) {
            this.key = key; this.paymentId = paymentId; this.ledgerRef = ledgerRef; this.status = status;
        }
    }

    /** Per payment: requests received, entries committed, repeats answered from the store, refunds done. */
    static final class Stats {
        final AtomicLong chargeRequests = new AtomicLong(), chargeCommits = new AtomicLong(), replays = new AtomicLong(),
                declines = new AtomicLong(), refundRequests = new AtomicLong(), refunds = new AtomicLong(),
                faults = new AtomicLong();
    }

    private final Map<String, List<Entry>> byPayment = new ConcurrentHashMap<>();
    private final Map<String, Stats> stats = new ConcurrentHashMap<>();
    /** Answers stored per idempotency key: status code and body. */
    private final Map<String, Object[]> answers = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();
    private volatile String mode = "none";
    private volatile double rate;
    private volatile boolean idempotency = true;
    private volatile double kafkaAckLossRate;

    public static void main(String[] args) throws IOException {
        int port = Env.getInt("LEDGER_PORT", 8097);
        new LedgerService().start(port);
        System.out.println("ledger service on :" + port);
    }

    HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 256);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/charges", ex -> handle(ex, () -> {
            if ("POST".equals(ex.getRequestMethod())) {
                charge(ex);
            } else {
                list(ex);
            }
        }));
        server.createContext("/refunds", ex -> handle(ex, () -> refund(ex)));
        server.createContext("/chaos", ex -> handle(ex, () -> chaos(ex)));
        server.createContext("/health", ex -> handle(ex, () -> respond(ex, 200, "{\"ok\":true}", false)));
        server.start();
        return server;
    }

    private interface Action { void run() throws IOException; }

    private static void handle(HttpExchange ex, Action a) {
        try (ex) {
            a.run();
        } catch (Exception e) {
            try {
                respond(ex, 500, Json.write(Map.of("error", String.valueOf(e))), false);
            } catch (IOException ignored) {
                // the client is gone
            }
        }
    }

    private Stats stats(String paymentId) {
        return stats.computeIfAbsent(paymentId, k -> new Stats());
    }

    private boolean hit(String m) {
        return mode.equals(m) && ThreadLocalRandom.current().nextDouble() < rate;
    }

    private void charge(HttpExchange ex) throws IOException {
        JsonNode body = Json.MAPPER.readTree(ex.getRequestBody());
        String paymentId = body.path("paymentId").asText();
        String key = ex.getRequestHeaders().getFirst("Idempotency-Key");
        Stats s = stats(paymentId);
        s.chargeRequests.incrementAndGet();
        if (hit("fail-before-commit")) {
            s.faults.incrementAndGet();
            respond(ex, 503, "{\"error\":\"chaos: unavailable, nothing committed\"}", false);
            return;
        }
        Object[] stored = idempotency && key != null ? answers.get(key) : null;
        if (stored != null) {
            s.replays.incrementAndGet();
            afterCommit(ex, s, (int) stored[0], (String) stored[1], true);
            return;
        }
        int code;
        String answer;
        synchronized (this) {
            // a concurrent first attempt and retry of one key: the second one waits and replays
            stored = idempotency && key != null ? answers.get(key) : null;
            if (stored != null) {
                s.replays.incrementAndGet();
                code = (int) stored[0];
                answer = (String) stored[1];
            } else {
                BigDecimal amount = body.path("amount").decimalValue();
                String ref = "LDG-" + seq.incrementAndGet();
                if (amount.compareTo(LIMIT) > 0) {
                    s.declines.incrementAndGet();
                    code = 422;
                    answer = Json.write(Map.of("error", "insufficient funds: " + amount + " is over the " + LIMIT + " limit"));
                    record(new Entry(key, paymentId, ref, "DECLINED"));
                } else {
                    s.chargeCommits.incrementAndGet();
                    code = 201;
                    answer = Json.write(Map.of("ledgerRef", ref, "status", "CHARGED"));
                    record(new Entry(key, paymentId, ref, "CHARGED"));
                }
                if (key != null) {
                    answers.put(key, new Object[] {code, answer});
                }
            }
        }
        afterCommit(ex, s, code, answer, false);
    }

    private void record(Entry e) {
        byPayment.computeIfAbsent(e.paymentId, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(e);
    }

    /** The answer, unless a post-commit fault eats it. */
    private void afterCommit(HttpExchange ex, Stats s, int code, String answer, boolean replayed) throws IOException {
        if (hit("commit-then-500")) {
            s.faults.incrementAndGet();
            respond(ex, 500, "{\"error\":\"chaos: committed, but answering 500\"}", false);
        } else if (hit("commit-then-timeout")) {
            s.faults.incrementAndGet();
            sleep(8000);
            respond(ex, code, answer, replayed);
        } else if (hit("commit-then-drop")) {
            s.faults.incrementAndGet();
            // close the connection without a status line: the client sees a dropped connection
            ex.close();
        } else {
            respond(ex, code, answer, replayed);
        }
    }

    private void refund(HttpExchange ex) throws IOException {
        JsonNode body = Json.MAPPER.readTree(ex.getRequestBody());
        String paymentId = body.path("paymentId").asText();
        String key = ex.getRequestHeaders().getFirst("Idempotency-Key");
        Stats s = stats(paymentId);
        s.refundRequests.incrementAndGet();
        Object[] stored = idempotency && key != null ? answers.get(key) : null;
        if (stored != null) {
            s.replays.incrementAndGet();
            respond(ex, (int) stored[0], (String) stored[1], true);
            return;
        }
        String answer;
        synchronized (this) {
            List<Entry> charged = byPayment.getOrDefault(paymentId, List.of()).stream()
                    .filter(e -> e.status.equals("CHARGED")).toList();
            charged.forEach(e -> e.status = "REFUNDED");
            s.refunds.addAndGet(charged.size());
            answer = charged.isEmpty()
                    ? Json.write(Map.of("status", "NOTHING_TO_REFUND"))
                    : Json.write(Map.of("ledgerRef", charged.getFirst().ledgerRef, "status", "REFUNDED"));
            if (key != null) {
                answers.put(key, new Object[] {200, answer});
            }
        }
        respond(ex, 200, answer, false);
    }

    private void list(HttpExchange ex) throws IOException {
        String prefix = query(ex).getOrDefault("prefix", "");
        List<Map<String, Object>> out = new ArrayList<>();
        stats.forEach((paymentId, s) -> {
            if (!paymentId.startsWith(prefix)) {
                return;
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("paymentId", paymentId);
            m.put("chargeRequests", s.chargeRequests.get());
            m.put("chargeCommits", s.chargeCommits.get());
            m.put("replays", s.replays.get());
            m.put("declines", s.declines.get());
            m.put("refundRequests", s.refundRequests.get());
            m.put("refunds", s.refunds.get());
            m.put("faults", s.faults.get());
            m.put("entries", byPayment.getOrDefault(paymentId, List.of()).stream()
                    .map(e -> Map.of("ledgerRef", e.ledgerRef, "status", e.status)).toList());
            out.add(m);
        });
        respond(ex, 200, Json.write(out), false);
    }

    private void chaos(HttpExchange ex) throws IOException {
        if ("POST".equals(ex.getRequestMethod())) {
            JsonNode n = Json.MAPPER.readTree(ex.getRequestBody());
            if (n.has("mode")) mode = n.get("mode").asText();
            if (n.has("rate")) rate = n.get("rate").asDouble();
            if (n.has("idempotency")) idempotency = n.get("idempotency").asBoolean();
            if (n.has("kafkaAckLossRate")) kafkaAckLossRate = n.get("kafkaAckLossRate").asDouble();
        }
        ObjectNode o = Json.MAPPER.createObjectNode();
        o.put("mode", mode).put("rate", rate).put("idempotency", idempotency).put("kafkaAckLossRate", kafkaAckLossRate);
        respond(ex, 200, o.toString(), false);
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> q = new LinkedHashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                int i = kv.indexOf('=');
                if (i > 0) {
                    q.put(kv.substring(0, i), URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
                }
            }
        }
        return q;
    }

    private static void respond(HttpExchange ex, int code, String body, boolean replayed) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        if (replayed) {
            ex.getResponseHeaders().set("Idempotent-Replayed", "true");
        }
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
