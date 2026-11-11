package io.concert.eco.payments.external;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.concert.common.Env;
import io.concert.common.Json;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A sample ledger REST service (an "external system" for the payments worker), with fault injection.
 *
 * <pre>
 * POST /charges   {paymentId, accountId, amount, currency}  header Idempotency-Key   201 {ledgerRef, status} | 422 {error}
 * POST /refunds   {paymentId}                               header Idempotency-Key   200 {ledgerRef, status}
 * GET  /charges?prefix=PAY-   what was committed, per payment (the chaos verifier reads it)
 * GET  /chaos, POST /chaos {mode, rate, idempotency, kafkaAckLossRate, latencyMs}
 * GET  /health
 * </pre>
 *
 * Charges above 10 000 are declined (422, nothing charged). With {@code idempotency} on (the default) a key is committed
 * once and every repeat gets the stored answer with {@code Idempotent-Replayed: true}; off, every request commits again
 * (the control experiment: what retries do without idempotency keys).
 *
 * <p>Storage ({@link LedgerStore}): Postgres when {@code LEDGER_JDBC_URL} is set ({@code LEDGER_DB_USER},
 * {@code LEDGER_DB_PASSWORD}), so commits and stored answers survive a restart; otherwise memory. Chaos settings are
 * always in memory: a restart clears them.
 *
 * <p>Chaos modes, applied to {@code rate} of the requests:
 * <ul>
 *   <li>{@code commit-then-500}: commits, then answers 500;
 *   <li>{@code commit-then-timeout}: commits, then answers after 8 s (the client gives up after 4 s);
 *   <li>{@code commit-then-drop}: commits, then closes the connection without an answer;
 *   <li>{@code fail-before-commit}: answers 503 without committing.
 * </ul>
 * {@code latencyMs} delays every answer after its commit (the ledger-restart scenario kills the process in that window).
 */
public final class LedgerService {
    private final LedgerStore store;
    private volatile String mode = "none";
    private volatile double rate;
    private volatile boolean idempotency = true;
    private volatile double kafkaAckLossRate;
    /** Delay between committing and answering: widens the window in which a crash loses a committed answer. */
    private volatile long latencyMs;

    LedgerService(LedgerStore store) {
        this.store = store;
    }

    public static void main(String[] args) throws IOException {
        int port = Env.getInt("LEDGER_PORT", 8097);
        String url = Env.get("LEDGER_JDBC_URL", "");
        LedgerStore store = url.isBlank() ? new LedgerStore.Memory()
                : new LedgerStore.Postgres(url, Env.get("LEDGER_DB_USER", "concert"), Env.get("LEDGER_DB_PASSWORD", "concert"));
        new LedgerService(store).start(port);
        System.out.println("ledger service on :" + port + " (" + (url.isBlank() ? "in memory" : "Postgres " + url) + ")");
    }

    HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 256);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/charges", ex -> handle(ex, () -> {
            if ("POST".equals(ex.getRequestMethod())) {
                charge(ex);
            } else {
                respond(ex, 200, Json.write(store.list(query(ex).getOrDefault("prefix", ""))), false);
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

    private boolean hit(String m) {
        return mode.equals(m) && ThreadLocalRandom.current().nextDouble() < rate;
    }

    private void charge(HttpExchange ex) throws IOException {
        JsonNode body = Json.MAPPER.readTree(ex.getRequestBody());
        String paymentId = body.path("paymentId").asText();
        String key = ex.getRequestHeaders().getFirst("Idempotency-Key");
        store.count(paymentId, "chargeRequests");
        if (hit("fail-before-commit")) {
            store.count(paymentId, "faults");
            respond(ex, 503, "{\"error\":\"chaos: unavailable, nothing committed\"}", false);
            return;
        }
        LedgerStore.Answer a = store.charge(key, paymentId, body.path("amount").decimalValue(), idempotency);
        afterCommit(ex, paymentId, a);
    }

    /** The answer, unless a post-commit fault eats it. */
    private void afterCommit(HttpExchange ex, String paymentId, LedgerStore.Answer a) throws IOException {
        if (hit("commit-then-500")) {
            store.count(paymentId, "faults");
            respond(ex, 500, "{\"error\":\"chaos: committed, but answering 500\"}", false);
        } else if (hit("commit-then-timeout")) {
            store.count(paymentId, "faults");
            sleep(8000);
            respond(ex, a.code(), a.body(), a.replayed());
        } else if (hit("commit-then-drop")) {
            store.count(paymentId, "faults");
            // close the connection without a status line: the client sees a dropped connection
            ex.close();
        } else {
            if (latencyMs > 0) {
                sleep(latencyMs);
            }
            respond(ex, a.code(), a.body(), a.replayed());
        }
    }

    private void refund(HttpExchange ex) throws IOException {
        JsonNode body = Json.MAPPER.readTree(ex.getRequestBody());
        String paymentId = body.path("paymentId").asText();
        store.count(paymentId, "refundRequests");
        LedgerStore.Answer a = store.refund(ex.getRequestHeaders().getFirst("Idempotency-Key"), paymentId, idempotency);
        respond(ex, a.code(), a.body(), a.replayed());
    }

    private void chaos(HttpExchange ex) throws IOException {
        if ("POST".equals(ex.getRequestMethod())) {
            JsonNode n = Json.MAPPER.readTree(ex.getRequestBody());
            if (n.has("mode")) mode = n.get("mode").asText();
            if (n.has("rate")) rate = n.get("rate").asDouble();
            if (n.has("idempotency")) idempotency = n.get("idempotency").asBoolean();
            if (n.has("kafkaAckLossRate")) kafkaAckLossRate = n.get("kafkaAckLossRate").asDouble();
            if (n.has("latencyMs")) latencyMs = n.get("latencyMs").asLong();
        }
        ObjectNode o = Json.MAPPER.createObjectNode();
        o.put("mode", mode).put("rate", rate).put("idempotency", idempotency).put("kafkaAckLossRate", kafkaAckLossRate)
                .put("latencyMs", latencyMs);
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
