package io.concert.eco.payments.external;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.concert.common.Json;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The sample ledger over HTTP, in memory: idempotency keys, declines, refunds, chaos after commit. */
class LedgerServiceTest {
    private HttpServer server;
    private String base;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void start() throws Exception {
        server = new LedgerService(new LedgerStore.Memory()).start(0);
        base = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void aRepeatedKeyGetsTheStoredAnswerAndCommitsOnce() throws Exception {
        HttpResponse<String> first = charge("P-1", "P-1:charge", "100");
        HttpResponse<String> again = charge("P-1", "P-1:charge", "100");

        assertEquals(201, first.statusCode());
        assertEquals(first.body(), again.body());
        assertEquals("true", again.headers().firstValue("Idempotent-Replayed").orElse(""));
        JsonNode p = payment("P-1");
        assertEquals(1, p.path("chargeCommits").asInt());
        assertEquals(1, p.path("replays").asInt());
    }

    @Test
    void aChargeOverTheLimitIsDeclinedAndNothingIsCharged() throws Exception {
        assertEquals(422, charge("P-2", "P-2:charge", "25000").statusCode());
        assertEquals(0, payment("P-2").path("chargeCommits").asInt());
    }

    @Test
    void commitThen500StillCommitsAndTheRetryReplays() throws Exception {
        post("/chaos", null, "{\"mode\":\"commit-then-500\",\"rate\":1}");
        assertEquals(500, charge("P-3", "P-3:charge", "10").statusCode());
        post("/chaos", null, "{\"mode\":\"none\"}");
        HttpResponse<String> retry = charge("P-3", "P-3:charge", "10");

        assertEquals(201, retry.statusCode());
        assertTrue(retry.headers().firstValue("Idempotent-Replayed").isPresent());
        assertEquals(1, payment("P-3").path("chargeCommits").asInt());
    }

    @Test
    void withoutIdempotencyEveryRetryChargesAgain() throws Exception {
        post("/chaos", null, "{\"idempotency\":false}");
        charge("P-4", "P-4:charge", "10");
        charge("P-4", "P-4:charge", "10");
        assertEquals(2, payment("P-4").path("chargeCommits").asInt());
    }

    @Test
    void aRefundIsDoneOnceAndARefundOfNothingSucceeds() throws Exception {
        assertEquals(200, post("/refunds", "P-5:refund", "{\"paymentId\":\"P-5\"}").statusCode(), "nothing to refund");
        charge("P-6", "P-6:charge", "10");
        post("/refunds", "P-6:refund", "{\"paymentId\":\"P-6\"}");
        post("/refunds", "P-6:refund", "{\"paymentId\":\"P-6\"}");
        JsonNode p = payment("P-6");
        assertEquals(1, p.path("refunds").asInt());
        assertFalse(p.path("entries").toString().contains("CHARGED"));
    }

    private HttpResponse<String> charge(String paymentId, String key, String amount) throws Exception {
        return post("/charges", key, "{\"paymentId\":\"" + paymentId + "\",\"accountId\":\"A\",\"amount\":" + amount + ",\"currency\":\"EUR\"}");
    }

    private HttpResponse<String> post(String path, String key, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) {
            b.header("Idempotency-Key", key);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode payment(String id) throws Exception {
        String body = http.send(HttpRequest.newBuilder(URI.create(base + "/charges?prefix=" + id)).build(),
                HttpResponse.BodyHandlers.ofString()).body();
        return Json.MAPPER.readTree(body).get(0);
    }
}
