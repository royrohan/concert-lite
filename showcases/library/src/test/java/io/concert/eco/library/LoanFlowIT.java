package io.concert.eco.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Env;
import io.concert.common.EventEnvelope;
import io.concert.common.Flows;
import io.concert.common.Json;
import io.concert.orchestration.KinesisClients;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * The loan flow through the deployed stack: events go onto the flow's Kinesis stream, the coordinator serializes
 * them and the library worker runs LoanMachine; the test reads the result back from the dashboard.
 *
 * <p>Needs the stack ({@code ./deploy-concert-ecosystem library}); run with {@code ./gradlew :library:integrationTest}.
 * {@code KINESIS_ENDPOINT} (default http://localhost:4566) and {@code DASHBOARD_URL} (default http://localhost:8088)
 * point it elsewhere.
 */
@Tag("integration")
class LoanFlowIT {

    private static final String DASHBOARD = Env.get("DASHBOARD_URL", "http://localhost:8088");
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void aLoanIsBorrowedRenewedAndReturned() throws Exception {
        String loanId = "LOAN-IT-" + System.currentTimeMillis();
        try (KinesisClient kinesis = KinesisClients.fromEnv()) {
            send(kinesis, loanId, "borrow", "{\"memberId\":\"M-7\",\"bookId\":\"B-42\",\"dueDate\":\"2026-12-01\"}");
            send(kinesis, loanId, "renew", "{\"dueDate\":\"2026-12-15\"}");
            send(kinesis, loanId, "return", "{\"condition\":\"good\"}");
        }

        JsonNode loan = awaitState(loanId, "RETURNED");
        assertEquals("B-42", loan.path("bookId").asText());
        assertEquals(1, loan.path("renewals").asInt());
        assertEquals("2026-12-15", loan.path("dueDate").asText());
    }

    @Test
    void aRejectedRenewalLeavesTheLoanAsItWas() throws Exception {
        String loanId = "LOAN-IT-" + System.currentTimeMillis() + "-R";
        try (KinesisClient kinesis = KinesisClients.fromEnv()) {
            send(kinesis, loanId, "borrow", "{\"memberId\":\"M-7\",\"bookId\":\"B-43\",\"dueDate\":\"2026-12-01\"}");
            send(kinesis, loanId, "renew", "{\"dueDate\":\"2026-11-01\"}"); // earlier: rejected by LoanMachine
            send(kinesis, loanId, "lose", "{\"note\":\"left on the train\"}");
        }

        JsonNode loan = awaitState(loanId, "LOST");
        assertEquals(0, loan.path("renewals").asInt());
        assertEquals("2026-12-01", loan.path("dueDate").asText());
    }

    /** Puts one event on the flow's stream, partitioned like the event sender (./showcases/library/send). */
    private static void send(KinesisClient kinesis, String loanId, String eventType, String payload) throws InterruptedException {
        EventEnvelope e = LibraryEvents.envelope("loan", eventType, loanId, payload);
        kinesis.putRecord(b -> b.streamName(Flows.stream(LibraryEvents.FLOW))
                .partitionKey(e.effectiveLockKeys().getFirst())
                .data(SdkBytes.fromUtf8String(Json.write(e))));
        Thread.sleep(200);
    }

    /** Polls the dashboard until the loan is in {@code state}; returns its data. */
    private JsonNode awaitState(String loanId, String state) throws Exception {
        Instant deadline = Instant.now().plus(TIMEOUT);
        String last = "not found";
        while (Instant.now().isBefore(deadline)) {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(DASHBOARD + "/api/entities/loan:" + loanId)).build(),
                    HttpResponse.BodyHandlers.ofString());
            JsonNode row = Json.MAPPER.readTree(r.body()).path("projection");
            if (!row.isMissingNode() && !row.isNull()) {
                last = row.path("state").asText();
                if (last.equals(state)) {
                    return Json.MAPPER.readTree(row.path("data").asText());
                }
            }
            Thread.sleep(500);
        }
        return fail("loan " + loanId + " did not reach " + state + " in " + TIMEOUT.toSeconds() + " s (last: " + last + ")");
    }
}
