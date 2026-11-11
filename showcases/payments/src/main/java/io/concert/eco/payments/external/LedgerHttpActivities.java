package io.concert.eco.payments.external;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * {@link LedgerActivities} over HTTP ({@code LEDGER_URL}, the sample service in {@link LedgerService}).
 *
 * <p>Status mapping: 2xx is the result; 422 is a business refusal (non-retryable {@code Declined}: retrying cannot
 * change it); anything else, a timeout or a dropped connection is retryable: the charge may or may not have been
 * committed, and the retry, with the same idempotency key, finds out.
 */
public final class LedgerHttpActivities implements LedgerActivities {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(4);
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String base;

    public LedgerHttpActivities(String ledgerUrl) {
        this.base = ledgerUrl;
    }

    @Override
    public Receipt charge(ChargeRequest r) {
        return post("/charges", r.idempotencyKey(), Map.of("paymentId", r.paymentId(), "accountId", r.accountId(),
                "amount", r.amount(), "currency", r.currency()));
    }

    @Override
    public Receipt refund(String idempotencyKey, String paymentId) {
        return post("/refunds", idempotencyKey, Map.of("paymentId", paymentId));
    }

    private Receipt post(String path, String idempotencyKey, Map<String, Object> body) {
        int attempt = Activity.getExecutionContext().getInfo().getAttempt();
        HttpResponse<String> resp;
        try {
            resp = http.send(HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", idempotencyKey)
                    .POST(HttpRequest.BodyPublishers.ofString(Json.write(body)))
                    .build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            // timeout or connection dropped: the outcome is unknown, retry with the same key
            throw ApplicationFailure.newFailure("ledger " + path + " gave no answer (attempt " + attempt + "): " + e, "LedgerNoAnswer");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ApplicationFailure.newFailure("interrupted", "LedgerNoAnswer");
        }
        int code = resp.statusCode();
        if (code == 422) {
            throw ApplicationFailure.newNonRetryableFailure(error(resp.body()), "Declined");
        }
        if (code / 100 != 2) {
            throw ApplicationFailure.newFailure("ledger " + path + " answered " + code + " (attempt " + attempt + "): "
                    + error(resp.body()), "LedgerError");
        }
        JsonNode n = read(resp.body());
        boolean replayed = resp.headers().firstValue("Idempotent-Replayed").map(Boolean::parseBoolean).orElse(false);
        return new Receipt(n.path("ledgerRef").asText(null), n.path("status").asText(), replayed, attempt);
    }

    private static String error(String body) {
        JsonNode n = read(body);
        return n.path("error").asText(body == null ? "" : body);
    }

    private static JsonNode read(String body) {
        try {
            return Json.MAPPER.readTree(body == null || body.isBlank() ? "{}" : body);
        } catch (IOException e) {
            return Json.MAPPER.createObjectNode();
        }
    }
}
