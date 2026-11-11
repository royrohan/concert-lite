package io.concert.eco.payments.external;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Fault injection for the worker side, read from the ledger service's {@code GET /chaos} (the chaos scripts set it there,
 * so one call switches both sides). Cached for a second; unreachable means no faults.
 */
final class ChaosControl {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private final URI uri;
    private volatile double kafkaAckLossRate;
    private volatile long readAt;

    ChaosControl(String ledgerUrl) {
        this.uri = URI.create(ledgerUrl + "/chaos");
    }

    /** Share of Kafka sends whose acknowledgement is dropped after the record was written. */
    double kafkaAckLossRate() {
        long now = System.currentTimeMillis();
        if (now - readAt > 1000) {
            readAt = now;
            try {
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(1)).build(),
                        HttpResponse.BodyHandlers.ofString());
                JsonNode n = Json.MAPPER.readTree(r.body());
                kafkaAckLossRate = n.path("kafkaAckLossRate").asDouble(0);
            } catch (Exception e) {
                kafkaAckLossRate = 0;
            }
        }
        return kafkaAckLossRate;
    }
}
