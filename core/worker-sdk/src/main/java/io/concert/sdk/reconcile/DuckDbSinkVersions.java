package io.concert.sdk.reconcile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** {@link SinkVersions} over the DuckDB sink's read-only {@code POST /versions} ({@code SINK_DUCKDB_URL}). */
public final class DuckDbSinkVersions implements SinkVersions {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final URI endpoint;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** @param baseUrl e.g. {@code http://sink-duckdb:8090} */
    public DuckDbSinkVersions(String baseUrl) {
        this.endpoint = URI.create(baseUrl.replaceAll("/+$", "") + "/versions");
    }

    @Override
    public Optional<Map<String, Long>> versions(String smType, List<String> entityIds) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("smType", smType);
        body.put("entityIds", entityIds);
        try {
            HttpResponse<byte[]> resp = http.send(HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body))).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            JsonNode n = JSON.readTree(resp.body());
            if (resp.statusCode() != 200) {
                throw new IllegalStateException("DuckDB sink " + endpoint + " answered " + resp.statusCode() + ": "
                        + n.path("error").asText(""));
            }
            if (n.path("table").isNull() || n.path("table").isMissingNode()) {
                return Optional.empty();
            }
            Map<String, Long> out = new HashMap<>();
            n.path("versions").properties().forEach(e -> out.put(e.getKey(), e.getValue().asLong()));
            return Optional.of(out);
        } catch (IOException e) {
            throw new UncheckedIOException("DuckDB sink not reachable at " + endpoint + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }
}
