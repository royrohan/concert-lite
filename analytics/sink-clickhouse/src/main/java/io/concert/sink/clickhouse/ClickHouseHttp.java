package io.concert.sink.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Minimal ClickHouse HTTP client: one statement per POST, credentials in headers. */
public final class ClickHouseHttp {

    /** A statement ClickHouse rejected; {@link #getMessage()} is ClickHouse's error text. */
    public static final class ClickHouseException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final int status;

        ClickHouseException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final URI base;
    private final String user;
    private final String password;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public ClickHouseHttp(String baseUrl, String user, String password) {
        this.base = URI.create(baseUrl.endsWith("/") ? baseUrl : baseUrl + "/");
        this.user = user;
        this.password = password;
    }

    /** Runs a statement (DDL, INSERT with inline data, ...) and returns the response body. */
    public String execute(String sql) {
        HttpRequest req = HttpRequest.newBuilder(base).timeout(Duration.ofSeconds(120))
                .header("X-ClickHouse-User", user).header("X-ClickHouse-Key", password)
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8)).build();
        HttpResponse<String> resp;
        try {
            resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("ClickHouse not reachable at " + base + ": " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
        if (resp.statusCode() != 200) {
            throw new ClickHouseException(resp.statusCode(), resp.body().strip());
        }
        return resp.body();
    }

    /** Runs a SELECT and returns its {@code data} rows ({@code FORMAT JSON} is appended). */
    public JsonNode select(String sql) {
        try {
            return JSON.readTree(execute(sql + "\nFORMAT JSON")).path("data");
        } catch (IOException e) {
            throw new IllegalStateException("unexpected ClickHouse response: " + e.getMessage(), e);
        }
    }

    /** True once {@code SELECT 1} succeeds. */
    public boolean ping() {
        try {
            return execute("SELECT 1").strip().equals("1");
        } catch (RuntimeException e) {
            return false;
        }
    }
}
