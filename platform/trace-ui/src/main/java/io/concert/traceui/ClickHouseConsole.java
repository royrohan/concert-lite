package io.concert.traceui;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The trace UI's ClickHouse console: SQL over ClickHouse's HTTP interface as the read-only user
 * ({@code readonly=2} profile with constraints: no writes or DDL, bounded time/rows/memory; see
 * {@code infra/clickhouse/users.d}), answers shaped like the DuckDB sink's
 * {@code {columns, types, rows, elapsedMs, truncated}} so the page reuses one console.
 *
 * <p>Tables and sample queries come from ClickHouse itself: the provisioner tags its tables with
 * comments ({@code concert root <smType>}, {@code concert child <smType> <root>}, {@code marketdata
 * <topic> <class>}) and writes the generated samples to {@code _concert_samples}.
 */
final class ClickHouseConsole {

    static final int MAX_ROWS = 5000;
    private static final Pattern TRAILING_FORMAT = Pattern.compile("(?is).*\\bFORMAT\\s+\\w+\\s*$");

    /** ClickHouse answered with an error; {@code status} is the HTTP status for the page. */
    static final class QueryException extends Exception {
        private static final long serialVersionUID = 1L;
        final int status;

        QueryException(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private final URI base;
    private final String user;
    private final String password;
    private final HttpClient http;

    ClickHouseConsole(String baseUrl, String user, String password, HttpClient http) {
        this.base = URI.create(baseUrl.endsWith("/") ? baseUrl : baseUrl + "/");
        this.user = user;
        this.password = password;
        this.http = http;
    }

    /** Runs one statement; the result in the DuckDB console's shape. */
    Map<String, Object> query(String sql) throws QueryException {
        String stmt = validate(sql);
        long t0 = System.nanoTime();
        JsonNode r = send(stmt + "\nFORMAT JSONCompact");
        return adapt(r, (System.nanoTime() - t0) / 1_000_000);
    }

    Map<String, Object> tables() throws QueryException {
        JsonNode r = send("SELECT t.name, t.engine, t.comment, c.name, c.type FROM system.tables AS t "
                + "INNER JOIN system.columns AS c ON c.database = t.database AND c.table = t.name "
                + "WHERE t.database = currentDatabase() AND t.engine NOT IN ('MaterializedView') "
                + "ORDER BY t.name, c.position FORMAT JSONCompact");
        Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
        for (JsonNode row : r.path("data")) {
            String name = row.get(0).asText();
            Map<String, Object> t = byName.computeIfAbsent(name, n -> describe(n, row.get(1).asText(), row.get(2).asText()));
            @SuppressWarnings("unchecked")
            List<Map<String, String>> cols = (List<Map<String, String>>) t.get("columns");
            cols.add(Map.of("name", row.get(3).asText(), "type", row.get(4).asText()));
        }
        List<Map<String, Object>> list = new ArrayList<>(byName.values());
        List<String> order = List.of("root", "child", "analytics", "marketdata", "system", "kafka");
        list.sort((a, b) -> Integer.compare(order.indexOf((String) a.get("kind")), order.indexOf((String) b.get("kind"))));
        return Map.of("tables", list);
    }

    /** Kind, smType and parent from the provisioner's table comment. */
    static Map<String, Object> describe(String name, String engine, String comment) {
        Map<String, Object> m = new LinkedHashMap<>();
        String[] c = comment.split(" ");
        String kind;
        String smType = null;
        String parent = null;
        if (engine.equals("Kafka")) {
            kind = "kafka";
        } else if (c.length >= 3 && c[0].equals("concert") && c[1].equals("root")) {
            kind = "root";
            smType = c[2];
        } else if (c.length >= 4 && c[0].equals("concert") && c[1].equals("child")) {
            kind = "child";
            smType = c[2];
            parent = c[3];
        } else if (c[0].equals("marketdata")) {
            kind = "marketdata";
        } else if (c[0].equals("analytics")) {
            kind = "analytics";
        } else {
            kind = "system";
        }
        m.put("name", name);
        m.put("kind", kind);
        m.put("smType", smType);
        m.put("parent", parent);
        m.put("engine", engine);
        m.put("columns", new ArrayList<Map<String, String>>());
        return m;
    }

    Map<String, Object> samples() throws QueryException {
        JsonNode r = send("SELECT title, sql FROM _concert_samples ORDER BY ord FORMAT JSONCompact");
        List<Map<String, String>> out = new ArrayList<>();
        for (JsonNode row : r.path("data")) {
            out.add(Map.of("title", row.get(0).asText(), "sql", row.get(1).asText()));
        }
        return Map.of("samples", out);
    }

    Map<String, Object> health() throws QueryException {
        Map<String, Object> out = new LinkedHashMap<>();
        JsonNode v = send("SELECT version(), uptime() FORMAT JSONCompact").path("data").path(0);
        out.put("status", "ok");
        out.put("version", v.path(0).asText());
        out.put("uptimeSeconds", v.path(1).asLong());
        List<Map<String, Object>> consumers = new ArrayList<>();
        for (JsonNode row : send("SELECT table, sum(num_messages_read), max(last_poll_time), sum(length(exceptions.text)),"
                + " arrayStringConcat(arrayFlatten(groupArray(exceptions.text)), ' | ') FROM system.kafka_consumers"
                + " WHERE database = currentDatabase() GROUP BY table ORDER BY table FORMAT JSONCompact").path("data")) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("table", row.get(0).asText());
            c.put("messagesRead", row.get(1).asLong());
            c.put("lastPoll", row.get(2).asText());
            c.put("exceptions", row.get(3).asLong());
            c.put("lastErrors", row.get(4).asText());
            consumers.add(c);
        }
        out.put("kafkaConsumers", consumers);
        out.put("parseErrors", send("SELECT count() FROM kafka_errors FORMAT JSONCompact").path("data").path(0).path(0).asLong());
        return out;
    }

    private JsonNode send(String sql) throws QueryException {
        HttpRequest req = HttpRequest.newBuilder(base).timeout(Duration.ofSeconds(35))
                .header("X-ClickHouse-User", user).header("X-ClickHouse-Key", password)
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8)).build();
        HttpResponse<String> resp;
        try {
            resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new QueryException(502, "ClickHouse not reachable at " + base + " (" + e + "); start it with "
                    + "scripts/up.sh <store> --analytics");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryException(502, "interrupted");
        }
        if (resp.statusCode() != 200) {
            String msg = resp.body().strip();
            throw new QueryException(msg.contains("(READONLY)") || msg.contains("SETTING_CONSTRAINT_VIOLATION") ? 403 : 400, msg);
        }
        try {
            return Json.MAPPER.readTree(resp.body());
        } catch (IOException e) {
            throw new QueryException(502, "unexpected ClickHouse response: " + e.getMessage());
        }
    }

    /** JSONCompact ({@code meta, data, rows, statistics}) to the console's result shape. */
    static Map<String, Object> adapt(JsonNode r, long elapsedMs) {
        List<String> columns = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (JsonNode m : r.path("meta")) {
            columns.add(m.path("name").asText());
            types.add(m.path("type").asText());
        }
        List<Object> rows = new ArrayList<>();
        boolean truncated = false;
        for (JsonNode row : r.path("data")) {
            if (rows.size() == MAX_ROWS) {
                truncated = true;
                break;
            }
            rows.add(row);
        }
        truncated |= rows.size() == MAX_ROWS;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("columns", columns);
        out.put("types", types);
        out.put("rows", rows);
        out.put("elapsedMs", r.path("statistics").has("elapsed")
                ? Math.round(r.path("statistics").path("elapsed").asDouble() * 1000) : elapsedMs);
        out.put("truncated", truncated);
        return out;
    }

    /**
     * One statement without trailing semicolons and without its own FORMAT clause (the console adds
     * JSONCompact). Permissions are ClickHouse's job (read-only user); this only keeps the request
     * well-formed.
     */
    static String validate(String sql) throws QueryException {
        if (sql == null || sql.isBlank()) {
            throw new QueryException(400, "empty query");
        }
        String s = sql.strip();
        while (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).stripTrailing();
        }
        if (hasUnquotedSemicolon(s)) {
            throw new QueryException(400, "only one statement per query");
        }
        if (TRAILING_FORMAT.matcher(stripTrailingComments(s)).matches()) {
            throw new QueryException(400, "leave out the FORMAT clause: the console reads JSONCompact (use the Play UI for other formats)");
        }
        return s; // FORMAT is appended on a new line, so a trailing line comment cannot swallow it
    }

    private static String stripTrailingComments(String s) {
        return s.replaceAll("(?m)--[^\\n]*$", "").strip();
    }

    /** A {@code ;} outside string literals, quoted identifiers and comments. */
    static boolean hasUnquotedSemicolon(String s) {
        char quote = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == '\\') {
                    i++;
                } else if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
            } else if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-') {
                int nl = s.indexOf('\n', i);
                if (nl < 0) {
                    return false;
                }
                i = nl;
            } else if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                if (end < 0) {
                    return false;
                }
                i = end + 1;
            } else if (c == ';') {
                return true;
            }
        }
        return false;
    }
}
