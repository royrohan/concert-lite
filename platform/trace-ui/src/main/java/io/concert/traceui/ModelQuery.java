package io.concert.traceui;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Json;
import io.concert.legend.LegendStoreGenerator;
import io.concert.legend.LegendStoreGenerator.Target;
import io.concert.legend.LegendStoreMain;
import io.concert.sink.ColumnSpec;
import io.concert.sink.ColumnType;
import io.concert.sink.TableSpec;
import java.io.IOException;
import java.io.InputStream;
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

/**
 * The Model query tab: Pure queries over the concert models, planned by legend-lite and run by concert.
 *
 * <ol>
 *   <li>The model context is the concert profile, the .pure files the DuckDB sink loaded and its roots ({@code GET
 *       /models}, so ecosystem overrides are included), and the Legend store layer generated from them
 *       ({@link LegendStoreGenerator}: the same tables the sinks built, DuckDB as is, ClickHouse through
 *       {@code <root>_latest}). Cached for a minute.
 *   <li>legend-lite parses the query ({@code grammarToJson/lambda}) and plans it per store ({@code generatePlan} with the
 *       store's runtime), which yields one SQL statement in that store's dialect.
 *   <li>concert runs that SQL read-only: DuckDB through the sink's {@code /query}, ClickHouse as the readonly user.
 * </ol>
 *
 * legend-lite never touches the stores (it couldn't: the DuckDB file is the sink's, and its ClickHouse connector isn't
 * wired); it only turns Pure into SQL.
 */
final class ModelQuery {
    private final String lite;
    private final String studioUrl = io.concert.common.Env.get("LEGEND_STUDIO_PUBLIC_URL", "http://localhost:8200");
    private final String sinkDuckDb;
    private final ClickHouseConsole clickHouse;
    private final HttpClient http;
    private Context context;

    /** The model code and the generated layer, with what the class browser and samples need. */
    record Context(long at, String code, String store, LegendStoreMain.Generated generated, int files,
            List<io.concert.legend.StudioProject.File> studio) {}

    ModelQuery(String liteUrl, String sinkDuckDb, ClickHouseConsole clickHouse, HttpClient http) {
        this.lite = liteUrl.endsWith("/") ? liteUrl.substring(0, liteUrl.length() - 1) : liteUrl;
        this.sinkDuckDb = sinkDuckDb;
        this.clickHouse = clickHouse;
        this.http = http;
    }

    synchronized Context context() throws IOException, InterruptedException {
        if (context != null && System.currentTimeMillis() - context.at() < 60_000) {
            return context;
        }
        JsonNode m = getJson(sinkDuckDb + "/models");
        if (m.has("error")) {
            throw new IOException("DuckDB sink /models: " + m.path("error").asText());
        }
        Map<String, String> files = new LinkedHashMap<>();
        m.path("files").forEach(f -> files.put(f.path("name").asText(), f.path("text").asText()));
        LegendStoreMain.Generated g = LegendStoreMain.fromSources(files, m.path("roots").asText());
        String store = g.generator().generate(List.of(Target.DUCKDB, Target.CLICKHOUSE));
        StringBuilder code = new StringBuilder(profile()).append('\n');
        files.values().forEach(t -> code.append(t).append('\n'));
        code.append(store);
        Map<String, String> withProfile = new LinkedHashMap<>();
        withProfile.put("concert/concert.pure", profile());
        withProfile.putAll(files);
        context = new Context(System.currentTimeMillis(), code.toString(), store, g, files.size(),
                io.concert.legend.StudioProject.files(withProfile, g, List.of(Target.DUCKDB, Target.CLICKHOUSE)));
        return context;
    }

    private static String profile() throws IOException {
        try (InputStream in = ModelQuery.class.getResourceAsStream("/concert/concert.pure")) {
            return in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Status, mapped classes (per table, with their property paths) and sample queries, for every root. */
    Map<String, Object> info() {
        return info(null);
    }

    /**
     * As {@link #info()}, limited to the tables of the given smTypes (a flow's snapshot types) when not null: the flow
     * page's Legend box shows only that flow's classes and examples.
     */
    Map<String, Object> info(java.util.Set<String> smTypes) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("liteUrl", lite);
        out.put("studioUrl", studioUrl);
        try {
            JsonNode h = getJson(lite + "/health");
            out.put("reachable", "ok".equals(h.path("status").asText()));
        } catch (IOException | InterruptedException e) {
            out.put("reachable", false);
            out.put("error", "legend-lite not reachable at " + lite + " (start with scripts/up.sh <store> --analytics)");
        }
        try {
            Context c = context();
            List<Map<String, Object>> classes = new ArrayList<>();
            List<Map<String, String>> samples = new ArrayList<>();
            java.util.Set<String> variants = new java.util.HashSet<>();
            c.generated().generator().variants().forEach(v -> variants.add(v.name()));
            for (TableSpec t : c.generated().schema().tables()) {
                if (smTypes != null && !smTypes.contains(t.smType())) {
                    continue;
                }
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("class", t.pureClass());
                m.put("smType", t.smType());
                m.put("kind", t.kind().name().toLowerCase());
                m.put("table", LegendStoreGenerator.tableName(t));
                // a root whose class another root maps too: its class queries run with its own mapping
                String variant = t.kind() == TableSpec.Kind.ROOT && variants.contains(t.name()) ? t.name() : null;
                m.put("variant", variant);
                m.put("mapping", variant == null ? LegendStoreGenerator.MAPPING : LegendStoreGenerator.variantMapping(t));
                List<String> props = properties(t);
                m.put("properties", props);
                classes.add(m);
                if (t.kind() == TableSpec.Kind.ROOT) {
                    for (Map<String, String> sm : samples(t, props, c)) {
                        Map<String, String> withVariant = new LinkedHashMap<>(sm);
                        if (variant != null) {
                            withVariant.put("variant", variant);
                        }
                        samples.add(withVariant);
                    }
                }
            }
            out.put("classes", classes);
            out.put("samples", samples);
            out.put("files", c.files());
            String hash = io.concert.legend.StudioProject.fingerprint(c.studio());
            out.put("studioProject", "Concert models (" + hash + ")");
            out.put("studioProjectId", "io.concert:concert-" + hash);
        } catch (IOException | RuntimeException e) {
            out.put("modelError", String.valueOf(e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return out;
    }

    private volatile Object[] lastStatus;

    /** {@code {reachable, url}} of legend-lite, cached for 5 s (the flow page polls it). */
    Map<String, Object> status() {
        Object[] hit = lastStatus;
        if (hit != null && System.currentTimeMillis() - (long) hit[0] < 5000) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) hit[1];
            return v;
        }
        boolean up;
        try {
            up = "ok".equals(getJson(lite + "/health").path("status").asText());
        } catch (IOException e) {
            up = false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            up = false;
        }
        Map<String, Object> v = Map.of("reachable", up, "url", lite);
        lastStatus = new Object[] {System.currentTimeMillis(), v};
        return v;
    }

    /**
     * The Legend Studio project of the current models and store layer, in the shape Studio's "Load demo projects"
     * reads ({@code projects/manifest.json} plus the files it lists): one step, one release, one element per file. The
     * artifact id carries a content hash, so a changed model publishes as a new project (Studio skips a step whose
     * version already exists).
     */
    Map<String, Object> studioManifest() throws IOException, InterruptedException {
        Context c = context();
        String hash = io.concert.legend.StudioProject.fingerprint(c.studio());
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("artifactId", "concert-" + hash);
        step.put("name", "Concert models (" + hash + ")");
        step.put("message", "concert models and the sinks' Legend store layer (" + c.studio().size() + " elements)");
        step.put("files", c.studio().stream().map(io.concert.legend.StudioProject.File::name).toList());
        step.put("release", "MAJOR");
        return Map.of("groupId", "io.concert", "steps", List.of(step));
    }

    /** Where Legend Studio opens concert's project: {@code {url, projectId, project}}, or null when the model isn't loaded. */
    Map<String, Object> studioLink() {
        try {
            String hash = io.concert.legend.StudioProject.fingerprint(context().studio());
            return Map.of("url", studioUrl + "/demo/index.html?concert=" + java.net.URLEncoder.encode("io.concert:concert-" + hash,
                    StandardCharsets.UTF_8), "projectId", "io.concert:concert-" + hash, "project", "Concert models (" + hash + ")");
        } catch (IOException | RuntimeException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** One file of {@link #studioManifest()}, or null. */
    String studioFile(String name) throws IOException, InterruptedException {
        return context().studio().stream().filter(f -> f.name().equals(name)).map(io.concert.legend.StudioProject.File::code)
                .findFirst().orElse(null);
    }

    /** The generated store layer (Database, Mapping, Connection, Runtime), for reading. */
    String store() throws IOException, InterruptedException {
        return context().store();
    }

    /** Mapped property paths of a table's class ({@code total.amount}), as the generator maps them. */
    static List<String> properties(TableSpec t) {
        boolean event = t.kind() == TableSpec.Kind.ROOT && t.smType().startsWith("evt_");
        List<String> out = new ArrayList<>();
        for (ColumnSpec c : t.columns()) {
            if (c.source() != ColumnSpec.Source.MODEL || c.type() == ColumnType.JSON) {
                continue;
            }
            List<String> p = c.path();
            if (event) {
                if (p.isEmpty() || !p.getFirst().equals("payload")) {
                    continue;
                }
                p = p.subList(1, p.size());
            }
            if (!p.isEmpty()) {
                out.add(String.join(".", p));
            }
        }
        return out;
    }

    /**
     * The example queries of a root, all in Legend and all portable (each runs unchanged on DuckDB and ClickHouse; the
     * runtime picks the store). Class queries for model properties; table access on {@code <root>_latest} for what
     * isn't in the model (sm_state, entity_version, created_at / completed_at, event lifecycle columns). They are the
     * Legend versions of the store pages' SQL examples: latest rows, by state, completion time by state, completions
     * per minute, latest versions, plus a group-by on the first enum and a query through the first association.
     */
    static List<Map<String, String>> samples(TableSpec root, List<String> props, Context c) {
        List<Map<String, String>> out = new ArrayList<>();
        String cls = root.pureClass();
        String name = simple(cls);
        String table = "#>{" + LegendStoreGenerator.DATABASE + "." + LegendStoreGenerator.tableName(root) + "}#";
        boolean event = root.smType().startsWith("evt_");
        List<String> pick = props.stream().limit(6).toList();
        if (!pick.isEmpty()) {
            // Sorted, so both stores return the same rows and can be compared side by side.
            out.add(sample(name + ": latest rows", cls + ".all()\n  ->project([" + lambdas(pick) + "],\n    [" + aliases(pick)
                    + "])\n  ->sort([asc('" + alias(pick.getFirst()) + "')])\n  ->take(20)"));
        }
        out.add(sample(name + ": by state", table + "\n  ->groupBy(~sm_state, ~entities: x|$x.entity_id : y|$y->count())"
                + "\n  ->sort(~entities->descending())"));
        out.add(sample(name + ": completion time by state", table + "\n  ->filter(x|$x.completed_at->isNotEmpty())"
                + "\n  ->extend(~ms: x|dateDiff($x.created_at->toOne(), $x.completed_at->toOne(), DurationUnit.MILLISECONDS))"
                + "\n  ->groupBy(~sm_state, ~[n: x|$x.entity_id : y|$y->count(), avg_ms: x|$x.ms : y|$y->average(), max_ms: x|$x.ms : y|$y->max()])"
                + "\n  ->sort(~n->descending())"));
        out.add(sample(name + ": completions per minute", table + "\n  ->filter(x|$x.completed_at->isNotEmpty())"
                + "\n  ->extend(~minute: x|$x.completed_at->toOne()->timeBucket(1, DurationUnit.MINUTES))"
                + "\n  ->groupBy(~minute, ~completions: x|$x.entity_id : y|$y->count())\n  ->sort(~minute->descending())\n  ->limit(60)"));
        out.add(sample(name + ": latest versions", table + "\n  ->select(~[entity_id, entity_version, sm_state, created_at, completed_at])"
                + "\n  ->sort(~completed_at->descending())\n  ->limit(20)"));
        if (event) {
            out.add(sample(name + ": lifecycle by status", table + "\n  ->groupBy(~[event_type, status], ~events: x|$x.entity_id : y|$y->count())"
                    + "\n  ->sort(~events->descending())"));
        }
        if (!pick.isEmpty()) {
            root.columns().stream().filter(col -> col.type() == ColumnType.ENUM && col.source() == ColumnSpec.Source.MODEL)
                    .map(col -> String.join(".", col.path().getFirst().equals("payload") ? col.path().subList(1, col.path().size()) : col.path()))
                    .filter(props::contains).findFirst()
                    .ifPresent(e -> {
                        out.add(sample(name + ": by " + e, cls + ".all()\n  ->groupBy([x|$x." + e + "], [agg(x|$x." + pick.getFirst()
                                + ", y|$y->count())], ['" + alias(e) + "', 'count'])\n  ->sort([desc('count')])"));
                        // the first top-level number (a quantity, notional, amount...) summed per that enum
                        root.columns().stream().filter(col -> col.source() == ColumnSpec.Source.MODEL
                                        && (col.type() == ColumnType.DECIMAL || col.type() == ColumnType.DOUBLE || col.type() == ColumnType.BIGINT))
                                .map(col -> String.join(".", col.path().getFirst().equals("payload") ? col.path().subList(1, col.path().size()) : col.path()))
                                .filter(n -> props.contains(n) && !n.contains(".")).findFirst()
                                .ifPresent(n -> out.add(sample(name + ": total " + n + " by " + e, cls + ".all()\n  ->groupBy([x|$x." + e
                                        + "], [agg(x|$x." + n + ", y|$y->sum()), agg(x|$x." + pick.getFirst() + ", y|$y->count())], ['" + alias(e)
                                        + "', 'total_" + alias(n) + "', 'count'])\n  ->sort([desc('total_" + alias(n) + "')])")));
                    });
            c.generated().schema().childrenOf(root).stream().findFirst().ifPresent(child -> {
                List<String> childProps = properties(child).stream().limit(3).toList();
                if (!childProps.isEmpty()) {
                    String assoc = child.sourcePath().getLast();
                    List<String> paths = new ArrayList<>(List.of(pick.getFirst()));
                    childProps.forEach(p -> paths.add(assoc + "." + p));
                    out.add(sample(name + ": with " + assoc, cls + ".all()\n  ->project([" + lambdas(paths) + "],\n    [" + aliases(paths)
                            + "])\n  ->sort([asc('" + alias(paths.getFirst()) + "'), asc('" + alias(paths.get(1)) + "')])\n  ->take(50)"));
                }
            });
        }
        return out;
    }

    private static Map<String, String> sample(String title, String query) {
        return Map.of("title", title, "query", query);
    }

    private static String simple(String cls) {
        return cls.substring(cls.lastIndexOf(':') + 1);
    }

    private static String lambdas(List<String> paths) {
        return String.join(", ", paths.stream().map(p -> "x|$x." + p).toList());
    }

    private static String alias(String path) {
        return path.replace('.', '_');
    }

    private static String aliases(List<String> paths) {
        return String.join(", ", paths.stream().map(p -> "'" + alias(p) + "'").toList());
    }

    /**
     * Plans {@code query} for each requested store and runs it there:
     * {@code {query, planMs, results: {duckdb|clickhouse: {sql, columns, types, rows, elapsedMs, truncated} | {sql?, error}}}}.
     */
    Map<String, Object> run(String query, String stores) {
        return run(query, stores, null);
    }

    /**
     * As {@link #run(String, String)}; with {@code variant} (a root table whose class another root maps too), class
     * queries read that table: the runtime of its own Mapping ({@link LegendStoreGenerator#variantRuntime}).
     */
    Map<String, Object> run(String query, String stores, String variant) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("query", query);
        Context c;
        JsonNode lambda;
        long t0 = System.currentTimeMillis();
        try {
            c = context();
            lambda = post("/api/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false", query, "text/plain");
        } catch (QueryError e) {
            out.put("error", "query: " + e.getMessage());
            return out;
        } catch (IOException | RuntimeException e) {
            out.put("error", String.valueOf(e.getMessage()));
            return out;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            out.put("error", "interrupted");
            return out;
        }
        Map<String, Object> results = new LinkedHashMap<>();
        for (String s : stores.split(",")) {
            Target t;
            try {
                t = Target.parse(s);
            } catch (IllegalArgumentException e) {
                continue;
            }
            results.put(s.trim().toLowerCase(), runOn(t, c, lambda, variant));
        }
        out.put("planMs", System.currentTimeMillis() - t0);
        out.put("results", results);
        return out;
    }

    private Map<String, Object> runOn(Target t, Context c, JsonNode lambda, String variant) {
        Map<String, Object> r = new LinkedHashMap<>();
        String sql;
        try {
            long t0 = System.currentTimeMillis();
            JsonNode plan = post("/api/pure/v1/execution/generatePlan", Json.write(Map.of("model", Map.of("_type", "text", "code", c.code()),
                    "function", lambda, "runtime", Map.of("_type", "runtimePointer", "runtime",
                            LegendStoreGenerator.variantRuntime(t, variant == null || variant.isBlank() ? null : variant)))), "application/json");
            sql = plan.findValuesAsText("sqlQuery").stream().findFirst().orElse(null);
            r.put("planMs", System.currentTimeMillis() - t0);
            if (sql == null) {
                r.put("error", "legend-lite returned a plan without SQL (graph fetch and other non-relational results aren't supported)");
                return r;
            }
            r.put("sql", sql);
        } catch (QueryError e) {
            r.put("error", "plan: " + e.getMessage());
            return r;
        } catch (IOException | RuntimeException e) {
            r.put("error", String.valueOf(e.getMessage()));
            return r;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            r.put("error", "interrupted");
            return r;
        }
        try {
            Map<String, Object> rows = t == Target.CLICKHOUSE ? clickHouse.query(sql) : duckDb(sql);
            if (rows.containsKey("error")) {
                r.put("error", rows.get("error"));
            } else {
                r.putAll(rows);
            }
        } catch (ClickHouseConsole.QueryException e) {
            r.put("error", e.getMessage());
        } catch (IOException | RuntimeException e) {
            r.put("error", String.valueOf(e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            r.put("error", "interrupted");
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> duckDb(String sql) throws IOException, InterruptedException {
        HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create(sinkDuckDb + "/query")).timeout(Duration.ofSeconds(30))
                .header("Content-Type", "text/plain; charset=utf-8").POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
                .build(), HttpResponse.BodyHandlers.ofString());
        return Json.MAPPER.readValue(resp.body(), Map.class);
    }

    /** legend-lite rejected the request (parse / compile / plan error): its message. */
    static final class QueryError extends IOException {
        QueryError(String message) {
            super(message);
        }
    }

    private JsonNode post(String path, String body, String contentType) throws IOException, InterruptedException {
        HttpResponse<String> r;
        try {
            r = http.send(HttpRequest.newBuilder(URI.create(lite + path)).timeout(Duration.ofSeconds(60)).header("Content-Type", contentType)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IOException("legend-lite not reachable at " + lite + " (" + e.getMessage() + ")");
        }
        if (r.statusCode() != 200) {
            throw new QueryError(message(r.body()));
        }
        return Json.MAPPER.readTree(r.body());
    }

    /** The human part of a legend-lite error body (JSON with message / errors, or text). */
    static String message(String body) {
        try {
            JsonNode n = Json.MAPPER.readTree(body);
            if (n.hasNonNull("message")) {
                return n.path("message").asText();
            }
            if (n.path("errors").isArray() && !n.path("errors").isEmpty()) {
                List<String> ms = new ArrayList<>();
                n.path("errors").forEach(e -> ms.add(e.hasNonNull("message") ? e.path("message").asText() : e.toString()));
                return String.join("; ", ms);
            }
        } catch (IOException ignored) {
            // not JSON
        }
        return body.length() > 2000 ? body.substring(0, 2000) + "…" : body;
    }

    private JsonNode getJson(String url) throws IOException, InterruptedException {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return Json.MAPPER.readTree(r.body());
    }
}
