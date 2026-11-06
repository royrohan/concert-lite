package io.concert.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.concert.legend.LegendStoreGenerator.Target;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * concert's Pure against legend-lite (real Pure semantics; concert's own parser is more lenient), when
 * {@code LEGEND_LITE_URL} points at a running server ({@code scripts/legend-lite.sh check} starts one and runs this):
 *
 * <ul>
 *   <li>every production model set compiles as is;
 *   <li>the whole deployment (every model directory the sinks load, plus the generated store layer for every sink root:
 *       the base roots of docker-compose.yml and each ecosystem's compose override) compiles;
 *   <li>a model query plans on both runtimes, against the sink tables (DuckDB) and the {@code _latest} views (ClickHouse).
 * </ul>
 */
class LegendLiteCompileTest {
    static final Path REPO = LegendStoreGeneratorTest.REPO;
    static final Path CONCERT_PROFILE = REPO.resolve("core/model-pure/src/main/resources/concert/concert.pure");
    static final ObjectMapper JSON = new ObjectMapper();
    static String url;
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void server() {
        url = System.getenv("LEGEND_LITE_URL");
        Assumptions.assumeTrue(url != null && !url.isBlank(), "LEGEND_LITE_URL not set: legend-lite checks skipped");
    }

    /** One model set: the files compiled together. */
    record ModelSet(String name, List<Path> files) {}

    static List<ModelSet> modelSets() {
        List<ModelSet> sets = new ArrayList<>();
        sets.add(new ModelSet("sample-workers", withProfile(pure(REPO.resolve("showcases/sample-workers/src/main/pure"), false))));
        sets.add(new ModelSet("trading-model", withProfile(pure(REPO.resolve("core/trading-model/src/main/pure"), false))));
        for (Path eco : ecosystems()) {
            // Generated modules carry their own copy of the profile (src/main/pure/concert/concert.pure).
            sets.add(new ModelSet("showcases/" + eco.getFileName(), pure(eco.resolve("src/main/pure"), true)));
            Path example = REPO.resolve("examples").resolve(eco.getFileName());
            if (Files.isDirectory(example)) {
                sets.add(new ModelSet("examples/" + eco.getFileName(), withProfile(pure(example, false))));
            }
        }
        return sets;
    }

    static List<Path> ecosystems() {
        try (Stream<Path> s = Files.list(REPO.resolve("showcases"))) {
            return s.filter(p -> Files.isRegularFile(p.resolve("ecosystem.json"))).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<Path> pure(Path dir, boolean recursive) {
        try (Stream<Path> s = recursive ? Files.walk(dir) : Files.list(dir)) {
            return s.filter(p -> p.toString().endsWith(".pure")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<Path> withProfile(List<Path> files) {
        List<Path> out = new ArrayList<>(List.of(CONCERT_PROFILE));
        out.addAll(files);
        return out;
    }

    static String read(List<Path> files) {
        StringBuilder sb = new StringBuilder();
        for (Path f : files) {
            try {
                sb.append(Files.readString(f)).append('\n');
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return sb.toString();
    }

    static JsonNode post(String path, String body, boolean json) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url + path))
                .header("Content-Type", json ? "application/json" : "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), path + ": " + r.body());
        return JSON.readTree(r.body());
    }

    static List<String> compile(String code) throws Exception {
        JsonNode r = post("/api/lite/v1/compilation/compile", JSON.writeValueAsString(Map.of("_type", "text", "code", code)), true);
        List<String> errors = new ArrayList<>();
        r.path("errors").forEach(e -> errors.add(e.toString()));
        return errors;
    }

    @Test
    void everyModelSetCompiles() throws Exception {
        Map<String, List<String>> failures = new LinkedHashMap<>();
        for (ModelSet set : modelSets()) {
            List<String> errors = compile(read(set.files()));
            if (!errors.isEmpty()) {
                failures.put(set.name(), errors);
            }
        }
        assertTrue(failures.isEmpty(), "legend-lite rejects: " + failures);
    }

    /** docker-compose.yml's base sink roots. */
    static String baseRoots() throws IOException {
        Matcher base = Pattern.compile("x-sink-roots: &sink-roots >-\\s*\\n\\s*(\\S+)").matcher(Files.readString(REPO.resolve("docker-compose.yml")));
        assertTrue(base.find(), "x-sink-roots in docker-compose.yml");
        return base.group(1);
    }

    /** An ecosystem's sink roots from its compose override, or null. */
    static String ecosystemRoots(Path eco) throws IOException {
        Matcher m = Pattern.compile("SINK_ROOTS_\\w+: (\\S+)").matcher(Files.readString(eco.resolve("compose.yml")));
        return m.find() ? m.group(1) : null;
    }

    static final List<Path> BASE_DIRS = List.of(REPO.resolve("showcases/sample-workers/src/main/pure"),
            REPO.resolve("core/trading-model/src/main/pure"));

    /**
     * Deployments the sinks can run: the base (sample + trading models, docker-compose.yml's roots), and the base plus
     * each ecosystem's compose override. A combination concert's own model loader rejects (e.g. trading-gen re-declares
     * the trading:: packages, so it replaces trading rather than joining it) is not deployable and is skipped.
     */
    @Test
    void eachDeploymentsStoreLayerCompilesAndPlansOnBothStores() throws Exception {
        Map<String, List<String>> failures = new LinkedHashMap<>();
        List<String> skipped = new ArrayList<>();
        String base = baseRoots();
        Map<String, Map.Entry<List<Path>, String>> deployments = new LinkedHashMap<>();
        deployments.put("base", Map.entry(BASE_DIRS, base));
        for (Path eco : ecosystems()) {
            String roots = ecosystemRoots(eco);
            if (roots != null) {
                List<Path> dirs = new ArrayList<>(BASE_DIRS);
                dirs.add(eco.resolve("src/main/pure"));
                deployments.put("base + " + eco.getFileName(), Map.entry(dirs, base + "," + roots));
            }
        }
        for (var d : deployments.entrySet()) {
            List<Path> dirs = d.getValue().getKey();
            String store;
            try {
                store = LegendStoreMain.generate(String.join(",", dirs.stream().map(Path::toString).toList()), d.getValue().getValue(),
                        List.of(Target.DUCKDB, Target.CLICKHOUSE));
            } catch (io.concert.model.pure.PureModelException e) {
                skipped.add(d.getKey() + " (concert itself can't load these models together)");
                continue;
            }
            // the files the sinks actually load (identical duplicates skipped), as the DuckDB sink's /models serves them
            List<Path> files = new ArrayList<>(List.of(CONCERT_PROFILE));
            files.addAll(io.concert.sink.SchemaMapper.load(dirs).files());
            String code = read(files) + "\n" + store;
            List<String> errors = compile(code);
            if (!errors.isEmpty()) {
                failures.put(d.getKey(), errors);
                continue;
            }
            assertPlans(code);
        }
        System.out.println("legend-lite: deployments skipped: " + skipped);
        assertTrue(failures.isEmpty(), "generated store layer rejected: " + failures);
        assertTrue(deployments.size() - skipped.size() >= 2, "base and at least one ecosystem checked; skipped " + skipped);
    }

    /** The Studio project compiles as Studio assembles it: every file its own section, no imports anywhere. */
    @Test
    void theStudioProjectCompiles() throws Exception {
        List<StudioProject.File> files = StudioProjectTest.sampleProject();
        StringBuilder code = new StringBuilder();
        for (StudioProject.File f : files) {
            code.append(f.code().startsWith("###") ? "" : "###Pure\n").append(f.code()).append('\n');
        }
        List<String> errors = compile(code.toString());
        assertTrue(errors.isEmpty(), "Studio project rejected: " + errors);
    }

    private static void assertPlans(String code) throws Exception {
        // One query, both runtimes: a class query through an association, and table access to system columns.
        String classQuery = "demo::order::Order.all()->filter(o|$o.status == demo::order::OrderStatus.DELIVERED)"
                + "->project([o|$o.orderId, o|$o.total.amount, o|$o.lines.sku], ['orderId', 'total', 'sku'])->take(5)";
        String tableQuery = "#>{concert::store::Sink.orders_latest}#->groupBy(~sm_state, ~n: x|$x.entity_id : y|$y->count())";
        for (String query : List.of(classQuery, tableQuery)) {
            JsonNode lambda = post("/api/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false", query, false);
            for (Target t : List.of(Target.DUCKDB, Target.CLICKHOUSE)) {
                JsonNode plan = post("/api/pure/v1/execution/generatePlan", JSON.writeValueAsString(Map.of(
                        "model", Map.of("_type", "text", "code", code), "function", lambda,
                        "runtime", Map.of("_type", "runtimePointer", "runtime", t.runtime()))), true);
                String sql = plan.findValuesAsText("sqlQuery").stream().findFirst().orElse("");
                assertTrue(sql.matches("(?s).*FROM \"?orders_latest\"? AS.*"), t + " reads orders_latest: " + sql);
                if (query == classQuery) {
                    assertTrue(sql.contains("order_lines"), t + " joins the child table: " + sql);
                }
            }
        }
    }
}
