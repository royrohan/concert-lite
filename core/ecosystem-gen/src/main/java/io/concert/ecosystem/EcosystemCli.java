package io.concert.ecosystem;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Command line of the ecosystem generator (wrapped by {@code ./generate-concert-ecosystem} and
 * {@code ./deploy-concert-ecosystem}).
 *
 * <pre>
 * generate &lt;models-dir&gt; --name &lt;name&gt; [--package io.concert.eco.&lt;name&gt;] [--force] [--repo DIR]
 * check    &lt;models-dir&gt; [--name &lt;name&gt;]                 validate only
 * summary  &lt;name&gt; [--store S] [--no-analytics] [--repo DIR]   what deploy prints at the end
 * </pre>
 *
 * Exit codes: 0 ok, 1 model errors, 2 usage.
 */
public final class EcosystemCli {
    private EcosystemCli() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        if (args.length == 0) {
            return usage();
        }
        List<String> positional = new ArrayList<>();
        String name = null;
        String pkg = null;
        String store = "postgres";
        boolean force = false;
        boolean analytics = true;
        Path repo = Path.of(System.getProperty("concert.repoRoot", ".")).toAbsolutePath().normalize();
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--name" -> name = args[++i];
                case "--package" -> pkg = args[++i];
                case "--force" -> force = true;
                case "--repo" -> repo = Path.of(args[++i]).toAbsolutePath().normalize();
                case "--store" -> store = args[++i];
                case "--no-analytics" -> analytics = false;
                default -> {
                    if (args[i].startsWith("--")) {
                        System.err.println("unknown option " + args[i]);
                        return usage();
                    }
                    positional.add(args[i]);
                }
            }
        }
        try {
            return switch (args[0]) {
                case "generate", "check" -> {
                    if (positional.size() != 1 || (name == null && args[0].equals("generate"))) {
                        yield usage();
                    }
                    Path dir = Path.of(positional.getFirst()).toAbsolutePath().normalize();
                    if (!Files.isDirectory(dir)) {
                        System.err.println("not a directory: " + dir);
                        yield 2;
                    }
                    String n = name != null ? name : dir.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
                    Ecosystem eco = Ecosystem.load(dir, n, pkg != null ? pkg : Ecosystem.defaultPackage(n));
                    eco.warnings().forEach(System.err::println);
                    if (args[0].equals("check")) {
                        System.out.println("ok: " + describe(eco));
                        yield 0;
                    }
                    Generator.Report r = new Generator(repo, eco, force).generate(dir);
                    r.warnings().stream().filter(w -> !eco.warnings().contains(w)).forEach(System.err::println);
                    System.out.println("ecosystem " + n + " -> showcases/" + n + " (" + describe(eco) + ")");
                    System.out.print(r.summary());
                    System.out.println("next: ./deploy-concert-ecosystem " + n + " --store spanner   (or ./gradlew :" + n + ":build)");
                    yield 0;
                }
                case "summary" -> {
                    if (positional.size() != 1) {
                        yield usage();
                    }
                    System.out.print(summary(repo, positional.getFirst(), store, analytics));
                    yield 0;
                }
                default -> usage();
            };
        } catch (EcosystemException e) {
            e.warnings().forEach(System.err::println);
            e.errors().forEach(System.err::println);
            System.err.println(e.errors().size() + " error(s); nothing generated");
            return 1;
        } catch (IOException e) {
            System.err.println(e);
            return 1;
        }
    }

    /** {@code 2 state machine(s): claim, policy; 5 event type(s) in domains orders, inventory; 2 keyed state(s)}. */
    static String describe(Ecosystem eco) {
        List<String> parts = new ArrayList<>();
        if (eco.hasMachines() || !eco.hasEvents()) {
            parts.add(eco.machines().size() + " state machine(s): " + String.join(", ", eco.machines().stream().map(MachineDecl::smType).toList()));
        }
        if (eco.hasEvents()) {
            parts.add(eco.events().size() + " event type(s) in domain(s) " + String.join(", ", eco.domains()) + ": "
                    + String.join(", ", eco.events().stream().map(EventDecl::name).toList()));
            parts.add(eco.states().size() + " keyed state(s)" + (eco.states().isEmpty() ? "" : ": "
                    + String.join(", ", eco.states().stream().map(EventDecl.StateDecl::name).toList())));
        }
        return String.join("; ", parts);
    }

    private static int usage() {
        System.err.println("""
                usage: generate <models-dir> --name <name> [--package io.concert.eco.<name>] [--force]
                       check <models-dir> [--name <name>]
                       summary <name> [--store postgres|dynamo|spanner] [--no-analytics]""");
        return 2;
    }

    /** The deploy printout: UI links, event commands and example SQL, from the module's ecosystem.json. */
    static String summary(Path repo, String name, String store, boolean analytics) throws IOException {
        Path manifestFile = repo.resolve("showcases").resolve(name).resolve("ecosystem.json");
        JsonNode m = SampleGenerator.JSON.readTree(Files.readString(manifestFile));
        String mod = "showcases/" + name;
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== ecosystem ").append(name).append(" deployed (store: ").append(store).append(") ===\n\n");
        sb.append("UIs\n");
        sb.append("  Temporal UI       http://localhost:8080/namespaces/default/workflows?query=")
                .append(java.net.URLEncoder.encode(temporalQuery(m), java.nio.charset.StandardCharsets.UTF_8)).append('\n');
        sb.append("  Trace UI          http://localhost:8088   (Analytics tab: http://localhost:8088/#analytics)\n");
        for (JsonNode r : m.path("roots")) {
            sb.append("    entity page     http://localhost:8088/#entity/").append(r.path("smType").asText()).append(':')
                    .append(r.path("sampleKey").asText()).append("   (after its happy path)\n");
        }
        if (m.has("events")) {
            sb.append("    events view     http://localhost:8088/#events   (error queue with Retry / Skip, scheduled, processors)\n");
            sb.append("    event page      http://localhost:8088/#event/<eventId>   (lifecycle, payload, causation tree)\n");
        }
        if (analytics) {
            sb.append("  ClickHouse Play   http://localhost:8123/play   (user readonly / readonly)\n");
            sb.append("  DuckDB sink       http://localhost:8090/tables\n");
            sb.append("  Deephaven IDE     http://localhost:10000/ide/?psk=").append(env("DEEPHAVEN_PSK", "concert")).append('\n');
            for (JsonNode t : m.path("deephavenTables")) {
                sb.append("    widget          http://localhost:10000/iframe/widget/?name=").append(t.asText()).append("&psk=").append(env("DEEPHAVEN_PSK", "concert")).append('\n');
            }
            sb.append("  Redpanda Console  http://localhost:8081   ").append(reachable("http://localhost:8081") ? "(running)"
                    : "(not running: COMPOSE_PROFILES=kafka-ui docker compose up -d redpanda-console)").append('\n');
        }
        sb.append("\nSend events\n");
        sb.append("  ./").append(mod).append("/send list\n");
        for (JsonNode e : m.path("events")) {
            sb.append("  ./").append(mod).append("/send ").append(e.path("eventType").asText()).append("   [").append(mod).append('/')
                    .append(e.path("sample").asText()).append("]   (--at +5m to schedule)\n");
        }
        for (JsonNode f : m.path("eventFlows")) {
            sb.append("  ./").append(mod).append("/send-flow ").append(mod).append('/').append(f.asText()).append('\n');
        }
        if (m.has("events")) {
            sb.append("  scripts/events.sh list all            # lifecycle rows; list errors | retry <id> | skip <id> [reason]\n");
        }
        for (JsonNode r : m.path("roots")) {
            String type = r.path("smType").asText();
            String first = r.path("events").path(0).asText();
            sb.append("  ./").append(mod).append("/send ").append(type).append(' ').append(first).append(' ').append(r.path("sampleKey").asText())
                    .append("   [").append(mod).append("/samples/").append(type).append('/').append(first).append(".json]\n");
        }
        sb.append("  ./").append(mod).append("/send-flow all                         # every happy path\n");
        for (JsonNode r : m.path("roots")) {
            sb.append("  ./").append(mod).append("/send-flow ").append(mod).append('/').append(r.path("happyPath").asText()).append('\n');
        }
        if (analytics) {
            sb.append("\nExample SQL (DuckDB via trace UI -> Analytics -> DuckDB; ClickHouse adds FINAL)\n");
            for (JsonNode r : m.path("roots")) {
                String table = r.path("table").asText();
                sb.append("  SELECT sm_state, count(*) AS entities, avg(date_diff('millisecond', created_at, completed_at)) AS avg_ms FROM ")
                        .append(table).append(" GROUP BY sm_state;\n");
            }
            for (JsonNode r : m.path("roots")) {
                sb.append("  SELECT * FROM ").append(r.path("table").asText()).append(" FINAL ORDER BY completed_at DESC LIMIT 10;   -- ClickHouse\n");
            }
            for (JsonNode e : m.path("events")) {
                sb.append("  SELECT status, count(*), avg(attempts) FROM ").append(e.path("table").asText()).append(" GROUP BY status;\n");
            }
            for (JsonNode st : m.path("states")) {
                sb.append("  SELECT * FROM ").append(st.path("table").asText()).append(" ORDER BY completed_at DESC LIMIT 10;\n");
            }
            String first = m.path("roots").path(0).path("table").asText(m.path("events").path(0).path("table").asText());
            sb.append("  curl -s localhost:8088/api/analytics/duckdb/query -d \"SELECT count(*) FROM ").append(first).append("\"\n");
        }
        return sb.toString();
    }

    private static String temporalQuery(JsonNode m) {
        List<String> parts = new ArrayList<>();
        for (JsonNode r : m.path("roots")) {
            parts.add("WorkflowId STARTS_WITH \"" + r.path("smType").asText() + ":\"");
        }
        for (JsonNode d : m.path("domains")) {
            parts.add("WorkflowId STARTS_WITH \"evproc:" + d.asText() + ":\"");
        }
        return String.join(" OR ", parts);
    }

    private static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? dflt : v;
    }

    private static boolean reachable(String url) {
        try {
            HttpClient c = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
            c.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.discarding());
            return true;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
