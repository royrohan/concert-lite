package io.concert.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.Env;
import io.concert.common.Json;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Runs {@code docker compose} for the dashboard's Start / Stop / Delete flow and shared-services buttons. In compose the
 * dashboard gets the host's Docker socket and the repo mounted read-only at its host path ({@code CONCERT_REPO}), so
 * relative bind mounts in the compose files resolve to the same host paths as when you run compose yourself. Every
 * call uses the base file plus every ecosystem override, so a service always gets the same configuration (sink-duckdb
 * keeps every ecosystem's models). The environment passed to compose is minimal: only the variables the compose files
 * interpolate, taken from the dashboard's own environment.
 *
 * <p>Local development only: the Docker socket is root on the host.
 */
final class DockerCompose {

    /** Interpolated by the compose files; everything else comes from the repo's .env and the files' defaults. */
    private static final List<String> PASSED_ENV = List.of("STORE_KIND", "KAFKA_BOOTSTRAP", "TRACE_SAMPLE", "SM_TYPE",
            "EVENT_DOMAINS", "DEEPHAVEN_PSK", "KINESIS_SHARDS", "TRADING_ORDERS", "TRADING_RATE", "DOCKER_HOST", "HOME");

    private final Path repo;
    private final List<String> files;
    private final String project = Env.get("COMPOSE_PROJECT_NAME", "concert-temporal");

    DockerCompose(Path repo, List<String> overrideFiles) {
        this.repo = repo;
        List<String> f = new ArrayList<>(List.of("docker-compose.yml"));
        overrideFiles.stream().filter(x -> Files.isRegularFile(repo.resolve(x))).forEach(f::add);
        this.files = List.copyOf(f);
    }

    /** Why compose can't be used here, or {@code null} when it can. */
    String unavailable() {
        if (!Files.isRegularFile(repo.resolve("docker-compose.yml"))) {
            return "the repo is not mounted at " + repo + " (CONCERT_REPO)";
        }
        if (!Files.exists(Path.of("/var/run/docker.sock")) && Env.get("DOCKER_HOST", "").isEmpty()) {
            return "no Docker socket (mount /var/run/docker.sock into the dashboard)";
        }
        return null;
    }

    /** Runs {@code docker compose <args>} with every profile active; streams output lines to {@code log}. */
    int compose(List<String> profiles, List<String> args, Consumer<String> log, long timeoutSec) {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-p", project, "--project-directory", repo.toString()));
        files.forEach(f -> cmd.addAll(List.of("-f", f)));
        profiles.forEach(p -> cmd.addAll(List.of("--profile", p)));
        cmd.addAll(args);
        return run(cmd, log, timeoutSec);
    }

    /**
     * State of every container of the project by service: {@code {service: {state, health, status}}} (state is
     * Docker's: running, exited, created, restarting, ...). One {@code docker ps} call.
     */
    Map<String, Map<String, String>> containers() {
        List<String> lines = new ArrayList<>();
        int rc = run(List.of("docker", "ps", "-a", "--filter", "label=com.docker.compose.project=" + project,
                "--format", "{{json .}}"), lines::add, 15);
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        if (rc != 0) {
            throw new IllegalStateException("docker ps failed: " + String.join(" ", lines));
        }
        for (String line : lines) {
            try {
                JsonNode c = Json.MAPPER.readTree(line);
                String service = label(c.path("Labels").asText(), "com.docker.compose.service");
                if (service == null || service.isEmpty()) {
                    continue;
                }
                String status = c.path("Status").asText();
                String health = status.contains("(healthy)") ? "healthy" : status.contains("(unhealthy)") ? "unhealthy"
                        : status.contains("(health: starting)") ? "starting" : "";
                Map<String, String> m = new LinkedHashMap<>();
                m.put("state", c.path("State").asText());
                m.put("health", health);
                m.put("status", status);
                out.putIfAbsent(service, m);
            } catch (IOException e) {
                // not a JSON line (warnings)
            }
        }
        return out;
    }

    private static String label(String labels, String key) {
        for (String kv : labels.split(",")) {
            int eq = kv.indexOf('=');
            if (eq > 0 && kv.substring(0, eq).equals(key)) {
                return kv.substring(eq + 1);
            }
        }
        return null;
    }

    private int run(List<String> cmd, Consumer<String> log, long timeoutSec) {
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(repo.toFile()).redirectErrorStream(true);
        Map<String, String> env = pb.environment();
        Map<String, String> keep = new LinkedHashMap<>();
        PASSED_ENV.forEach(k -> {
            String v = System.getenv(k);
            if (v != null) {
                keep.put(k, v);
            }
        });
        env.keySet().retainAll(List.of("PATH"));
        env.putAll(keep);
        env.put("PWD", repo.toString());
        env.put("CONCERT_REPO", repo.toString());
        env.putIfAbsent("HOME", "/tmp");
        try {
            Process p = pb.start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    log.accept(line);
                }
            }
            if (!p.waitFor(timeoutSec, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                log.accept("timed out after " + timeoutSec + "s: " + String.join(" ", cmd));
                return -1;
            }
            return p.exitValue();
        } catch (IOException e) {
            log.accept("cannot run docker: " + e.getMessage());
            return -1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
    }
}
