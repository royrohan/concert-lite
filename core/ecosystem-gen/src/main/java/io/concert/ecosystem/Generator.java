package io.concert.ecosystem;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ConcertProfile;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.PureModel;
import io.concert.model.pure.PureParser;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Writes (or updates) an ecosystem module in a repository checkout. Three kinds of files:
 *
 * <ul>
 *   <li><b>generated</b> (specs, registries, worker main, event sender, build, compose, README, ...): always
 *       rewritten; generated files of an earlier run that are no longer produced are deleted;
 *   <li><b>stubs</b> ({@code <Root>Machine.java}, {@code <Event>Handler.java}): written only if absent; {@code force} backs the existing
 *       file up as {@code <file>.bak-<timestamp>} and rewrites it;
 *   <li><b>samples</b>: rewritten unless the user edited them since they were generated (their hashes are
 *       kept in {@code ecosystem.json}); {@code force} rewrites them after a backup.
 * </ul>
 */
final class Generator {

    /** What a run did, by path relative to the repository root. */
    record Report(List<String> created, List<String> updated, List<String> unchanged, List<String> kept, List<String> backedUp,
            List<String> deleted, List<String> warnings) {

        String summary() {
            StringBuilder sb = new StringBuilder();
            created.forEach(p -> sb.append("  created   ").append(p).append('\n'));
            updated.forEach(p -> sb.append("  updated   ").append(p).append('\n'));
            kept.forEach(p -> sb.append("  kept      ").append(p).append("  (yours)\n"));
            backedUp.forEach(p -> sb.append("  backup    ").append(p).append('\n'));
            deleted.forEach(p -> sb.append("  deleted   ").append(p).append('\n'));
            sb.append("  ").append(unchanged.size()).append(" file(s) unchanged\n");
            return sb.toString();
        }
    }

    private enum Kind { GENERATED, STUB, SAMPLE }

    private final Path repo;
    private final Ecosystem eco;
    private final boolean force;
    private final String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));

    private final List<String> created = new ArrayList<>();
    private final List<String> updated = new ArrayList<>();
    private final List<String> unchanged = new ArrayList<>();
    private final List<String> kept = new ArrayList<>();
    private final List<String> backedUp = new ArrayList<>();
    private final List<String> deleted = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final Set<String> generated = new LinkedHashSet<>();
    private final Map<String, String> sampleHashes = new LinkedHashMap<>();
    private Map<String, String> previousSampleHashes = Map.of();

    Generator(Path repo, Ecosystem eco, boolean force) {
        this.repo = repo;
        this.eco = eco;
        this.force = force;
    }

    Report generate(Path modelsDir) {
        JavaSources java = new JavaSources(eco);
        ProjectFiles files = new ProjectFiles(eco, java);
        SampleGenerator samples = new SampleGenerator(eco);
        String module = files.moduleDir();
        Path moduleDir = repo.resolve(module);
        JsonNode previous = readJson(moduleDir.resolve("ecosystem.json"));
        List<String> previousGenerated = new ArrayList<>();
        if (previous != null) {
            Map<String, String> h = new LinkedHashMap<>();
            previous.path("sampleHashes").properties().forEach(e -> h.put(e.getKey(), e.getValue().asText()));
            previousSampleHashes = h;
            previous.path("generatedFiles").forEach(n -> previousGenerated.add(n.asText()));
        }

        // models: copies of the models dir plus the built-in profile
        String pure = module + "/src/main/pure/";
        for (String f : eco.files()) {
            write(pure + f, eco.sources().get(f), Kind.GENERATED);
        }
        write(pure + "concert/" + ConcertProfile.FILE_NAME, ConcertProfile.source(), Kind.GENERATED);

        String src = module + "/src/main/java/" + eco.javaPackage().replace('.', '/') + "/";
        String testSrc = module + "/src/test/java/" + eco.javaPackage().replace('.', '/') + "/";
        write(src + java.modelsClass() + ".java", java.models(), Kind.GENERATED);
        if (eco.hasMachines()) {
            for (MachineDecl m : eco.machines()) {
                write(src + JavaSources.specClass(m) + ".java", java.spec(m), Kind.GENERATED);
                write(src + JavaSources.machineClass(m) + ".java", java.stub(m), Kind.STUB);
            }
            write(src + java.machinesClass() + ".java", java.machines(), Kind.GENERATED);
            write(src + java.workerMainClass() + ".java", java.workerMain(), Kind.GENERATED);
            write(module + "/src/main/resources/META-INF/services/io.concert.sdk.MachineCatalog",
                    java.pkg() + "." + java.machinesClass() + "\n", Kind.GENERATED);
            write(testSrc + java.testClass() + ".java", java.test(), Kind.GENERATED);
        }
        write(src + java.eventsClass() + ".java", java.events(), Kind.GENERATED);
        if (eco.hasEvents()) {
            EventSources ev = new EventSources(eco);
            StringBuilder services = new StringBuilder();
            for (String d : eco.domains()) {
                write(src + EventSources.catalogClass(d) + ".java", ev.catalog(d), Kind.GENERATED);
                services.append(java.pkg()).append('.').append(EventSources.catalogClass(d)).append('\n');
            }
            write(module + "/src/main/resources/META-INF/services/io.concert.sdk.events.EventCatalog", services.toString(), Kind.GENERATED);
            write(src + ev.typesClass() + ".java", ev.types(), Kind.GENERATED);
            write(src + ev.workerMainClass() + ".java", ev.workerMain(), Kind.GENERATED);
            for (EventDecl e : eco.events()) {
                write(src + e.handlerClass() + ".java", ev.stub(e), Kind.STUB);
            }
            write(testSrc + ev.testClass() + ".java", ev.test(java.eventsClass()), Kind.GENERATED);
        }

        for (MachineDecl m : eco.machines()) {
            samples.samples(m).forEach((event, payload) -> {
                if (payload != null) {
                    write(module + "/samples/" + m.smType() + "/" + event + ".json", SampleGenerator.pretty(payload), Kind.SAMPLE);
                }
            });
            write(module + "/samples/flows/" + m.smType() + "-happy-path.json", SampleGenerator.pretty(samples.happyPath(m)), Kind.SAMPLE);
        }
        for (EventDecl e : eco.events()) {
            write(module + "/samples/events/" + e.name() + ".json", SampleGenerator.pretty(samples.eventSample(e)), Kind.SAMPLE);
        }
        for (EventDecl root : samples.rootEvents()) {
            write(module + "/samples/event-flows/" + root.name() + "-flow.json", SampleGenerator.pretty(samples.eventFlow(root)), Kind.SAMPLE);
        }

        write(module + "/build.gradle.kts", files.build(), Kind.GENERATED);
        write(module + "/compose.yml", files.compose(), Kind.GENERATED);
        write(module + "/send", files.sendScript(), Kind.GENERATED);
        write(module + "/send-flow", files.sendFlowScript(), Kind.GENERATED);
        executable(module + "/send");
        executable(module + "/send-flow");
        write(module + "/README.md", files.readme(samples), Kind.GENERATED);
        write("infra/deephaven/app.d/ecosystems/" + eco.name() + ".py", files.deephaven(), Kind.GENERATED);

        // stale model copies and generated files of earlier runs
        deleteStalePure(moduleDir.resolve("src/main/pure"));
        for (String p : previousGenerated) {
            if (!generated.contains(p) && !p.equals(module + "/ecosystem.json") && Files.isRegularFile(repo.resolve(p))) {
                delete(p);
            }
        }

        checkCollisions();
        editSettings();

        ObjectNode manifest = files.manifest(repo.relativize(modelsDir.toAbsolutePath().normalize()).toString());
        manifest.put("generator", "./generate-concert-ecosystem");
        ObjectNode hashes = manifest.putObject("sampleHashes");
        sampleHashes.forEach(hashes::put);
        var gen = manifest.putArray("generatedFiles");
        generated.forEach(gen::add);
        write(module + "/ecosystem.json", SampleGenerator.pretty(manifest), Kind.GENERATED);

        List<String> allWarnings = new ArrayList<>(eco.warnings());
        allWarnings.addAll(warnings);
        return new Report(created, updated, unchanged, kept, backedUp, deleted, allWarnings);
    }

    // ---- writing ----

    private void write(String path, String content, Kind kind) {
        Path file = repo.resolve(path);
        try {
            boolean exists = Files.isRegularFile(file);
            String current = exists ? Files.readString(file, StandardCharsets.UTF_8) : null;
            switch (kind) {
                case GENERATED -> generated.add(path);
                case STUB -> {
                    if (exists && !force) {
                        kept.add(path);
                        return;
                    }
                    if (exists && !current.equals(content)) {
                        backup(path, current);
                    }
                }
                case SAMPLE -> {
                    String recorded = previousSampleHashes.get(path);
                    boolean edited = exists && (recorded == null || !recorded.equals(sha(current)));
                    if (edited && !force) {
                        if (!current.equals(content)) {
                            kept.add(path);
                            sampleHashes.put(path, recorded == null ? "edited" : recorded);
                            return;
                        }
                    } else if (edited && !current.equals(content)) {
                        backup(path, current);
                    }
                    sampleHashes.put(path, sha(content));
                }
            }
            if (content.equals(current)) {
                unchanged.add(path);
                return;
            }
            Files.createDirectories(file.getParent());
            Files.writeString(file, content, StandardCharsets.UTF_8);
            (exists ? updated : created).add(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void backup(String path, String content) throws IOException {
        String bak = path + ".bak-" + stamp;
        Files.writeString(repo.resolve(bak), content, StandardCharsets.UTF_8);
        backedUp.add(bak);
    }

    private void delete(String path) {
        try {
            Files.deleteIfExists(repo.resolve(path));
            deleted.add(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void executable(String path) {
        try {
            Files.setPosixFilePermissions(repo.resolve(path), PosixFilePermissions.fromString("rwxr-xr-x"));
        } catch (UnsupportedOperationException | IOException e) {
            warnings.add(path + ": warning: could not make it executable (" + e.getMessage() + ")");
        }
    }

    private void deleteStalePure(Path dir) {
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : s.filter(x -> x.toString().endsWith(".pure")).toList()) {
                if (!eco.files().contains(p.getFileName().toString())) {
                    delete(repo.relativize(p).toString());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String sha(String content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static JsonNode readJson(Path p) {
        try {
            return Files.isRegularFile(p) ? SampleGenerator.JSON.readTree(Files.readString(p)) : null;
        } catch (IOException e) {
            return null;
        }
    }

    // ---- repository edits ----

    static final String SETTINGS_BEGIN = "// <concert-ecosystems>";
    static final String SETTINGS_END = "// </concert-ecosystems>";

    /** Adds {@code "<name>" to "showcases",} between the ecosystem markers of settings.gradle.kts, once. */
    private void editSettings() {
        Path settings = repo.resolve("settings.gradle.kts");
        try {
            String s = Files.readString(settings, StandardCharsets.UTF_8);
            String edited = addToSettings(s, eco.name());
            if (edited.equals(s)) {
                unchanged.add("settings.gradle.kts");
            } else {
                Files.writeString(settings, edited, StandardCharsets.UTF_8);
                updated.add("settings.gradle.kts");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String addToSettings(String settings, String name) {
        int begin = settings.indexOf(SETTINGS_BEGIN);
        int end = settings.indexOf(SETTINGS_END);
        if (begin < 0 || end < begin) {
            throw new IllegalStateException("settings.gradle.kts has no " + SETTINGS_BEGIN + " ... " + SETTINGS_END
                    + " block inside the modules map");
        }
        String entry = "\"" + name + "\" to \"showcases\",";
        String block = settings.substring(begin, end);
        if (block.lines().anyMatch(l -> l.strip().equals(entry))) {
            return settings;
        }
        int lineStart = settings.lastIndexOf('\n', end) + 1;
        String indent = settings.substring(lineStart, end);
        return settings.substring(0, lineStart) + indent + entry + "\n" + settings.substring(lineStart);
    }

    /** Warns about Pure element names this ecosystem shares with other model directories the sinks may load. */
    private void checkCollisions() {
        Set<String> mine = new LinkedHashSet<>();
        eco.model().classes().stream().map(ClassDef::qualifiedName).forEach(mine::add);
        eco.model().enums().stream().map(EnumDef::qualifiedName).forEach(mine::add);
        mine.remove(ConcertProfile.NAME);
        List<Path> others = new ArrayList<>(List.of(repo.resolve("showcases/sample-workers/src/main/pure"),
                repo.resolve("core/trading-model/src/main/pure")));
        try (Stream<Path> s = Files.list(repo.resolve("showcases"))) {
            s.filter(d -> Files.isRegularFile(d.resolve("ecosystem.json")) && !d.getFileName().toString().equals(eco.name()))
                    .map(d -> d.resolve("src/main/pure")).forEach(others::add);
        } catch (IOException e) {
            return;
        }
        for (Path dir : others) {
            if (!Files.isDirectory(dir)) {
                continue;
            }
            Set<String> theirs = new LinkedHashSet<>();
            try (Stream<Path> s = Files.list(dir)) {
                for (Path p : s.filter(x -> x.toString().endsWith(".pure")).toList()) {
                    PureModel m = PureParser.parse(p);
                    m.classes().forEach(c -> theirs.add(c.qualifiedName()));
                    m.enums().forEach(c -> theirs.add(c.qualifiedName()));
                }
            } catch (IOException | RuntimeException e) {
                continue;
            }
            theirs.retainAll(mine);
            if (!theirs.isEmpty()) {
                warnings.add(repo.relativize(dir) + ": warning: defines " + theirs.size() + " of this ecosystem's Pure names (e.g. "
                        + theirs.iterator().next() + "); the analytics sinks resolve all deployed models together, so do not "
                        + "deploy both");
            }
        }
    }
}
