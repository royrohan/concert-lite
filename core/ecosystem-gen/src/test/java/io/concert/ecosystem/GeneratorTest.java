package io.concert.ecosystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** File ownership (generated / stubs / samples), repository edits and generated content, in a temporary repository. */
class GeneratorTest {

    static final String SETTINGS = """
            val modules = linkedMapOf(
                "common" to "core",
                // <concert-ecosystems>
                // </concert-ecosystems>
            )
            """;

    @TempDir
    Path tmp;

    Path repo;
    Path models;

    @BeforeEach
    void setUp() throws IOException {
        repo = Files.createDirectories(tmp.resolve("repo"));
        Files.createDirectories(repo.resolve("showcases"));
        Files.writeString(repo.resolve("settings.gradle.kts"), SETTINGS);
        models = TestModels.dir(tmp, Map.of("m.pure", TestModels.THING));
    }

    private Generator.Report generate(boolean force) {
        return new Generator(repo, Ecosystem.load(models, "things", "io.concert.eco.things"), force).generate(models);
    }

    private Path stub() {
        return repo.resolve("showcases/things/src/main/java/io/concert/eco/things/ThingMachine.java");
    }

    @Test
    void writesTheModule() throws IOException {
        Generator.Report r = generate(false);
        String mod = "showcases/things/";
        for (String f : List.of("build.gradle.kts", "compose.yml", "send", "send-flow", "README.md", "ecosystem.json",
                "src/main/pure/m.pure", "src/main/pure/concert/concert.pure", "samples/thing/finish.json", "samples/thing/drop.json",
                "samples/flows/thing-happy-path.json", "src/main/resources/META-INF/services/io.concert.sdk.MachineCatalog")) {
            assertTrue(Files.isRegularFile(repo.resolve(mod + f)), f);
        }
        for (String c : List.of("ThingsModels", "ThingSpec", "ThingMachine", "ThingsMachines", "ThingsWorkerMain", "ThingsEvents")) {
            assertTrue(Files.isRegularFile(repo.resolve(mod + "src/main/java/io/concert/eco/things/" + c + ".java")), c);
        }
        assertTrue(Files.isRegularFile(repo.resolve("infra/deephaven/app.d/ecosystems/things.py")));
        assertTrue(Files.isExecutable(repo.resolve(mod + "send")));
        assertTrue(r.created().contains(mod + "src/main/java/io/concert/eco/things/ThingMachine.java"));
        assertTrue(Files.readString(repo.resolve("settings.gradle.kts")).contains("    \"things\" to \"showcases\",\n    // </concert-ecosystems>"));

        String spec = Files.readString(repo.resolve(mod + "src/main/java/io/concert/eco/things/ThingSpec.java"));
        assertTrue(spec.contains(".on(\"OPEN\", \"finish\", \"DONE\", io.concert.eco.things.model.t.Finish.class)"), spec);
        assertTrue(spec.contains("Map.entry(\"finish\", List.of(\"user:{ownerId}\"))"), spec);
        assertFalse(spec.contains(".terminal("), "derived terminal states are not listed");

        String compose = Files.readString(repo.resolve(mod + "compose.yml"));
        assertTrue(compose.contains("SINK_ROOTS_THINGS: thing:t::Thing"), compose);
        assertTrue(compose.contains("target: showcase-worker"), compose);
        assertTrue(compose.contains("- ./showcases/things/src/main/pure:/models-eco/things:ro"), compose);

        String dh = Files.readString(repo.resolve("infra/deephaven/app.d/ecosystems/things.py"));
        assertTrue(dh.contains("things_things_latest = _things_root(\"thing\", ["), dh);
        assertTrue(dh.contains("(\"OwnerId\", \"/model/ownerId\", \"string\")"), dh);

        JsonNode flow = SampleGenerator.JSON.readTree(Files.readString(repo.resolve(mod + "samples/flows/thing-happy-path.json")));
        assertEquals("finish", flow.path("events").path(0).path("eventType").asText());
        assertEquals("THING-1001", flow.path("events").path(0).path("instanceKey").asText());
    }

    @Test
    void rerunKeepsEditedStubsAndChangesNothingElse() throws IOException {
        generate(false);
        Files.writeString(stub(), Files.readString(stub()).replace("// TODO OPEN -finish-> DONE", "// done by me"));
        String edited = Files.readString(stub());
        Generator.Report r = generate(false);
        assertEquals(edited, Files.readString(stub()));
        assertTrue(r.kept().contains(repo.relativize(stub()).toString()));
        assertEquals(List.of(), r.created());
        assertEquals(List.of(), r.updated());
        assertEquals(List.of(), r.deleted());
    }

    @Test
    void forceBacksUpAndRegeneratesStubs() throws IOException {
        generate(false);
        String original = Files.readString(stub());
        Files.writeString(stub(), original.replace("// TODO OPEN -finish-> DONE", "// done by me"));
        Generator.Report r = generate(true);
        assertEquals(original, Files.readString(stub()));
        assertEquals(1, r.backedUp().size());
        Path backup = repo.resolve(r.backedUp().getFirst());
        assertTrue(backup.getFileName().toString().startsWith("ThingMachine.java.bak-"), backup.toString());
        assertTrue(Files.readString(backup).contains("// done by me"));
    }

    @Test
    void editedSamplesAreKeptUneditedOnesFollowTheModel() throws IOException {
        generate(false);
        Path finish = repo.resolve("showcases/things/samples/thing/finish.json");
        Path drop = repo.resolve("showcases/things/samples/thing/drop.json");
        Files.writeString(finish, "{\"ownerId\": \"me\"}\n");
        // the model gains a field: the unedited sample is regenerated, the edited one kept
        Files.writeString(models.resolve("m.pure"), TestModels.THING.replace("reason: String[1];", "reason: String[1];\n  note: String[0..1];"));
        Generator.Report r = generate(false);
        assertEquals("{\"ownerId\": \"me\"}\n", Files.readString(finish));
        assertTrue(r.kept().contains(repo.relativize(finish).toString()));
        assertTrue(Files.readString(drop).contains("\"note\""));
        // and stays kept on the next run
        assertTrue(generate(false).kept().contains(repo.relativize(finish).toString()));
    }

    @Test
    void removedRootsAndModelFilesAreCleanedUp() throws IOException {
        generate(false);
        Files.writeString(models.resolve("extra.pure"), "###Pure\nClass t::Extra\n{\n  x: String[0..1];\n}\n");
        generate(false);
        assertTrue(Files.exists(repo.resolve("showcases/things/src/main/pure/extra.pure")));
        Files.delete(models.resolve("extra.pure"));
        Generator.Report r = generate(false);
        assertTrue(r.deleted().contains("showcases/things/src/main/pure/extra.pure"), r.deleted().toString());
    }

    @Test
    void settingsEditIsIdempotent() {
        String once = Generator.addToSettings(SETTINGS, "insurance");
        assertEquals(once, Generator.addToSettings(once, "insurance"));
        String twice = Generator.addToSettings(once, "trading-gen");
        assertTrue(twice.contains("    \"insurance\" to \"showcases\",\n    \"trading-gen\" to \"showcases\",\n    // </concert-ecosystems>"), twice);
    }

    @Test
    void composeAndAllGeneratedFilesAreStableAcrossRuns() throws IOException {
        generate(false);
        Map<Path, String> before = snapshot();
        generate(false);
        assertEquals(before, snapshot());
    }

    private Map<Path, String> snapshot() throws IOException {
        Map<Path, String> out = new java.util.TreeMap<>();
        try (Stream<Path> s = Files.walk(repo)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                out.put(repo.relativize(p), Files.readString(p));
            }
        }
        return out;
    }
}
