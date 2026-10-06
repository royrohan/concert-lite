package io.concert.ecosystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.model.runtime.ModelJson;
import io.concert.model.runtime.ModelObject;
import io.concert.sdk.MachineCatalog;
import io.concert.sdk.StateMachineSpec;
import io.concert.sdk.events.EventCatalog;
import io.concert.sdk.events.OnError;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * Generates a module into a temporary repository and compiles its main sources in-process with the
 * model-codegen annotation processor (as Gradle would, with -Werror), then loads the classes: the specs
 * match the declarations, the catalog is discoverable, and every sample binds to and validates against its
 * generated payload class.
 */
class GeneratedModuleCompileTest {

    @TempDir
    Path tmp;

    @ParameterizedTest
    @ValueSource(strings = {"things", "insurance", "trading-gen"})
    void generatedModuleCompilesWithoutWarningsAndSamplesBind(String name) throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Files.createDirectories(repo.resolve("showcases"));
        Files.writeString(repo.resolve("settings.gradle.kts"), GeneratorTest.SETTINGS);
        Path realRepo = Path.of(System.getProperty("concert.repoRoot"));
        Path models = name.equals("things") ? TestModels.dir(tmp, Map.of("m.pure", TestModels.THING)) : realRepo.resolve("examples/" + name);
        Ecosystem eco = Ecosystem.load(models, name, Ecosystem.defaultPackage(name));
        new Generator(repo, eco, false).generate(models);
        Path module = repo.resolve("showcases/" + name);

        Path out = compile(module);

        try (URLClassLoader loader = new URLClassLoader(new URL[] {out.toUri().toURL()}, getClass().getClassLoader())) {
            Class<?> catalogClass = loader.loadClass(eco.javaPackage() + "." + eco.pascalName() + "Machines");
            MachineCatalog catalog = (MachineCatalog) catalogClass.getConstructor().newInstance();
            assertEquals(name, catalog.name());
            assertEquals(eco.machines().stream().map(MachineDecl::smType).toList(), catalog.machines().stream().map(MachineCatalog.Machine::smType).toList());
            int bound = 0;
            for (MachineDecl decl : eco.machines()) {
                MachineCatalog.Machine m = catalog.machines().stream().filter(x -> x.smType().equals(decl.smType())).findFirst().orElseThrow();
                StateMachineSpec spec = m.spec();
                assertEquals(decl.initial(), spec.initialState());
                assertEquals(decl.terminal(), spec.terminalStates());
                assertEquals(decl.transitions().size(), spec.edges().size());
                assertTrue(m.modelMermaid().startsWith("classDiagram"));
                for (StateMachineSpec.Edge e : spec.edges()) {
                    Path sample = module.resolve("samples/" + decl.smType() + "/" + e.eventType() + ".json");
                    if (e.payloadType() == null || !Files.exists(sample)) {
                        continue;
                    }
                    Object payload = ModelJson.read(Files.readString(sample), e.payloadType());
                    assertEquals(List.of(), ((ModelObject) payload).validationErrors(), sample.toString());
                    bound++;
                }
            }
            assertTrue(bound > 0);
        }
    }

    /** Compiles the module's main sources with the model processor and {@code -Werror}; returns the classes dir. */
    private Path compile(Path module) throws Exception {
        List<Path> sources;
        try (Stream<Path> s = Files.walk(module.resolve("src/main/java"))) {
            sources = s.filter(p -> p.toString().endsWith(".java")).toList();
        }
        Path out = Files.createDirectories(tmp.resolve("classes-" + module.getFileName()));
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fm = javac.getStandardFileManager(diagnostics, null, null)) {
            List<String> options = List.of("-d", out.toString(), "-classpath", System.getProperty("java.class.path"),
                    "-processorpath", System.getProperty("concert.processorPath"),
                    "-Aconcert.modelRoots=" + module.resolve("src/main/pure"), "-parameters", "-Xlint:all,-serial,-processing", "-Werror");
            boolean ok = javac.getTask(null, fm, diagnostics, options, null, fm.getJavaFileObjectsFromPaths(sources)).call();
            List<String> messages = new ArrayList<>();
            for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
                if (d.getKind() != Diagnostic.Kind.NOTE) {
                    messages.add(d.getKind() + " " + d.getSource() + ":" + d.getLineNumber() + " " + d.getMessage(null));
                }
            }
            assertTrue(ok && messages.isEmpty(), String.join("\n", messages));
        }

        return out;
    }

    @Test
    void eventStyleModuleCompilesAndItsCatalogsAreDiscoverable() throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Files.createDirectories(repo.resolve("showcases"));
        Files.writeString(repo.resolve("settings.gradle.kts"), GeneratorTest.SETTINGS);
        Path models = Path.of(System.getProperty("concert.repoRoot")).resolve("examples/order-events");
        Ecosystem eco = Ecosystem.load(models, "order-events", Ecosystem.defaultPackage("order-events"));
        new Generator(repo, eco, false).generate(models);
        Path module = repo.resolve("showcases/order-events");
        Path out = compile(module);
        try (URLClassLoader loader = new URLClassLoader(new URL[] {out.toUri().toURL()}, getClass().getClassLoader())) {
            Class<?> types = loader.loadClass(eco.javaPackage() + ".OrderEventsEventTypes");
            @SuppressWarnings("unchecked")
            List<EventCatalog> catalogs = (List<EventCatalog>) types.getField("CATALOGS").get(null);
            assertEquals(List.of("order-events/inventory", "order-events/orders"), catalogs.stream().map(EventCatalog::name).toList());
            List<EventCatalog.EventType> all = catalogs.stream().flatMap(c -> c.events().stream()).toList();
            assertEquals(eco.events().stream().map(EventDecl::name).toList(), all.stream().map(EventCatalog.EventType::name).toList());
            EventCatalog.EventType reserve = all.stream().filter(t -> t.name().equals("ReserveInventoryEvent")).findFirst().orElseThrow();
            assertEquals(List.of("sku:{sku}"), reserve.lockTemplates());
            assertEquals(OnError.NON_BLOCKING, reserve.onError());
            assertEquals(3, reserve.maxAttempts());
            assertTrue(catalogs.getFirst().flowMermaid().startsWith("flowchart LR"));
            assertTrue(catalogs.getFirst().modelMermaid().startsWith("classDiagram"));
            assertEquals(List.of("ReserveInventoryEvent", "OrderExpiryCheckEvent"),
                    catalogs.get(1).emits().get("OrderAcceptedEvent"));
            for (EventCatalog.EventType t : all) {
                Object payload = ModelJson.read(Files.readString(module.resolve("samples/events/" + t.name() + ".json")), t.payloadType());
                assertEquals(List.of(), ((ModelObject) payload).validationErrors(), t.name());
            }
            String services = Files.readString(module.resolve("src/main/resources/META-INF/services/io.concert.sdk.events.EventCatalog"));
            assertEquals(eco.javaPackage() + ".InventoryEventCatalog\n" + eco.javaPackage() + ".OrdersEventCatalog\n", services);
        }
    }
}
