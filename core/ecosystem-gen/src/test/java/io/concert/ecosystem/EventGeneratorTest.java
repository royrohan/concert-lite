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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Generating an event-style ecosystem ({@code concert::event}) and a mixed one. */
class EventGeneratorTest {

    @TempDir
    Path tmp;

    Path repo;
    Path models;

    @BeforeEach
    void setUp() throws IOException {
        repo = Files.createDirectories(tmp.resolve("repo"));
        Files.createDirectories(repo.resolve("showcases"));
        Files.writeString(repo.resolve("settings.gradle.kts"), GeneratorTest.SETTINGS);
        models = TestModels.dir(tmp, Map.of("m.pure", TestModels.EVT));
    }

    private Generator.Report generate(boolean force) {
        return new Generator(repo, Ecosystem.load(models, "shop", "io.concert.eco.shop"), force).generate(models);
    }

    private Path module() {
        return repo.resolve("showcases/shop");
    }

    private String read(String rel) throws IOException {
        return Files.readString(module().resolve(rel));
    }

    @Test
    void writesHandlersCatalogsSamplesAndConfig() throws IOException {
        generate(false);
        String src = "src/main/java/io/concert/eco/shop/";
        String stub = read(src + "CreateHandler.java");
        assertTrue(stub.contains("@Handles(Create.class)"), stub);
        assertTrue(stub.contains("public class CreateHandler implements EventHandler<Create>"), stub);
        assertTrue(stub.contains("// Order order = ctx.state(Order.class, \"order:\" + event.getOrderId()).orElseGet(Order::new);"), stub);
        assertTrue(stub.contains("// ctx.emit(new OrderPlaced().setOrderId(event.getOrderId()));"), stub);
        assertTrue(stub.contains("ctx.emitAt(Instant.now().plus(Duration.ofMinutes(2))"), stub);
        assertTrue(stub.contains("throw new NonBlockingError("), stub);
        assertTrue(Files.exists(module().resolve(src + "PlacedHandler.java")));
        assertTrue(read(src + "ShopEventCatalog.java").contains("List.of(\"order:{orderId}\", \"customer:{customerId}\"), OnError.BLOCKING, 3)"));
        assertTrue(read(src + "ShopEventTypes.java").contains(".add(new CreateHandler())"));
        assertFalse(Files.exists(module().resolve(src + "ShopMachines.java")), "no machines, no machine catalog");
        assertEquals("io.concert.eco.shop.ShopEventCatalog\n",
                read("src/main/resources/META-INF/services/io.concert.sdk.events.EventCatalog"));
        JsonNode flow = SampleGenerator.JSON.readTree(read("samples/event-flows/Create-flow.json"));
        assertEquals("Create -> Placed", flow.path("description").asText());
        assertEquals("event", flow.path("events").path(0).path("style").asText());
        assertTrue(Files.exists(module().resolve("samples/events/Placed.json")));

        String compose = read("compose.yml");
        assertTrue(compose.contains("shop-event-worker:"), compose);
        assertTrue(compose.contains("entrypoint: [\"/app/bin/ecosystem-event-worker\"]"), compose);
        assertFalse(compose.contains("  shop-worker:"), compose);
        assertTrue(compose.contains("SINK_ROOTS_SHOP: evt_create:s::Create,evt_placed:s::OrderPlaced,st_order:s::Order"), compose);
        String build = read("build.gradle.kts");
        assertTrue(build.contains("mainClass.set(\"io.concert.eco.shop.ShopEventWorkerMain\")"), build);
        assertTrue(build.contains("applicationName = \"ecosystem-event-worker\""), build);
        String dh = Files.readString(repo.resolve("infra/deephaven/app.d/ecosystems/shop.py"));
        for (String t : List.of("shop_events_latest", "shop_events_by_status", "shop_event_errors", "shop_events_scheduled",
                "shop_causation_depth", "shop_st_orders_latest")) {
            assertTrue(dh.contains("\"" + t + "\": " + t), t + " in\n" + dh);
        }
        String readme = read("README.md");
        assertTrue(readme.contains("```mermaid\nflowchart LR"), readme);
        assertTrue(readme.contains("  Create --> Placed"), readme);
        JsonNode manifest = SampleGenerator.JSON.readTree(read("ecosystem.json"));
        assertEquals("shop-event-worker", manifest.path("eventService").asText());
        assertEquals("evt_creates", manifest.path("events").path(0).path("table").asText());
    }

    @Test
    void rerunIsIdempotentAndKeepsHandlers() throws IOException {
        generate(false);
        Path stub = module().resolve("src/main/java/io/concert/eco/shop/CreateHandler.java");
        Files.writeString(stub, Files.readString(stub).replace("// TODO Create", "// mine"));
        String edited = Files.readString(stub);
        Generator.Report r = generate(false);
        assertEquals(edited, Files.readString(stub));
        assertTrue(r.kept().contains(repo.relativize(stub).toString()));
        assertEquals(List.of(), r.created());
        assertEquals(List.of(), r.updated());
        assertEquals(List.of(), r.deleted());
        Generator.Report forced = generate(true);
        assertEquals(1, forced.backedUp().size());
        assertTrue(Files.readString(stub).contains("// TODO Create"));
    }

    @Test
    void mixedEcosystemGetsBothWorkers() throws IOException {
        Files.writeString(models.resolve("t.pure"), TestModels.THING);
        generate(false);
        String compose = read("compose.yml");
        assertTrue(compose.contains("  shop-worker:") && compose.contains("  shop-event-worker:"), compose);
        assertTrue(compose.contains("thing:t::Thing,evt_create:s::Create"), compose);
        assertTrue(Files.exists(module().resolve("src/main/java/io/concert/eco/shop/ShopMachines.java")));
        String events = read("src/main/java/io/concert/eco/shop/ShopEvents.java");
        assertTrue(events.contains("ShopEventTypes.isEventType(args[1])") && events.contains("ShopMachines.get(smType)"), events);
    }
}
