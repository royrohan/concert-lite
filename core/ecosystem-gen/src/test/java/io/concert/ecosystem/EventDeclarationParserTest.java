package io.concert.ecosystem;

import static io.concert.ecosystem.TestModels.EVT;
import static io.concert.ecosystem.TestModels.THING;
import static io.concert.ecosystem.TestModels.pos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The {@code concert::event} declarations: parsing, defaults and {@code file:line:col} errors. */
class EventDeclarationParserTest {

    @TempDir
    Path tmp;

    private List<String> errors(String src) {
        EcosystemException e = assertThrows(EcosystemException.class, () -> TestModels.load(tmp, src));
        return e.errors();
    }

    private static void assertHas(List<String> messages, String location, String text) {
        assertTrue(messages.stream().anyMatch(m -> m.startsWith(location + ": ") && m.contains(text)),
                "expected '" + location + ": ..." + text + "...' in\n" + String.join("\n", messages));
    }

    @Test
    void parsesEventsAndStates() {
        Ecosystem eco = TestModels.load(tmp, EVT);
        assertEquals(List.of(), eco.warnings());
        assertEquals(List.of(), eco.machines());
        assertEquals(List.of("shop"), eco.domains());
        EventDecl create = eco.events().get(0);
        assertEquals("Create", create.name());
        assertEquals(List.of("order:{orderId}", "customer:{customerId}"), create.locks());
        assertEquals("BLOCKING", create.onError());
        assertEquals(3, create.maxAttempts()); // default retries 2
        assertEquals(List.of("Placed"), create.emits());
        assertEquals("evt_create", create.smType());
        assertEquals("CreateHandler", create.handlerClass());
        EventDecl placed = eco.events().get(1);
        assertEquals("Placed", placed.name()); // concert::event.name
        assertEquals("s::OrderPlaced", placed.cls().qualifiedName());
        assertEquals("NON_BLOCKING", placed.onError());
        assertEquals(1, placed.maxAttempts());
        assertEquals("evt_placed", placed.smType());
        EventDecl.StateDecl order = eco.states().getFirst();
        assertEquals("order:{orderId}", order.keyTemplate());
        assertEquals("st_order", order.smType());
        assertEquals("st_orders", order.table());
        assertEquals("s::Create", eco.legendRoot());
    }

    @Test
    void snakeMatchesTheSdk() {
        for (String n : List.of("OrderCreateEvent", "Tick", "HTTPRequestDone", "Order2Shipped", "a_b")) {
            assertEquals(io.concert.sdk.events.EventRows.eventSmType(n), "evt_" + EventDecl.snake(n), n);
        }
    }

    @Test
    void missingAndUnknownDomain() {
        assertHas(errors(EVT.replace("  concert::event.domain = 'shop',\n  concert::event.locks = 'order:{orderId}, customer",
                "  concert::event.locks = 'order:{orderId}, customer")), "m.pure:16:1", "event class s::Create has no concert::event.domain");
        assertHas(errors(EVT.replace("concert::event.domain = 'shop',\n  concert::event.locks = 'order:{orderId}, customer",
                "concert::event.domain = 'Shop!',\n  concert::event.locks = 'order:{orderId}, customer")), pos(EVT, 13, "shop"),
                "unknown domain 'Shop!'");
    }

    @Test
    void lockTemplateFieldMissingOrNotScalar() {
        List<String> e = errors(EVT.replace("customer:{customerId}'", "customer:{client}, tag:{tags}'"));
        assertHas(e, pos(EVT, 14, "customerId"), "references field 'client', which is not a property of s::Create");
        assertTrue(e.stream().anyMatch(m -> m.contains("field 'tags' of s::Create") && m.contains("must be a to-one primitive or enum")),
                e.toString());
    }

    @Test
    void stateKeyFieldMissing() {
        String src = EVT.replace("'order:{orderId}' } s::Order", "'order:{id}' } s::Order");
        assertHas(errors(src), pos(src, 6, "id}"), "references field 'id', which is not a property of s::Order");
    }

    @Test
    void invalidOnErrorAndRetries() {
        List<String> e = errors(EVT.replace("'NON_BLOCKING'", "'SOMETIMES'").replace("retries = '0'", "retries = 'many'"));
        assertHas(e, pos(EVT, 27, "NON_BLOCKING"), "invalid onError 'SOMETIMES': expected BLOCKING or NON_BLOCKING");
        assertHas(e, pos(EVT, 28, "0'"), "invalid retries 'many'");
        assertHas(errors(EVT.replace("retries = '0'", "retries = '99'")), pos(EVT, 28, "0'"), "expected 0..20");
    }

    @Test
    void eventAlsoMarkedSmRootAndTagsWithoutStereotype() {
        assertHas(errors(EVT.replace("Class <<concert::event.event>>\n{\n  concert::event.domain = 'shop',\n  concert::event.locks = 'order:{orderId}',",
                "Class <<concert::event.event, concert::sm.root>>\n{\n  concert::event.domain = 'shop',\n  concert::event.locks = 'order:{orderId}',")),
                "m.pure:31:1", "class s::OrderPlaced is marked <<concert::event.event>> and <<concert::sm.root>>");
        String untagged = EVT.replace("Class <<concert::event.state>> { concert", "Class { concert");
        assertHas(errors(untagged), pos(untagged, 6, "'order"),
                "has concert::event tags but is not marked");
        assertHas(errors(EVT.replace("  concert::event.name = 'Placed'", "  concert::event.name = 'Placed',\n  concert::event.key = 'x'")),
                "m.pure:30:24", "concert::event.key does not apply to an event class");
    }

    @Test
    void duplicateEventNamesAndUnknownEmits() {
        assertHas(errors(EVT.replace("concert::event.name = 'Placed'", "concert::event.name = 'Create'")), pos(EVT, 29, "'Placed'"),
                "duplicate event type name 'Create'");
        assertHas(errors(EVT.replace("emits = 'Placed'", "emits = 'Placed, Shipped'")), "m.pure:15:35",
                "unknown event 'Shipped' in concert::event.emits of Create");
    }

    @Test
    void emitsResolveClassNamesAndMixedModelsWork() {
        Ecosystem eco = TestModels.load(tmp, EVT.replace("emits = 'Placed'", "emits = 'OrderPlaced'")
                + THING.replace("###Pure\n", ""));
        assertEquals(List.of("Placed"), eco.events().getFirst().emits());
        assertEquals(List.of("thing"), eco.machines().stream().map(MachineDecl::smType).toList());
        assertEquals("t::Thing", eco.legendRoot());
    }

    @Test
    void stateWithoutKeyWarnsAndUnlockableKeyWarns() {
        Ecosystem eco = TestModels.load(tmp, EVT.replace("Class <<concert::event.state>> { concert::event.key = 'order:{orderId}' } s::Order",
                "Class <<concert::event.state>> s::Order"));
        assertNull(eco.states().getFirst().keyTemplate());
        assertTrue(eco.warnings().stream().anyMatch(w -> w.contains("has no concert::event.key")), eco.warnings().toString());
        Ecosystem eco2 = TestModels.load(tmp, EVT.replace("'order:{orderId}' } s::Order", "'ord:{orderId}' } s::Order"));
        assertTrue(eco2.warnings().stream().anyMatch(w -> w.contains("matches no event's lock template")), eco2.warnings().toString());
    }
}
