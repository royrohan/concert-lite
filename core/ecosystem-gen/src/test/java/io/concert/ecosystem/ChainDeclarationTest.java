package io.concert.ecosystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Event chaining between machines, declared with concert::sm.emits. */
class ChainDeclarationTest {

    @TempDir
    Path tmp;

    static final String CHAIN = """
            ###Pure
            Class <<concert::sm.root>>
            {
              concert::sm.type = 'thing',
              concert::sm.transitions = 'OPEN -start-> WORKING : Start; WORKING -finish-> DONE : Finish',
              concert::sm.emits = 'DONE -> audit.record [{thingId}-A] (note = finished {score}, auditId = {thingId}-A);
                WORKING | DONE -> audit.touch [{thingId}-A]'
            }
            t::Thing
            {
              thingId: String[1];
              ownerId: String[0..1];
            }
            Class <<concert::sm.root>>
            {
              concert::sm.type = 'audit',
              concert::sm.transitions = 'NEW -record-> RECORDED : Record; NEW | RECORDED -touch-> NEW : Touch'
            }
            t::Audit
            {
              auditId: String[1];
              note: String[0..1];
            }
            Class <<concert::sm.command>> t::Start { ownerId: String[1]; }
            Class <<concert::sm.command>> t::Finish { score: Integer[1]; }
            Class <<concert::sm.command>> t::Record { auditId: String[1]; note: String[1]; thingId: String[1]; }
            Class <<concert::sm.command>> t::Touch { thingId: String[1]; }
            """;

    private List<String> errors(String src) {
        return assertThrows(EcosystemException.class, () -> TestModels.load(tmp, src)).errors();
    }

    private static void assertHas(List<String> messages, String text) {
        assertTrue(messages.stream().anyMatch(m -> m.contains(text)), "expected '" + text + "' in\n" + String.join("\n", messages));
    }

    @Test
    void parsesEmitsAndGeneratesThemIntoTheSpec() {
        Ecosystem eco = TestModels.load(tmp, CHAIN);
        MachineDecl thing = eco.machines().getFirst();
        assertEquals(2, thing.emits().size());
        MachineDecl.Emit record = thing.emits().getFirst();
        assertEquals(Set.of("DONE"), record.onEnter());
        assertEquals("audit", record.smType());
        assertEquals("record", record.eventType());
        assertEquals("{thingId}-A", record.keyTemplate());
        assertEquals(Map.of("note", "finished {score}", "auditId", "{thingId}-A"), record.fields());
        assertEquals(Set.of("WORKING", "DONE"), thing.emits().get(1).onEnter());

        String spec = new JavaSources(eco).spec(thing);
        assertTrue(spec.contains(".emit(java.util.Set.of(\"DONE\"), \"audit\", \"record\", \"{thingId}-A\""), spec);
        assertTrue(spec.contains("Map.entry(\"note\", \"finished {score}\")"), spec);
    }

    @Test
    void sampleFlowsStopAtChainedEvents() {
        Ecosystem eco = TestModels.load(tmp, CHAIN.replace("NEW | RECORDED -touch-> NEW : Touch", "RECORDED -touch-> CLOSED : Touch"));
        MachineDecl audit = eco.machines().get(1);
        // audit starts with an event the chain emits: it keeps its full flow (replayed standalone)
        assertEquals(2, new SampleGenerator(eco).happyPath(audit).path("events").size());
    }

    @Test
    void unknownTargetEventAndFields() {
        assertHas(errors(CHAIN.replace("audit.record", "nope.record")), "emit target 'nope' is not a machine of this ecosystem");
        assertHas(errors(CHAIN.replace("audit.record", "audit.nope")), "machine audit has no event 'nope'");
        assertHas(errors(CHAIN.replace("auditId = {thingId}-A", "bogus = x")), "emit field 'bogus' is not a property of t::Record");
        assertHas(errors(CHAIN.replace("finished {score}", "finished {missing}")), "'{missing}' in emit template");
        assertHas(errors(CHAIN.replace("DONE -> audit.record", "LATER -> audit.record")), "unknown state 'LATER' in concert::sm.emits");
        assertHas(errors(CHAIN.replace("DONE -> audit.record [{thingId}-A]", "DONE => audit.record")), "malformed emit");
    }
}
