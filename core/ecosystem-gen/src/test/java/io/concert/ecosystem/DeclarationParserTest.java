package io.concert.ecosystem;

import static io.concert.ecosystem.TestModels.THING;
import static io.concert.ecosystem.TestModels.pos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeclarationParserTest {

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
    void parsesAValidDeclaration() {
        Ecosystem eco = TestModels.load(tmp, THING);
        assertEquals(List.of(), eco.warnings());
        MachineDecl m = eco.machines().getFirst();
        assertEquals("thing", m.smType());
        assertEquals("t::Thing", m.root().qualifiedName());
        assertEquals("OPEN", m.initial()); // defaults to the first transition's source
        assertEquals(List.of("OPEN -finish-> DONE : t::Finish", "OPEN -drop-> GONE : t::Drop"), m.transitions().stream()
                .map(t -> t.from() + " -" + t.eventType() + "-> " + t.to() + " : " + t.payloadClass()).toList());
        assertEquals(Set.of("DONE", "GONE"), m.terminal());
        assertFalse(m.terminalDeclared());
        assertEquals(Map.of("finish", List.of("user:{ownerId}"), "drop", List.of()), m.locks());
        assertEquals("status", m.statusProperty()); // defaults to an enum property named status
        assertEquals("t::Status", m.statusEnum());
        assertEquals("thingId", m.idProperty()); // defaults to <root>Id
        assertEquals(List.of("finish"), m.happyPath().stream().map(MachineDecl.Transition::eventType).toList());
        // transition entries keep their own position, also on the second line of a multi-line value
        assertEquals(pos(THING, 9, "OPEN"), m.transitions().get(0).location().toString());
        assertEquals(pos(THING, 10, "OPEN"), m.transitions().get(1).location().toString());
    }

    @Test
    void stateListsTerminalOverrideAndStarLocks() {
        String src = THING.replace("concert::sm.locks = 'finish: user:{ownerId}'",
                        "concert::sm.locks = 'finish: user:{ownerId}; *: thing-tag:{id}',\n  concert::sm.terminal = 'DONE'")
                .replace("OPEN -drop-> GONE : Drop'", "OPEN | DONE -drop-> GONE : Drop'");
        Ecosystem eco = TestModels.load(tmp, src);
        MachineDecl m = eco.machines().getFirst();
        assertEquals(Set.of("DONE"), m.terminal());
        assertTrue(m.terminalDeclared());
        assertEquals(List.of("user:{ownerId}", "thing-tag:{id}"), m.locks().get("finish"));
        assertEquals(List.of("thing-tag:{id}"), m.locks().get("drop"));
        assertEquals(3, m.transitions().size());
        // DONE is declared terminal but has an outgoing transition; GONE has none but is not terminal
        assertTrue(eco.warnings().stream().anyMatch(w -> w.contains("terminal state DONE has outgoing transitions")), eco.warnings().toString());
        assertTrue(eco.warnings().stream().anyMatch(w -> w.contains("state GONE has no outgoing transitions but is not terminal")));
    }

    @Test
    void unknownPayloadClass() {
        String src = THING.replace(": Finish;", ": Finnish;");
        assertHas(errors(src), pos(src, 9, "Finnish"), "unknown payload class 'Finnish'");
    }

    @Test
    void payloadNotMarkedAsCommandWarns() {
        String src = THING.replace("Class <<concert::sm.command>> t::Finish", "Class t::Finish");
        Ecosystem eco = TestModels.load(tmp, src);
        assertHas(eco.warnings(), pos(src, 9, "OPEN"), "payload class t::Finish is not marked <<concert::sm.command>>");
    }

    @Test
    void unreachableState() {
        String src = THING.replace("OPEN -drop-> GONE : Drop'", "OPEN -drop-> GONE : Drop;\n    LIMBO -wake-> OPEN : Drop'");
        assertHas(errors(src), pos(src, 11, "LIMBO"), "state LIMBO of thing is unreachable from the initial state OPEN");
    }

    @Test
    void lockTemplateReferencingAFieldNotOnThePayload() {
        String src = THING.replace("user:{ownerId}", "user:{ownrId}");
        assertHas(errors(src), pos(src, 11, "user:"), "references field 'ownrId', which is not a property of t::Finish");
    }

    @Test
    void lockTemplateOnAnUntypedEvent() {
        String src = THING.replace("OPEN -drop-> GONE : Drop'", "OPEN -drop-> GONE'").replace("'finish: user:{ownerId}'",
                "'drop: user:{ownerId}'");
        assertHas(errors(src), pos(src, 11, "user:"), "event drop (OPEN -> GONE) has no payload class");
    }

    @Test
    void statusEnumMismatch() {
        String src = THING.replace("OPEN, DONE, GONE", "OPEN, DONE, LOST");
        List<String> errors = errors(src);
        assertHas(errors, pos(src, 10, "GONE"), "status enum mismatch: state GONE is not a value of t::Status");
    }

    @Test
    void unknownTerminalAndInitialStates() {
        String src = THING.replace("concert::sm.type = 'thing',",
                "concert::sm.type = 'thing', concert::sm.terminal = 'DONE, NOPE', concert::sm.initial = 'START',");
        List<String> errors = errors(src);
        assertHas(errors, pos(src, 8, "NOPE"), "unknown state 'NOPE' in concert::sm.terminal");
        assertHas(errors, pos(src, 8, "'START'"), "unknown state 'START'");
    }

    @Test
    void malformedTransitionAndUnknownLockEvent() {
        String src = THING.replace("OPEN -drop-> GONE : Drop'", "OPEN drop GONE'").replace("'finish: user", "'finsh: user");
        List<String> errors = errors(src);
        assertHas(errors, pos(src, 10, "OPEN"), "malformed transition 'OPEN drop GONE'");
        assertHas(errors, pos(src, 11, "finsh"), "unknown event 'finsh' in concert::sm.locks");
    }

    @Test
    void unknownProfileTagIsAResolverError() {
        String src = THING.replace("concert::sm.locks =", "concert::sm.lock =");
        assertHas(errors(src), pos(src, 11, "'finish"), "unknown tag 'lock' in profile concert::sm");
    }

    @Test
    void rootWithoutTypeAndTagsWithoutRoot() {
        String src = THING.replace("  concert::sm.type = 'thing',\n", "")
                .replace("Class <<concert::sm.command>> t::Drop", "Class <<concert::sm.command>> {concert::sm.type = 'x'} t::Drop");
        List<String> errors = errors(src);
        assertTrue(errors.stream().anyMatch(e -> e.contains("root class t::Thing has no concert::sm.type")), errors.toString());
        assertTrue(errors.stream().anyMatch(e -> e.contains("class t::Drop has concert::sm tags but is not marked <<concert::sm.root>>")),
                errors.toString());
    }

    @Test
    void duplicateSmTypeAndBadSmType() {
        String second = """
                ###Pure
                Class <<concert::sm.root>> {concert::sm.type = 'thing', concert::sm.transitions = 'A -go-> B'} t::Other
                {
                  otherId: String[1];
                }
                Class <<concert::sm.root>> {concert::sm.type = 'Bad-Type', concert::sm.transitions = 'A -go-> B'} t::Third
                {
                  thirdId: String[1];
                }
                """;
        EcosystemException e = assertThrows(EcosystemException.class, () -> Ecosystem.load(
                TestModels.dir(tmp, Map.of("m.pure", THING, "n.pure", second)), "things", "io.concert.eco.things"));
        assertTrue(e.errors().stream().anyMatch(x -> x.startsWith("n.pure:2:") && x.contains("duplicate smType 'thing'")), e.errors().toString());
        assertTrue(e.errors().stream().anyMatch(x -> x.startsWith("n.pure:6:") && x.contains("smType 'Bad-Type' must match")), e.errors().toString());
    }

    @Test
    void parseErrorsCarryTheirLocation() {
        List<String> errors = errors(THING.replace("thingId: String[1];", "thingId String[1];"));
        assertTrue(errors.getFirst().startsWith("m.pure:15:11: expected ':'"), errors.toString());
    }

    @Test
    void noRootAtAll() {
        List<String> errors = errors("###Pure\nClass t::Plain\n{\n  x: String[1];\n}\n");
        assertTrue(errors.getFirst().contains("no class is marked <<concert::sm.root>>"), errors.toString());
    }

    @Test
    void lockKeysThatChangeTheFirstSortedKeyWarn() {
        // "a-owner:" sorts before "thing:", so finish's first key differs from drop's (thing:{id})
        Ecosystem eco = TestModels.load(tmp, THING.replace("user:{ownerId}", "a-user:{ownerId}"));
        assertTrue(eco.warnings().stream().anyMatch(w -> w.contains("do not share their first sorted lock key")), eco.warnings().toString());
    }
}
