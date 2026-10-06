package io.concert.model.pure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PureParserErrorTest {

    private static String error(String src) {
        PureParseException e = assertThrows(PureParseException.class, () -> PureParser.parse(src, "t.pure"));
        return e.getMessage();
    }

    @Test
    void importWithoutWildcard() {
        assertEquals("t.pure:1:12: expected '::' but found ';'", error("import a::b;"));
    }

    @Test
    void missingSemicolon() {
        assertEquals("t.pure:4:3: expected ';' but found 'y'", error("""
                Class a::A
                {
                  x: String[1]
                  y: Integer[1];
                }
                """));
    }

    @Test
    void invertedMultiplicity() {
        assertEquals("t.pure:2:12: invalid multiplicity [2..1]: upper bound is less than lower bound", error("""
                Class a::A {
                  x: String[2..1];
                }
                """));
    }

    @Test
    void associationWithThreeEnds() {
        assertEquals("t.pure:5:3: association 'a::Triple' must have exactly two ends, found a third end 'c'", error("""
                Association a::Triple
                {
                  a: a::A[1];
                  b: a::B[1];
                  c: a::C[1];
                }
                """));
    }

    @Test
    void associationWithOneEnd() {
        assertEquals("t.pure:1:34: association 'a::Single' must have exactly two ends, found 1",
                error("Association a::Single { a: A[1]; }"));
    }

    @Test
    void unterminatedString() {
        assertEquals("t.pure:2:18: unterminated string literal", error("""
                Class a::A {
                  x: String[1] = 'abc;
                }
                """));
    }

    @Test
    void unknownTopLevelElement() {
        assertEquals("t.pure:3:1: unknown top-level element 'Mapping'; expected import, Class, Enum, Association, Profile or function",
                error("""
                ###Pure
                Class a::A {}
                Mapping a::M ( )
                """));
    }

    @Test
    void badAggregationKind() {
        assertEquals("t.pure:1:15: expected aggregation kind composite, shared or none but found 'strong'",
                error("Class a::A { (strong) x: String[1]; }"));
    }

    @Test
    void unclosedDerivedBody() {
        assertEquals("t.pure:1:18: unclosed '{'", error("Class a::A { f() { $this.x->map(y | 1) : String[1];"));
    }

    @Test
    void unbalancedConstraint() {
        assertEquals("t.pure:1:20: unbalanced '}'", error("Class a::A [ c: (1 } ] { }"));
    }

    @Test
    void missingMultiplicity() {
        assertEquals("t.pure:1:23: expected '[' but found ';'", error("Class a::A { x: String; }"));
    }

    @Test
    void badDefaultLiteral() {
        assertEquals("t.pure:1:29: expected literal value but found ';'", error("Class a::A { x: String[1] = ; }"));
    }
}
