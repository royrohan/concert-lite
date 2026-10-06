package io.concert.model.pure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.concert.model.pure.PureLexer.Token;
import io.concert.model.pure.PureLexer.TokenType;
import java.util.List;
import org.junit.jupiter.api.Test;

class PureLexerTest {

    private static List<String> texts(String src) {
        return PureLexer.tokenize(src, "t.pure").stream().map(Token::text).toList();
    }

    @Test
    void multiCharSymbolsAndRanges() {
        assertEquals(List.of("[", "1", "..", "*", "]", "a", "::", "b", "->", "f", "<<", ">>", "==", ">=", "<=", "!=", ""),
                texts("[1..*] a::b->f << >> == >= <= !="));
    }

    @Test
    void numbersStringsAndComments() {
        List<Token> tokens = PureLexer.tokenize("""
                // line comment
                x /* block
                   comment */ 42 100.0 'it\\'s' -1.5e3
                """, "t.pure");
        assertEquals(List.of(TokenType.IDENT, TokenType.INTEGER, TokenType.FLOAT, TokenType.STRING,
                        TokenType.SYMBOL, TokenType.FLOAT, TokenType.EOF),
                tokens.stream().map(Token::type).toList());
        assertEquals("it's", tokens.get(3).text());
        assertEquals(2, tokens.get(0).line());
        assertEquals(1, tokens.get(0).column());
        assertEquals(3, tokens.get(1).line());
        assertEquals(15, tokens.get(1).column());
    }

    @Test
    void nonPureSectionIsSkipped() {
        List<Token> tokens = PureLexer.tokenize("###Pure\na\n###Mapping\nMapping x ( ~src #{ }\n###Pure\nb\n", "t.pure");
        assertEquals(List.of("Pure", "a", "Mapping", "Pure", "b", ""), tokens.stream().map(Token::text).toList());
        assertEquals(TokenType.SECTION, tokens.get(2).type());
        assertEquals(6, tokens.get(4).line());
    }

    @Test
    void unterminatedStringReportsStart() {
        PureParseException e = assertThrows(PureParseException.class,
                () -> PureLexer.tokenize("a\n  b = 'oops;\n", "t.pure"));
        assertEquals("t.pure:2:7: unterminated string literal", e.getMessage());
    }

    @Test
    void unterminatedBlockComment() {
        PureParseException e = assertThrows(PureParseException.class, () -> PureLexer.tokenize("a /* b", "t.pure"));
        assertEquals("t.pure:1:3: unterminated block comment", e.getMessage());
    }
}
