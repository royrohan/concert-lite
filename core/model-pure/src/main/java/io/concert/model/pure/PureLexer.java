package io.concert.model.pure;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits Pure source into tokens. Comments and whitespace are dropped; the content of non-Pure
 * sections ({@code ###Mapping}, {@code ###Runtime}, ...) is skipped up to the next section marker so
 * their foreign syntax never reaches the parser. Tokens carry source offsets so the parser can
 * keep expression bodies as raw text.
 */
public final class PureLexer {

    public enum TokenType {
        IDENT,
        INTEGER,
        FLOAT,
        /** A single-quoted string; {@code text} is the unescaped value. */
        STRING,
        /** Operators and punctuation. */
        SYMBOL,
        /** A {@code ###Name} marker; {@code text} is the section name. */
        SECTION,
        EOF
    }

    /**
     * @param start offset of the first character in the source
     * @param end offset just past the last character in the source
     */
    public record Token(TokenType type, String text, int line, int column, int start, int end) {

        /** True for an identifier or symbol with exactly this text (never for strings). */
        public boolean is(String s) {
            return (type == TokenType.SYMBOL || type == TokenType.IDENT) && text.equals(s);
        }
    }

    private static final String[] MULTI_CHAR_SYMBOLS = {"::", "..", "->", "<<", ">>", "==", ">=", "<=", "!="};

    private final String src;
    private final String file;
    private final List<Token> tokens = new ArrayList<>();
    private int pos;
    private int line = 1;
    private int col = 1;

    private PureLexer(String src, String file) {
        this.src = src;
        this.file = file;
    }

    public static List<Token> tokenize(String source, String file) {
        PureLexer lexer = new PureLexer(source, file);
        lexer.run();
        return List.copyOf(lexer.tokens);
    }

    private void run() {
        while (true) {
            skipWhitespaceAndComments();
            if (pos >= src.length()) {
                tokens.add(new Token(TokenType.EOF, "", line, col, pos, pos));
                return;
            }
            int start = pos;
            int startLine = line;
            int startCol = col;
            char c = src.charAt(pos);
            if (src.startsWith("###", pos)) {
                section(start, startLine, startCol);
            } else if (c == '\'') {
                string(start, startLine, startCol);
            } else if (Character.isDigit(c)) {
                number(start, startLine, startCol);
            } else if (isIdentStart(c)) {
                while (pos < src.length() && isIdentPart(src.charAt(pos))) {
                    advance();
                }
                add(TokenType.IDENT, src.substring(start, pos), start, startLine, startCol);
            } else if (Character.isISOControl(c)) {
                throw new PureParseException(
                        String.format("unexpected character U+%04X", (int) c), file, startLine, startCol);
            } else {
                String sym = String.valueOf(c);
                for (String s : MULTI_CHAR_SYMBOLS) {
                    if (src.startsWith(s, pos)) {
                        sym = s;
                        break;
                    }
                }
                for (int i = 0; i < sym.length(); i++) {
                    advance();
                }
                add(TokenType.SYMBOL, sym, start, startLine, startCol);
            }
        }
    }

    private void section(int start, int startLine, int startCol) {
        advance();
        advance();
        advance();
        int nameStart = pos;
        while (pos < src.length() && isIdentPart(src.charAt(pos))) {
            advance();
        }
        if (pos == nameStart) {
            throw new PureParseException("expected section name after '###'", file, startLine, startCol);
        }
        String name = src.substring(nameStart, pos);
        add(TokenType.SECTION, name, start, startLine, startCol);
        if (!name.equals("Pure")) {
            while (pos < src.length()) {
                boolean newline = src.charAt(pos) == '\n';
                advance();
                if (newline && src.startsWith("###", pos)) {
                    return;
                }
            }
        }
    }

    private void string(int start, int startLine, int startCol) {
        advance();
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= src.length()) {
                throw new PureParseException("unterminated string literal", file, startLine, startCol);
            }
            char c = src.charAt(pos);
            advance();
            if (c == '\'') {
                break;
            }
            if (c == '\\') {
                if (pos >= src.length()) {
                    throw new PureParseException("unterminated string literal", file, startLine, startCol);
                }
                char e = src.charAt(pos);
                advance();
                sb.append(switch (e) {
                    case 'n' -> '\n';
                    case 't' -> '\t';
                    case 'r' -> '\r';
                    default -> e;
                });
            } else {
                sb.append(c);
            }
        }
        add(TokenType.STRING, sb.toString(), start, startLine, startCol);
    }

    private void number(int start, int startLine, int startCol) {
        TokenType type = TokenType.INTEGER;
        digits();
        // A '.' followed by a digit makes a float; "1..*" must stay INTEGER, "..", "*".
        if (pos + 1 < src.length() && src.charAt(pos) == '.' && Character.isDigit(src.charAt(pos + 1))) {
            type = TokenType.FLOAT;
            advance();
            digits();
        }
        if (pos < src.length() && (src.charAt(pos) == 'e' || src.charAt(pos) == 'E')) {
            int save = pos;
            int saveCol = col;
            advance();
            if (pos < src.length() && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) {
                advance();
            }
            if (pos < src.length() && Character.isDigit(src.charAt(pos))) {
                type = TokenType.FLOAT;
                digits();
            } else {
                pos = save;
                col = saveCol;
            }
        }
        add(type, src.substring(start, pos), start, startLine, startCol);
    }

    private void digits() {
        while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
            advance();
        }
    }

    private void skipWhitespaceAndComments() {
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (Character.isWhitespace(c)) {
                advance();
            } else if (src.startsWith("//", pos)) {
                while (pos < src.length() && src.charAt(pos) != '\n') {
                    advance();
                }
            } else if (src.startsWith("/*", pos)) {
                int startLine = line;
                int startCol = col;
                advance();
                advance();
                while (!src.startsWith("*/", pos)) {
                    if (pos >= src.length()) {
                        throw new PureParseException("unterminated block comment", file, startLine, startCol);
                    }
                    advance();
                }
                advance();
                advance();
            } else {
                return;
            }
        }
    }

    private void advance() {
        if (src.charAt(pos) == '\n') {
            line++;
            col = 1;
        } else {
            col++;
        }
        pos++;
    }

    private void add(TokenType type, String text, int start, int startLine, int startCol) {
        tokens.add(new Token(type, text, startLine, startCol, start, pos));
    }

    private static boolean isIdentStart(char c) {
        return c == '_' || (c < 128 && Character.isLetter(c));
    }

    private static boolean isIdentPart(char c) {
        return c == '_' || (c < 128 && Character.isLetterOrDigit(c));
    }
}
