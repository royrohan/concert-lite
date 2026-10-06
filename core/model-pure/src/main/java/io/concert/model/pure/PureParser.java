package io.concert.model.pure;

import io.concert.model.pure.PureLexer.Token;
import io.concert.model.pure.PureLexer.TokenType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Recursive-descent parser for the structural subset of Legend Pure: classes, enums, associations,
 * profiles and function signatures. Expressions (constraints, derived property and function
 * bodies) are not interpreted; they are captured as raw source text with balanced brackets.
 */
public final class PureParser {

    private static final Map<String, String> CLOSERS = Map.of("(", ")", "[", "]", "{", "}");

    private final String source;
    private final String file;
    private final List<Token> tokens;
    private int pos;

    private PureParser(String source, String file) {
        this.source = source;
        this.file = file;
        this.tokens = PureLexer.tokenize(source, file);
    }

    public static PureModel parse(String source, String file) {
        return new PureParser(source, file).model();
    }

    public static PureModel parse(Path path) throws IOException {
        return parse(Files.readString(path, StandardCharsets.UTF_8), path.getFileName().toString());
    }

    private record Name(String value, Token first) {}

    private record Annotations(List<StereotypeRef> stereotypes, List<TaggedValue> tags) {}

    // ---- top level ----

    private PureModel model() {
        List<ClassDef> classes = new ArrayList<>();
        List<EnumDef> enums = new ArrayList<>();
        List<AssociationDef> associations = new ArrayList<>();
        List<ProfileDef> profiles = new ArrayList<>();
        List<FunctionDef> functions = new ArrayList<>();
        List<String> imports = new ArrayList<>();
        while (peek().type() != TokenType.EOF) {
            Token t = next();
            if (t.type() == TokenType.SECTION) {
                continue;
            }
            if (t.type() != TokenType.IDENT) {
                throw error(t, "unexpected " + describe(t) + "; expected import, Class, Enum, Association, Profile or function");
            }
            switch (t.text()) {
                case "Class" -> classes.add(classDef());
                case "Enum" -> enums.add(enumDef());
                case "Association" -> associations.add(associationDef());
                case "Profile" -> profiles.add(profileDef());
                case "function" -> functions.add(functionDef());
                case "import" -> imports.add(importStatement());
                default -> throw error(t, "unknown top-level element '" + t.text()
                        + "'; expected import, Class, Enum, Association, Profile or function");
            }
        }
        return new PureModel(classes, enums, associations, profiles, functions, imports);
    }

    /** {@code import a::b::*;}; returns the package {@code a::b}. */
    private String importStatement() {
        StringBuilder sb = new StringBuilder(expectIdent("package name").text());
        while (true) {
            expect("::");
            if (accept("*")) {
                break;
            }
            sb.append("::").append(expectIdent("package name or '*'").text());
        }
        expect(";");
        return sb.toString();
    }

    private ClassDef classDef() {
        Annotations ann = annotations();
        Name name = qualifiedName("class name");
        List<String> supers = new ArrayList<>();
        if (accept("extends")) {
            do {
                supers.add(qualifiedName("supertype name").value());
            } while (accept(","));
        }
        List<ConstraintDef> constraints = peek().is("[") ? constraints() : List.of();
        expect("{");
        List<PropertyDef> properties = new ArrayList<>();
        List<DerivedPropertyDef> derived = new ArrayList<>();
        while (!peek().is("}")) {
            Object member = member();
            if (member instanceof PropertyDef p) {
                properties.add(p);
            } else {
                derived.add((DerivedPropertyDef) member);
            }
        }
        expect("}");
        return new ClassDef(name.value(), supers, properties, derived, constraints, ann.stereotypes(), ann.tags(), loc(name.first()));
    }

    private EnumDef enumDef() {
        Annotations ann = annotations();
        Name name = qualifiedName("enum name");
        expect("{");
        List<EnumValueDef> values = new ArrayList<>();
        while (!peek().is("}")) {
            Annotations valueAnn = annotations();
            Token v = expectIdent("enum value");
            values.add(new EnumValueDef(v.text(), valueAnn.stereotypes(), valueAnn.tags(), loc(v)));
            if (!accept(",")) {
                break;
            }
        }
        expect("}");
        return new EnumDef(name.value(), values, ann.stereotypes(), ann.tags(), loc(name.first()));
    }

    private AssociationDef associationDef() {
        Annotations ann = annotations();
        Name name = qualifiedName("association name");
        expect("{");
        List<PropertyDef> ends = new ArrayList<>();
        while (!peek().is("}")) {
            Token start = peek();
            if (!(member() instanceof PropertyDef p)) {
                throw error(start, "association ends cannot be derived properties");
            }
            if (ends.size() == 2) {
                throw error(start, "association '" + name.value() + "' must have exactly two ends, found a third end '"
                        + p.name() + "'");
            }
            ends.add(p);
        }
        Token close = expect("}");
        if (ends.size() != 2) {
            throw error(close, "association '" + name.value() + "' must have exactly two ends, found " + ends.size());
        }
        return new AssociationDef(name.value(), ends.get(0), ends.get(1), ann.stereotypes(), ann.tags(), loc(name.first()));
    }

    private ProfileDef profileDef() {
        Name name = qualifiedName("profile name");
        expect("{");
        List<String> stereotypes = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        while (!peek().is("}")) {
            Token kind = expectIdent("'stereotypes' or 'tags'");
            List<String> target = switch (kind.text()) {
                case "stereotypes" -> stereotypes;
                case "tags" -> tags;
                default -> throw error(kind, "expected 'stereotypes' or 'tags' but found '" + kind.text() + "'");
            };
            expect(":");
            expect("[");
            if (!peek().is("]")) {
                do {
                    target.add(expectIdent(kind.text().substring(0, kind.text().length() - 1) + " name").text());
                } while (accept(","));
            }
            expect("]");
            expect(";");
        }
        expect("}");
        return new ProfileDef(name.value(), stereotypes, tags, loc(name.first()));
    }

    private FunctionDef functionDef() {
        Name name = qualifiedName("function name");
        String params = balanced("(");
        expect(":");
        TypeRef type = type();
        Multiplicity mult = multiplicity();
        String body = balanced("{");
        return new FunctionDef(name.value(), params, type, mult, body, loc(name.first()));
    }

    // ---- members ----

    /** A stored property ({@link PropertyDef}) or a qualified property ({@link DerivedPropertyDef}). */
    private Object member() {
        Annotations ann = annotations();
        Aggregation aggregation = peek().is("(") ? aggregation() : null;
        Token name = expectIdent("property name");
        if (aggregation == null && peek().is("(")) {
            String params = balanced("(");
            String body = balanced("{");
            expect(":");
            TypeRef type = type();
            Multiplicity mult = multiplicity();
            expect(";");
            return new DerivedPropertyDef(name.text(), params, body, type, mult, loc(name));
        }
        expect(":");
        TypeRef type = type();
        Multiplicity mult = multiplicity();
        Literal defaultValue = accept("=") ? literal() : null;
        expect(";");
        return new PropertyDef(name.text(), type, mult, aggregation, defaultValue, ann.stereotypes(), ann.tags(), loc(name));
    }

    private Aggregation aggregation() {
        expect("(");
        Token kind = expectIdent("aggregation kind");
        Aggregation result = switch (kind.text()) {
            case "composite" -> Aggregation.COMPOSITE;
            case "shared" -> Aggregation.SHARED;
            case "none" -> Aggregation.NONE;
            default -> throw error(kind, "expected aggregation kind composite, shared or none but found '" + kind.text() + "'");
        };
        expect(")");
        return result;
    }

    private List<ConstraintDef> constraints() {
        expect("[");
        List<ConstraintDef> result = new ArrayList<>();
        if (!peek().is("]")) {
            do {
                String name = null;
                if (peek().type() == TokenType.IDENT && peek(1).is(":")) {
                    name = next().text();
                    next();
                }
                result.add(new ConstraintDef(name, expressionUntilCommaOrBracket()));
            } while (accept(","));
        }
        expect("]");
        return result;
    }

    private String expressionUntilCommaOrBracket() {
        Token first = peek();
        Token last = null;
        Deque<Token> open = new ArrayDeque<>();
        while (true) {
            Token t = peek();
            if (t.type() == TokenType.EOF) {
                throw error(open.isEmpty() ? t : open.peek(), open.isEmpty()
                        ? "unexpected end of file in constraint"
                        : "unclosed '" + open.peek().text() + "'");
            }
            if (open.isEmpty() && (t.is(",") || t.is("]"))) {
                break;
            }
            trackBrackets(t, open);
            last = next();
        }
        if (last == null) {
            throw error(first, "expected constraint expression but found " + describe(first));
        }
        return source.substring(first.start(), last.end()).strip();
    }

    // ---- annotations, names, types ----

    private Annotations annotations() {
        List<StereotypeRef> stereotypes = new ArrayList<>();
        List<TaggedValue> tags = new ArrayList<>();
        if (accept("<<")) {
            do {
                Name profile = qualifiedName("profile name");
                expect(".");
                stereotypes.add(new StereotypeRef(profile.value(), expectIdent("stereotype name").text()));
            } while (accept(","));
            expect(">>");
        }
        if (accept("{")) {
            do {
                Name profile = qualifiedName("profile name");
                expect(".");
                String tag = expectIdent("tag name").text();
                expect("=");
                Token value = next();
                if (value.type() != TokenType.STRING) {
                    throw error(value, "expected string tag value but found " + describe(value));
                }
                tags.add(new TaggedValue(profile.value(), tag, value.text(), loc(value)));
            } while (accept(","));
            expect("}");
        }
        return new Annotations(stereotypes, tags);
    }

    private Name qualifiedName(String what) {
        Token first = expectIdent(what);
        StringBuilder sb = new StringBuilder(first.text());
        while (peek().is("::")) {
            next();
            sb.append("::").append(expectIdent(what).text());
        }
        return new Name(sb.toString(), first);
    }

    private TypeRef type() {
        String name = qualifiedName("type name").value();
        boolean primitive = !QualifiedNames.isQualified(name) && PrimitiveType.fromPureName(name).isPresent();
        return new TypeRef(name, primitive);
    }

    private Multiplicity multiplicity() {
        Token open = expect("[");
        if (accept("*")) {
            expect("]");
            return Multiplicity.MANY;
        }
        int lower = integer("multiplicity bound");
        Integer upper = lower;
        if (accept("..")) {
            upper = accept("*") ? null : integer("multiplicity upper bound or '*'");
        }
        expect("]");
        if (upper != null && upper < lower) {
            throw error(open, "invalid multiplicity [" + lower + ".." + upper + "]: upper bound is less than lower bound");
        }
        return new Multiplicity(lower, upper);
    }

    private int integer(String what) {
        Token t = next();
        if (t.type() != TokenType.INTEGER) {
            throw error(t, "expected " + what + " but found " + describe(t));
        }
        try {
            return Integer.parseInt(t.text());
        } catch (NumberFormatException e) {
            throw error(t, "integer out of range: " + t.text());
        }
    }

    private Literal literal() {
        Token t = peek();
        if (t.is("[")) {
            next();
            List<Literal> values = new ArrayList<>();
            if (!peek().is("]")) {
                do {
                    values.add(literal());
                } while (accept(","));
            }
            expect("]");
            return new Literal.ListLit(values);
        }
        if (t.type() == TokenType.STRING) {
            next();
            return new Literal.StringLit(t.text());
        }
        if (t.is("true") || t.is("false")) {
            next();
            return new Literal.BoolLit(t.text().equals("true"));
        }
        if (t.type() == TokenType.IDENT) {
            Name enumName = qualifiedName("enum name");
            expect(".");
            return new Literal.EnumLit(enumName.value(), expectIdent("enum value").text());
        }
        boolean negative = false;
        if (t.is("-")) {
            negative = true;
            next();
        }
        Token n = next();
        try {
            if (n.type() == TokenType.INTEGER) {
                long v = Long.parseLong(n.text());
                return new Literal.IntLit(negative ? -v : v);
            }
            if (n.type() == TokenType.FLOAT) {
                double v = Double.parseDouble(n.text());
                return new Literal.FloatLit(negative ? -v : v);
            }
        } catch (NumberFormatException e) {
            throw error(n, "number out of range: " + n.text());
        }
        throw error(n, "expected literal value but found " + describe(n));
    }

    /** Consumes a bracketed region starting at {@code open} and returns its inner raw text. */
    private String balanced(String open) {
        Token start = expect(open);
        Deque<Token> stack = new ArrayDeque<>();
        stack.push(start);
        while (true) {
            Token t = next();
            if (t.type() == TokenType.EOF) {
                throw error(stack.peek(), "unclosed '" + stack.peek().text() + "'");
            }
            trackBrackets(t, stack);
            if (stack.isEmpty()) {
                return source.substring(start.end(), t.start()).strip();
            }
        }
    }

    private void trackBrackets(Token t, Deque<Token> stack) {
        if (t.type() != TokenType.SYMBOL) {
            return;
        }
        if (CLOSERS.containsKey(t.text())) {
            stack.push(t);
        } else if (CLOSERS.containsValue(t.text())) {
            if (stack.isEmpty() || !CLOSERS.get(stack.peek().text()).equals(t.text())) {
                throw error(t, "unbalanced '" + t.text() + "'");
            }
            stack.pop();
        }
    }

    // ---- token helpers ----

    private Token peek() {
        return peek(0);
    }

    private Token peek(int ahead) {
        return tokens.get(Math.min(pos + ahead, tokens.size() - 1));
    }

    private Token next() {
        Token t = peek();
        if (t.type() != TokenType.EOF) {
            pos++;
        }
        return t;
    }

    private boolean accept(String text) {
        if (peek().is(text)) {
            pos++;
            return true;
        }
        return false;
    }

    private Token expect(String text) {
        Token t = peek();
        if (!t.is(text)) {
            throw error(t, "expected '" + text + "' but found " + describe(t));
        }
        return next();
    }

    private Token expectIdent(String what) {
        Token t = peek();
        if (t.type() != TokenType.IDENT) {
            throw error(t, "expected " + what + " but found " + describe(t));
        }
        return next();
    }

    private static String describe(Token t) {
        return switch (t.type()) {
            case EOF -> "end of file";
            case STRING -> "string '" + t.text() + "'";
            case SECTION -> "section '###" + t.text() + "'";
            default -> "'" + t.text() + "'";
        };
    }

    private SourceLocation loc(Token t) {
        return new SourceLocation(file, t.line(), t.column());
    }

    private PureParseException error(Token t, String message) {
        return new PureParseException(message, file, t.line(), t.column());
    }
}
