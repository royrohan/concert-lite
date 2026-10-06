package io.concert.model.codegen;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import javax.lang.model.SourceVersion;

/**
 * Maps Pure names to valid Java identifiers, packages and accessor names. Public so tools that write
 * code against the generated classes (e.g. the ecosystem generator) name them exactly as
 * {@link JavaGenerator} does.
 */
public final class JavaNames {

    private JavaNames() {}

    /** Makes {@code name} a valid Java identifier: invalid characters become {@code _}, keywords get a {@code _} suffix. */
    public static String identifier(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = i == 0 ? Character.isJavaIdentifierStart(c) : Character.isJavaIdentifierPart(c);
            if (!ok && i == 0 && Character.isJavaIdentifierPart(c)) {
                sb.append('_').append(c);
            } else {
                sb.append(ok ? c : '_');
            }
        }
        String id = sb.isEmpty() ? "_" : sb.toString();
        return SourceVersion.isKeyword(id) ? id + "_" : id;
    }

    /** Java package for Pure package {@code a::b} under {@code basePackage}. */
    public static String javaPackage(String basePackage, String purePackage) {
        if (purePackage.isEmpty()) {
            return basePackage;
        }
        StringBuilder sb = new StringBuilder(basePackage);
        for (String segment : purePackage.split("::")) {
            sb.append('.').append(identifier(segment));
        }
        return sb.toString();
    }

    public static String capitalize(String name) {
        return name.isEmpty() ? name : Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    public static String getter(String property) {
        String g = "get" + capitalize(property);
        // Object.getClass() is final.
        return identifier(g.equals("getClass") ? g + "_" : g);
    }

    public static String setter(String property) {
        return identifier("set" + capitalize(property));
    }

    /** Irregular plurals, matched as the whole name or as its last camel-case word. */
    private static final Map<String, String> IRREGULAR = new LinkedHashMap<>();

    static {
        IRREGULAR.put("people", "person");
        IRREGULAR.put("children", "child");
        IRREGULAR.put("men", "man");
        IRREGULAR.put("women", "woman");
        IRREGULAR.put("statuses", "status");
        IRREGULAR.put("aliases", "alias");
        IRREGULAR.put("indices", "index");
        IRREGULAR.put("criteria", "criterion");
    }

    /**
     * The singular used for adder names ({@code lines} gives {@code addLine}): irregular plurals from a
     * small table, then {@code ies→y}, {@code sses→ss}, {@code ches/shes/xes→} drop {@code es}; names
     * ending in {@code ss} or {@code us} are kept, otherwise a trailing {@code s} is dropped. The
     * generator falls back to {@code addTo<Property>} when the result collides with another member.
     */
    static String singular(String property) {
        for (Map.Entry<String, String> e : IRREGULAR.entrySet()) {
            String plural = e.getKey();
            if (property.equals(plural)) {
                return e.getValue();
            }
            String word = capitalize(plural);
            if (property.endsWith(word) && property.length() > word.length()) {
                return property.substring(0, property.length() - word.length()) + capitalize(e.getValue());
            }
        }
        String lower = property.toLowerCase(Locale.ROOT);
        if (property.length() <= 2 || lower.endsWith("ss") || lower.endsWith("us")) {
            return property;
        }
        if (lower.endsWith("ies") && property.length() > 4) {
            return property.substring(0, property.length() - 3) + (Character.isUpperCase(property.charAt(property.length() - 3)) ? "Y" : "y");
        }
        if (lower.endsWith("sses") || lower.endsWith("ches") || lower.endsWith("shes") || lower.endsWith("xes")) {
            return property.substring(0, property.length() - 2);
        }
        return lower.endsWith("s") ? property.substring(0, property.length() - 1) : property;
    }

    /**
     * Package of a file's {@code <FileStem>Model} class: the base package plus the first Pure package
     * segment its types share ({@code trading::order::Order} under {@code io.concert} gives
     * {@code io.concert.trading}). Falls back to the base package when the types have no package or
     * differ in the first segment.
     */
    public static String modelPackage(String basePackage, java.util.List<String> qualifiedNames) {
        String first = null;
        for (String q : qualifiedNames) {
            int i = q.indexOf("::");
            String segment = i < 0 ? "" : q.substring(0, i);
            if (segment.isEmpty() || (first != null && !first.equals(segment))) {
                return basePackage;
            }
            first = segment;
        }
        return first == null ? basePackage : javaPackage(basePackage, first);
    }

    /** {@code order.pure} becomes {@code Order}; {@code my-model.pure} becomes {@code My_model}. */
    public static String fileStem(String file) {
        String name = file.substring(Math.max(file.lastIndexOf('/'), file.lastIndexOf('\\')) + 1);
        int dot = name.lastIndexOf('.');
        return identifier(capitalize(dot > 0 ? name.substring(0, dot) : name));
    }
}
