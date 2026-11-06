package io.concert.legend;

import io.concert.legend.LegendStoreGenerator.Target;
import io.concert.model.pure.AssociationDef;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.ProfileDef;
import io.concert.model.pure.PureModel;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The concert models plus the generated store layer as a Legend Studio project: one element per file, as Studio keeps
 * them, and no {@code import} lines (Studio refuses imports in files), so every type reference is written out in full.
 *
 * <p>Each source file is cut at its elements (a line starting with {@code Class}, {@code Enum}, {@code Association},
 * {@code Profile}, {@code function}, ...), the comment lines just above an element going with it. In each element's
 * text a type's simple name becomes its qualified name when the element's own package or one of the file's imported
 * packages declares it, outside strings and comments, and not where it is a property name ({@code name:}) or part of
 * a path ({@code a::B}, {@code $x.B}, {@code E.VALUE}'s value). The store layer's elements are already qualified.
 */
public final class StudioProject {
    private StudioProject() {}

    /** One file of the project: a file name (served as is) and its Pure text, one element. */
    public record File(String name, String element, String code) {}

    private static final Pattern ELEMENT = Pattern.compile("^(Class|Enum|Association|Profile|function|Measure|Primitive)\\b");
    private static final Pattern IMPORT = Pattern.compile("^\\s*import\\s+([\\w:]+)::\\*\\s*;\\s*$");

    /**
     * The project's files: every element of {@code sources} (file name to Pure text, e.g. what the DuckDB sink's
     * {@code /models} serves, plus the concert profile), then the store layer of {@code generated}.
     */
    public static List<File> files(Map<String, String> sources, LegendStoreMain.Generated generated, List<Target> targets) {
        ResolvedModel model = generated.model();
        Map<String, Set<String>> bySimple = new LinkedHashMap<>();
        for (String q : qualifiedNames(model)) {
            bySimple.computeIfAbsent(simple(q), k -> new LinkedHashSet<>()).add(q);
        }
        List<File> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (var src : sources.entrySet()) {
            for (String chunk : split(src.getValue())) {
                String element = elementName(chunk, src.getKey());
                if (element == null || !seen.add(element)) {
                    continue; // nothing declared, or already in the project (e.g. the profile in several modules)
                }
                String code = qualify(chunk, packageOf(element), imports(src.getValue()), bySimple);
                out.add(new File(fileName(out.size(), element), element, code.strip() + "\n"));
            }
        }
        generated.generator().elements(targets).forEach((element, code) -> {
            if (seen.add(element)) {
                out.add(new File(fileName(out.size(), element), element, code));
            }
        });
        return out;
    }

    /** A short hash of the project's content: a changed model is a new Studio project, not a no-op reload. */
    public static String fingerprint(List<File> files) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (File f : files) {
                md.update(f.code().getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(md.digest(), 0, 4);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static String fileName(int index, String element) {
        return String.format("%03d-%s.pure", index, element.replace("::", "_"));
    }

    private static Set<String> qualifiedNames(ResolvedModel m) {
        Set<String> out = new LinkedHashSet<>();
        m.classes().forEach(c -> out.add(c.qualifiedName()));
        m.enums().forEach(e -> out.add(e.qualifiedName()));
        m.associations().forEach(a -> out.add(a.qualifiedName()));
        m.profiles().forEach(p -> out.add(p.name()));
        return out;
    }

    /** The source cut into element chunks (comment lines just above an element included); headers and imports dropped. */
    static List<String> split(String source) {
        List<String> lines = new ArrayList<>(List.of(source.split("\n", -1)));
        lines.removeIf(l -> l.startsWith("###") || IMPORT.matcher(l).matches());
        List<Integer> starts = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            if (ELEMENT.matcher(lines.get(i)).find()) {
                int s = i;
                while (s > 0 && lines.get(s - 1).stripLeading().startsWith("//") && (starts.isEmpty() || s - 1 > starts.getLast())) {
                    s--;
                }
                starts.add(s);
            }
        }
        List<String> out = new ArrayList<>();
        for (int k = 0; k < starts.size(); k++) {
            int end = k + 1 < starts.size() ? starts.get(k + 1) : lines.size();
            out.add(String.join("\n", lines.subList(starts.get(k), end)));
        }
        return out;
    }

    /** The qualified name a chunk declares (parsed alone: names are as written, which for an element is its full path). */
    static String elementName(String chunk, String file) {
        try {
            PureModel m = PureParser.parse(chunk, file);
            for (ClassDef c : m.classes()) {
                return c.qualifiedName();
            }
            for (EnumDef e : m.enums()) {
                return e.qualifiedName();
            }
            for (AssociationDef a : m.associations()) {
                return a.qualifiedName();
            }
            for (ProfileDef p : m.profiles()) {
                return p.name();
            }
        } catch (RuntimeException e) {
            // not a model element concert's parser knows (a function, ...): left out
        }
        return null;
    }

    static List<String> imports(String source) {
        List<String> out = new ArrayList<>();
        for (String l : source.split("\n")) {
            Matcher m = IMPORT.matcher(l);
            if (m.matches()) {
                out.add(m.group(1));
            }
        }
        return out;
    }

    static String packageOf(String qualified) {
        int i = qualified.lastIndexOf("::");
        return i < 0 ? "" : qualified.substring(0, i);
    }

    static String simple(String qualified) {
        int i = qualified.lastIndexOf("::");
        return i < 0 ? qualified : qualified.substring(i + 2);
    }

    /** The chunk with each type's simple name written in full (see the class doc). */
    static String qualify(String chunk, String ownPackage, List<String> imports, Map<String, Set<String>> bySimple) {
        StringBuilder out = new StringBuilder(chunk.length() + 64);
        int i = 0;
        int n = chunk.length();
        while (i < n) {
            char c = chunk.charAt(i);
            if (c == '\'') { // string literal
                int j = i + 1;
                while (j < n && chunk.charAt(j) != '\'') {
                    j += chunk.charAt(j) == '\\' ? 2 : 1;
                }
                j = Math.min(n, j + 1);
                out.append(chunk, i, j);
                i = j;
            } else if (c == '/' && i + 1 < n && chunk.charAt(i + 1) == '/') { // line comment
                int j = chunk.indexOf('\n', i);
                j = j < 0 ? n : j;
                out.append(chunk, i, j);
                i = j;
            } else if (c == '/' && i + 1 < n && chunk.charAt(i + 1) == '*') { // block comment
                int j = chunk.indexOf("*/", i + 2);
                j = j < 0 ? n : j + 2;
                out.append(chunk, i, j);
                i = j;
            } else if (Character.isJavaIdentifierStart(c) && c != '$') {
                int j = i + 1;
                while (j < n && Character.isJavaIdentifierPart(chunk.charAt(j)) && chunk.charAt(j) != '$') {
                    j++;
                }
                String word = chunk.substring(i, j);
                out.append(qualified(word, chunk, i, j, ownPackage, imports, bySimple));
                i = j;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static String qualified(String word, String text, int start, int end, String ownPackage, List<String> imports,
            Map<String, Set<String>> bySimple) {
        Set<String> candidates = bySimple.get(word);
        if (candidates == null) {
            return word;
        }
        char before = start > 0 ? text.charAt(start - 1) : ' ';
        if (before == ':' || before == '.' || before == '$' || Character.isJavaIdentifierPart(before)) {
            return word; // part of a path, a property access or an enum value
        }
        int k = end;
        while (k < text.length() && (text.charAt(k) == ' ' || text.charAt(k) == '\t')) {
            k++;
        }
        boolean colon = k < text.length() && text.charAt(k) == ':';
        boolean path = colon && k + 1 < text.length() && text.charAt(k + 1) == ':';
        if (path || (colon && end < text.length())) {
            return word; // already a path (A::B), or a property / tag name (name: ...)
        }
        String own = ownPackage.isEmpty() ? word : ownPackage + "::" + word;
        if (candidates.contains(own)) {
            return own;
        }
        for (String pkg : imports) {
            if (candidates.contains(pkg + "::" + word)) {
                return pkg + "::" + word;
            }
        }
        return word;
    }
}
