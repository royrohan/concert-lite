package io.concert.ecosystem;

import io.concert.model.codegen.JavaNames;
import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ConcertProfile;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.ModelResolver;
import io.concert.model.pure.PureModel;
import io.concert.model.pure.PureModelException;
import io.concert.model.pure.PureParseException;
import io.concert.model.pure.PureParser;
import io.concert.model.pure.ResolvedModel;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A validated ecosystem: the Pure models of a directory with their {@code concert::sm} state machines and / or
 * {@code concert::event} event types and keyed states, plus the names everything is generated under.
 *
 * @param name module name, e.g. {@code insurance} (project {@code :insurance}, dir {@code showcases/insurance})
 * @param javaPackage package of the generated machine classes, e.g. {@code io.concert.eco.insurance}
 * @param files the model files (names in the models directory), sorted
 * @param sources file name to Pure source
 * @param events event-style event types ({@code <<concert::event.event>>}), in declaration order
 * @param states keyed-state types ({@code <<concert::event.state>>}), in declaration order
 * @param warnings validation warnings, {@code file:line:col: warning: ...}
 */
record Ecosystem(
        String name,
        String javaPackage,
        List<String> files,
        Map<String, String> sources,
        ResolvedModel model,
        List<MachineDecl> machines,
        List<EventDecl> events,
        List<EventDecl.StateDecl> states,
        List<String> warnings) {

    /** Module names: lower case, digits and dashes, starting with a letter. */
    static boolean validName(String name) {
        return name.matches("[a-z][a-z0-9-]*") && !name.endsWith("-");
    }

    static String defaultPackage(String name) {
        return "io.concert.eco." + name.replace('-', '_');
    }

    /**
     * Parses and validates every {@code *.pure} file of {@code dir} (not recursive) with the built-in
     * {@code concert::sm} profile.
     *
     * @throws EcosystemException listing every error (parse, resolution and state machine declarations)
     */
    static Ecosystem load(Path dir, String name, String javaPackage) {
        Diagnostics diag = new Diagnostics();
        if (!validName(name)) {
            diag.error(null, "bad name '" + name + "': use lower case letters, digits and dashes, e.g. insurance");
            throw new EcosystemException(diag.errors, diag.warnings);
        }
        if (!javaPackage.matches("[a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)*")) {
            diag.error(null, "bad Java package '" + javaPackage + "'");
            throw new EcosystemException(diag.errors, diag.warnings);
        }
        List<Path> paths;
        try (Stream<Path> s = Files.list(dir)) {
            paths = s.filter(p -> p.toString().endsWith(".pure") && Files.isRegularFile(p)).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read models directory " + dir, e);
        }
        Map<String, String> sources = new LinkedHashMap<>();
        List<PureModel> models = new ArrayList<>();
        for (Path p : paths) {
            String file = p.getFileName().toString();
            if (file.equals(ConcertProfile.FILE_NAME)) {
                diag.warn(new io.concert.model.pure.SourceLocation(file, 1, 1),
                        "skipped: the concert::sm profile is built in (concert/concert.pure is added to the module)");
                continue;
            }
            try {
                String src = Files.readString(p, StandardCharsets.UTF_8);
                sources.put(file, src);
                models.add(PureParser.parse(src, file));
            } catch (PureParseException e) {
                diag.errors.add(e.file() + ":" + e.line() + ":" + e.column() + ": " + e.detail());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (sources.isEmpty() && diag.errors.isEmpty()) {
            diag.error(null, "no .pure files in " + dir.toAbsolutePath());
        }
        if (!diag.errors.isEmpty()) {
            throw new EcosystemException(diag.errors, diag.warnings);
        }
        models.add(ConcertProfile.model());
        ResolvedModel resolved;
        try {
            resolved = new ModelResolver().resolve(models.toArray(PureModel[]::new));
        } catch (PureModelException e) {
            throw new EcosystemException(e.errors(), diag.warnings);
        }
        resolved.warnings().forEach(w -> diag.warnings.add(w.replaceFirst(": ", ": warning: ")));
        TagText text = new TagText(sources);
        List<MachineDecl> machines = new DeclarationParser(resolved, text, diag).parse();
        Set<String> smTypes = new LinkedHashSet<>();
        machines.forEach(m -> smTypes.add(m.smType()));
        EventDeclarationParser.Result ev = new EventDeclarationParser(resolved, text, diag).parse(smTypes);
        if (machines.isEmpty() && ev.events().isEmpty() && diag.errors.isEmpty()) {
            diag.error(null, "no class is marked <<" + ConcertProfile.NAME + "." + ConcertProfile.ROOT + ">> or <<"
                    + ConcertProfile.EVENT_PROFILE + "." + ConcertProfile.EVENT + ">>: declare at least one state machine or event");
        }
        if (ev.events().isEmpty() && !ev.states().isEmpty() && diag.errors.isEmpty()) {
            diag.warn(ev.states().getFirst().cls().location(), "keyed states are declared but no <<concert::event.event>>: no "
                    + "handler can use them");
        }
        if (!diag.errors.isEmpty()) {
            throw new EcosystemException(diag.errors, diag.warnings);
        }
        return new Ecosystem(name, javaPackage, List.copyOf(sources.keySet()), sources, resolved, machines, ev.events(),
                ev.states(), diag.warnings);
    }

    // ---- names ----

    /** Event domains in declaration order. */
    List<String> domains() {
        return events.stream().map(EventDecl::domain).distinct().toList();
    }

    List<EventDecl> eventsOf(String domain) {
        return events.stream().filter(e -> e.domain().equals(domain)).toList();
    }

    boolean hasMachines() {
        return !machines.isEmpty();
    }

    boolean hasEvents() {
        return !events.isEmpty();
    }

    /** The event type named {@code name}, if any. */
    java.util.Optional<EventDecl> event(String name) {
        return events.stream().filter(e -> e.name().equals(name)).findFirst();
    }

    /** Qualified Pure name of the class the generated {@code @LegendModel} names as its root. */
    String legendRoot() {
        return hasMachines() ? machines.getFirst().root().qualifiedName() : events.getFirst().cls().qualifiedName();
    }

    /** {@code insurance} -> {@code Insurance}, {@code trading-gen} -> {@code TradingGen}. */
    String pascalName() {
        return pascal(name);
    }

    /** {@code claim} -> {@code Claim}, {@code gen_trading_order} -> {@code GenTradingOrder}. */
    static String pascal(String s) {
        StringBuilder sb = new StringBuilder();
        for (String part : s.split("[-_]")) {
            if (!part.isEmpty()) {
                sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
        }
        return sb.toString();
    }

    /** {@code claim} -> {@code claim}, {@code trading_order} -> {@code tradingOrder}. */
    static String camel(String s) {
        String p = pascal(s);
        return Character.toLowerCase(p.charAt(0)) + p.substring(1);
    }

    /** Base package of the generated model classes. */
    String modelPackage() {
        return javaPackage + ".model";
    }

    /** Generated Java class (qualified) of a Pure class or enum. */
    String javaType(String pureQualifiedName) {
        int i = pureQualifiedName.lastIndexOf("::");
        String pkg = i < 0 ? "" : pureQualifiedName.substring(0, i);
        String simple = i < 0 ? pureQualifiedName : pureQualifiedName.substring(i + 2);
        return JavaNames.javaPackage(modelPackage(), pkg) + "." + JavaNames.identifier(simple);
    }

    static String simpleName(String qualifiedJava) {
        return qualifiedJava.substring(qualifiedJava.lastIndexOf('.') + 1);
    }

    /** The {@code @LegendModel} files, relative to {@code src/main/pure}: the profile first, then the models. */
    List<String> legendFiles() {
        List<String> out = new ArrayList<>();
        out.add("concert/" + ConcertProfile.FILE_NAME);
        out.addAll(files);
        return out;
    }

    /** Qualified {@code <Stem>Model} classes (Mermaid diagrams) of the model files that define types. */
    List<String> modelRegistryClasses() {
        List<String> out = new ArrayList<>();
        for (String file : files) {
            List<String> elements = new ArrayList<>();
            model.classes().stream().filter(c -> c.location().file().equals(file)).map(ClassDef::qualifiedName).forEach(elements::add);
            model.enums().stream().filter(e -> e.location().file().equals(file)).map(EnumDef::qualifiedName).forEach(elements::add);
            if (!elements.isEmpty()) {
                out.add(JavaNames.modelPackage(modelPackage(), elements) + "." + JavaNames.fileStem(file) + "Model");
            }
        }
        return out;
    }

    /** Sample instance key of a machine, e.g. {@code CLAIM-1001}. */
    static String sampleKey(MachineDecl m) {
        return m.smType().toUpperCase(Locale.ROOT).replace('_', '-') + "-1001";
    }

    /** Root (analytics) table of an smType: plural(smType), as the sinks name it (SchemaMapper). */
    static String rootTable(String smType) {
        String w = smType;
        if (w.endsWith("y") && w.length() > 1 && "aeiou".indexOf(w.charAt(w.length() - 2)) < 0) {
            return w.substring(0, w.length() - 1) + "ies";
        }
        if (w.endsWith("s") || w.endsWith("x") || w.endsWith("z") || w.endsWith("ch") || w.endsWith("sh")) {
            return w + "es";
        }
        return w + "s";
    }
}
