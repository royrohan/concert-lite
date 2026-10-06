package io.concert.ecosystem;

import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ConcertProfile;
import io.concert.model.pure.EnumDef;
import io.concert.model.pure.EnumValueDef;
import io.concert.model.pure.PrimitiveType;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import io.concert.model.pure.SourceLocation;
import io.concert.model.pure.TaggedValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the state machines declared with the {@code concert::sm} profile from a resolved model and
 * validates them. Syntax of the tagged values on a {@code <<concert::sm.root>>} class:
 *
 * <pre>
 * type           'claim'                          smType, [a-z][a-z0-9_]*, unique
 * initial        'OPEN'                           default: the first transition's source state
 * transitions    'FROM -event-> TO : Payload; …'  FROM may list states: 'A | B -cancel-> C'; ': Payload'
 *                                                 is optional (untyped event); entries separated by ';' or
 *                                                 new lines; the tag may be repeated
 * terminal       'PAID, REJECTED'                 default: states without outgoing transitions
 * locks          'approve: policy:{policyId}; *: region:{region}'   event[, event] (or *) ':' templates,
 *                                                 comma separated; {id} = instance key, {field} = payload field
 * statusProperty 'status'                         enum property kept equal to the state (default: 'status'
 *                                                 if it is an enum property)
 * idProperty     'claimId'                        String property set to the instance key (default:
 *                                                 '&lt;rootName&gt;Id', then 'id')
 * </pre>
 *
 * Errors (and warnings) carry the {@code file:line:col} of the offending text, inside the tagged value
 * where possible.
 */
final class DeclarationParser {

    private static final Pattern SM_TYPE = Pattern.compile("[a-z][a-z0-9_]*");
    private static final Pattern STATE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern TRANSITION = Pattern.compile(
            "(?<from>[A-Za-z_]\\w*(?:\\s*\\|\\s*[A-Za-z_]\\w*)*)\\s*-\\s*(?<event>[A-Za-z_][A-Za-z0-9_.]*)\\s*->\\s*"
                    + "(?<to>[A-Za-z_]\\w*)\\s*(?::\\s*(?<payload>[A-Za-z_][\\w:]*)\\s*)?");
    private static final Set<String> REPEATABLE = Set.of(ConcertProfile.TRANSITIONS, ConcertProfile.LOCKS);

    private final ResolvedModel model;
    private final TagText text;
    private final Diagnostics diag;

    DeclarationParser(ResolvedModel model, TagText text, Diagnostics diag) {
        this.model = model;
        this.text = text;
        this.diag = diag;
    }

    /** One entry of a multi-entry tag value, with its offset in the value. */
    private record Entry(String text, int offset, TaggedValue tag) {}

    List<MachineDecl> parse() {
        List<MachineDecl> machines = new ArrayList<>();
        Map<String, SourceLocation> smTypes = new HashMap<>();
        for (ClassDef c : model.classes()) {
            boolean root = hasStereotype(c, ConcertProfile.ROOT);
            List<TaggedValue> tags = c.tags().stream().filter(t -> t.profile().equals(ConcertProfile.NAME)).toList();
            if (!root) {
                if (!tags.isEmpty()) {
                    diag.error(loc(tags.getFirst(), c), "class " + c.qualifiedName() + " has concert::sm tags but is not marked <<"
                            + ConcertProfile.NAME + "." + ConcertProfile.ROOT + ">>");
                }
                continue;
            }
            MachineDecl m = machine(c, tags);
            if (m == null) {
                continue;
            }
            SourceLocation first = smTypes.putIfAbsent(m.smType(), c.location());
            if (first != null) {
                diag.error(c.location(), "duplicate smType '" + m.smType() + "' (first declared at " + first + ")");
                continue;
            }
            machines.add(m);
        }
        if (machines.isEmpty() && diag.errors.isEmpty()) {
            diag.error(null, "no class is marked <<" + ConcertProfile.NAME + "." + ConcertProfile.ROOT
                    + ">>: declare at least one state machine");
        }
        return machines;
    }

    private static boolean hasStereotype(ClassDef c, String value) {
        return c.stereotypes().stream().anyMatch(s -> s.profile().equals(ConcertProfile.NAME) && s.value().equals(value));
    }

    private static SourceLocation loc(TaggedValue t, ClassDef c) {
        return t.location() != null ? t.location() : c.location();
    }

    private MachineDecl machine(ClassDef c, List<TaggedValue> tags) {
        int errorsBefore = diag.errors.size();
        Map<String, List<TaggedValue>> byTag = new LinkedHashMap<>();
        tags.forEach(t -> byTag.computeIfAbsent(t.tag(), k -> new ArrayList<>()).add(t));
        byTag.forEach((tag, values) -> {
            if (values.size() > 1 && !REPEATABLE.contains(tag)) {
                diag.error(loc(values.get(1), c), "concert::sm." + tag + " is given more than once on " + c.qualifiedName());
            }
        });

        // type
        TaggedValue typeTag = single(byTag, ConcertProfile.TYPE);
        if (typeTag == null) {
            diag.error(c.location(), "root class " + c.qualifiedName() + " has no concert::sm.type (the smType, e.g. 'claim')");
            return null;
        }
        String smType = typeTag.value().trim();
        if (!SM_TYPE.matcher(smType).matches()) {
            diag.error(loc(typeTag, c), "smType '" + smType + "' must match [a-z][a-z0-9_]*");
        }

        // transitions
        List<TaggedValue> transitionTags = byTag.getOrDefault(ConcertProfile.TRANSITIONS, List.of());
        if (transitionTags.isEmpty()) {
            diag.error(c.location(), "root class " + c.qualifiedName() + " has no concert::sm.transitions");
            return null;
        }
        List<MachineDecl.Transition> transitions = new ArrayList<>();
        Map<String, SourceLocation> stateFirstSeen = new LinkedHashMap<>();
        for (Entry e : entries(transitionTags)) {
            Matcher m = TRANSITION.matcher(e.text());
            if (!m.matches()) {
                diag.error(at(e, 0), "malformed transition '" + e.text() + "'; expected FROM -event-> TO [: PayloadClass]");
                continue;
            }
            String to = m.group("to");
            String payloadName = m.group("payload");
            String payload = null;
            SourceLocation entryLoc = at(e, 0);
            if (payloadName != null) {
                payload = resolvePayload(payloadName, c, at(e, m.start("payload")));
            }
            for (String from : m.group("from").split("\\|")) {
                from = from.trim();
                MachineDecl.Transition t = new MachineDecl.Transition(from, m.group("event"), to, payload, entryLoc);
                for (MachineDecl.Transition other : transitions) {
                    if (other.from().equals(from) && other.eventType().equals(t.eventType())) {
                        diag.error(entryLoc, "duplicate transition " + from + " -" + t.eventType() + "-> (first at "
                                + other.location() + ")");
                    }
                }
                transitions.add(t);
                stateFirstSeen.putIfAbsent(from, entryLoc);
            }
            stateFirstSeen.putIfAbsent(to, at(e, m.start("to")));
        }
        if (transitions.isEmpty()) {
            return null;
        }

        // initial
        TaggedValue initialTag = single(byTag, ConcertProfile.INITIAL);
        String initial = initialTag == null ? transitions.getFirst().from() : initialTag.value().trim();
        if (initialTag != null && !stateFirstSeen.containsKey(initial)) {
            diag.error(loc(initialTag, c), "unknown state '" + initial + "': the initial state appears in no transition");
        }
        Set<String> states = new LinkedHashSet<>();
        states.add(initial);
        states.addAll(stateFirstSeen.keySet());

        // reachability
        Set<String> reached = new LinkedHashSet<>(List.of(initial));
        boolean grew = true;
        while (grew) {
            grew = false;
            for (MachineDecl.Transition t : transitions) {
                if (reached.contains(t.from()) && reached.add(t.to())) {
                    grew = true;
                }
            }
        }
        for (String s : states) {
            if (!reached.contains(s)) {
                diag.error(stateFirstSeen.get(s), "state " + s + " of " + smType + " is unreachable from the initial state " + initial);
            }
        }

        // terminal
        TaggedValue terminalTag = single(byTag, ConcertProfile.TERMINAL);
        Set<String> withOutgoing = new LinkedHashSet<>();
        transitions.forEach(t -> withOutgoing.add(t.from()));
        Set<String> terminal = new LinkedHashSet<>();
        if (terminalTag != null) {
            int offset = 0;
            for (String part : terminalTag.value().split(",", -1)) {
                String s = part.trim();
                int at = offset + part.indexOf(s);
                offset += part.length() + 1;
                if (s.isEmpty()) {
                    continue;
                }
                if (!states.contains(s)) {
                    diag.error(text.at(terminalTag, at), "unknown state '" + s + "' in concert::sm.terminal; states are " + states);
                } else {
                    terminal.add(s);
                    if (withOutgoing.contains(s)) {
                        diag.warn(text.at(terminalTag, at), "terminal state " + s + " has outgoing transitions: a snapshot is "
                                + "published each time it is entered");
                    }
                }
            }
            for (String s : states) {
                if (!withOutgoing.contains(s) && !terminal.contains(s)) {
                    diag.warn(stateFirstSeen.get(s), "state " + s + " has no outgoing transitions but is not terminal: entities "
                            + "that reach it are never published to the analytics sinks");
                }
            }
        } else {
            states.stream().filter(s -> !withOutgoing.contains(s)).forEach(terminal::add);
        }
        if (terminal.isEmpty()) {
            diag.warn(c.location(), smType + " has no terminal state: its entities are never published to the analytics sinks");
        }

        // payload classes: marked as commands, one class per event
        Map<String, String> payloadByEvent = new LinkedHashMap<>();
        Set<String> warnedCommands = new LinkedHashSet<>();
        for (MachineDecl.Transition t : transitions) {
            if (t.payloadClass() == null) {
                continue;
            }
            String first = payloadByEvent.putIfAbsent(t.eventType(), t.payloadClass());
            if (first != null && !first.equals(t.payloadClass())) {
                diag.warn(t.location(), "event " + t.eventType() + " takes " + t.payloadClass() + " here but " + first
                        + " elsewhere; samples and the event sender use " + first);
            }
            ClassDef p = model.findClass(t.payloadClass()).orElseThrow();
            if (!hasStereotype(p, ConcertProfile.COMMAND) && warnedCommands.add(p.qualifiedName())) {
                diag.warn(t.location(), "payload class " + p.qualifiedName() + " is not marked <<" + ConcertProfile.NAME + "."
                        + ConcertProfile.COMMAND + ">>");
            }
        }

        // locks
        List<String> events = transitions.stream().map(MachineDecl.Transition::eventType).distinct().toList();
        Map<String, List<String>> locks = new LinkedHashMap<>();
        events.forEach(e -> locks.put(e, new ArrayList<>()));
        for (Entry e : entries(byTag.getOrDefault(ConcertProfile.LOCKS, List.of()))) {
            int colon = e.text().indexOf(':');
            if (colon <= 0) {
                diag.error(at(e, 0), "malformed lock rule '" + e.text() + "'; expected event[, event] (or *): template[, template]");
                continue;
            }
            List<String> targets = new ArrayList<>();
            int offset = 0;
            for (String part : e.text().substring(0, colon).split("[,|]", -1)) {
                String ev = part.trim();
                int at = offset + part.indexOf(ev);
                offset += part.length() + 1;
                if (ev.equals("*")) {
                    targets.addAll(events);
                } else if (!events.contains(ev)) {
                    diag.error(at(e, at), "unknown event '" + ev + "' in concert::sm.locks; events of " + smType + " are " + events);
                } else {
                    targets.add(ev);
                }
            }
            offset = colon + 1;
            for (String part : e.text().substring(colon + 1).split(",", -1)) {
                String template = part.trim();
                int at = offset + part.indexOf(template);
                offset += part.length() + 1;
                if (template.isEmpty()) {
                    diag.error(at(e, at), "empty lock template in '" + e.text() + "'");
                    continue;
                }
                if (!checkTemplate(template, targets, transitions, at(e, at))) {
                    continue;
                }
                for (String ev : targets) {
                    if (!locks.get(ev).contains(template)) {
                        locks.get(ev).add(template);
                    }
                }
            }
        }
        warnFirstKeys(smType, locks, c);

        // status property
        TaggedValue statusTag = single(byTag, ConcertProfile.STATUS_PROPERTY);
        String statusProperty = null;
        String statusEnum = null;
        if (statusTag != null || property(c, "status").map(p -> model.isEnum(p.type())).orElse(false)) {
            String name = statusTag == null ? "status" : statusTag.value().trim();
            SourceLocation at = statusTag == null ? c.location() : loc(statusTag, c);
            Optional<PropertyDef> p = property(c, name);
            if (p.isEmpty()) {
                diag.error(at, "status property '" + name + "' is not a property of " + c.qualifiedName());
            } else if (!model.isEnum(p.get().type()) || !p.get().multiplicity().isToOne()) {
                diag.error(at, "status property '" + name + "' of " + c.qualifiedName() + " must be a to-one enum, not "
                        + p.get().type() + p.get().multiplicity());
            } else {
                statusProperty = name;
                statusEnum = p.get().type().name();
                EnumDef en = model.findEnum(statusEnum).orElseThrow();
                Set<String> values = new LinkedHashSet<>(en.values().stream().map(EnumValueDef::name).toList());
                for (String s : states) {
                    if (!values.contains(s)) {
                        diag.error(stateFirstSeen.getOrDefault(s, loc(initialTag != null ? initialTag : typeTag, c)),
                                "status enum mismatch: state " + s + " is not a value of " + statusEnum + " " + values);
                    }
                }
                for (String v : values) {
                    if (!states.contains(v)) {
                        diag.warn(en.location(), "status enum mismatch: " + statusEnum + "." + v + " is not a state of " + smType);
                    }
                }
            }
        }

        // id property
        TaggedValue idTag = single(byTag, ConcertProfile.ID_PROPERTY);
        String idProperty = null;
        if (idTag != null) {
            String name = idTag.value().trim();
            Optional<PropertyDef> p = property(c, name);
            if (p.isEmpty()) {
                diag.error(loc(idTag, c), "id property '" + name + "' is not a property of " + c.qualifiedName());
            } else if (!isString(p.get()) || !p.get().multiplicity().isToOne()) {
                diag.error(loc(idTag, c), "id property '" + name + "' must be String[1] or String[0..1], not " + p.get().type()
                        + p.get().multiplicity());
            } else {
                idProperty = name;
            }
        } else {
            String simple = c.simpleName();
            for (String candidate : List.of(Character.toLowerCase(simple.charAt(0)) + simple.substring(1) + "Id", "id")) {
                Optional<PropertyDef> p = property(c, candidate);
                if (p.isPresent() && isString(p.get()) && p.get().multiplicity().isToOne()) {
                    idProperty = candidate;
                    break;
                }
            }
            if (idProperty == null) {
                diag.warn(c.location(), c.qualifiedName() + " has no id property (set concert::sm.idProperty): the instance key "
                        + "is not stored in the entity data");
            }
        }

        if (diag.errors.size() > errorsBefore) {
            return null;
        }
        Map<String, List<String>> frozen = new LinkedHashMap<>();
        locks.forEach((k, v) -> frozen.put(k, List.copyOf(v)));
        return new MachineDecl(smType, c, initial, List.copyOf(transitions), Collections.unmodifiableSet(terminal),
                terminalTag != null, Collections.unmodifiableMap(frozen), statusProperty, statusEnum, idProperty);
    }

    private TaggedValue single(Map<String, List<TaggedValue>> byTag, String tag) {
        List<TaggedValue> v = byTag.get(tag);
        return v == null ? null : v.getFirst();
    }

    private SourceLocation at(Entry e, int offsetInEntry) {
        return text.at(e.tag(), e.offset() + offsetInEntry);
    }

    /** Splits tag values into trimmed, non-empty entries on ';' and new lines. */
    private static List<Entry> entries(List<TaggedValue> tags) {
        List<Entry> out = new ArrayList<>();
        for (TaggedValue t : tags) {
            String v = t.value();
            int start = 0;
            for (int i = 0; i <= v.length(); i++) {
                if (i == v.length() || v.charAt(i) == ';' || v.charAt(i) == '\n') {
                    String raw = v.substring(start, i);
                    String trimmed = raw.strip();
                    if (!trimmed.isEmpty()) {
                        out.add(new Entry(trimmed, start + raw.indexOf(trimmed), t));
                    }
                    start = i + 1;
                }
            }
        }
        return out;
    }

    /** Resolves a payload class name: qualified, else the root's package, else a unique simple name. */
    private String resolvePayload(String name, ClassDef root, SourceLocation at) {
        if (name.contains("::")) {
            if (model.findClass(name).isPresent()) {
                return name;
            }
            diag.error(at, (model.findEnum(name).isPresent() ? "payload '" + name + "' is an enum, not a class"
                    : "unknown payload class '" + name + "'"));
            return null;
        }
        String samePackage = root.packageName().isEmpty() ? name : root.packageName() + "::" + name;
        if (model.findClass(samePackage).isPresent()) {
            return samePackage;
        }
        List<String> candidates = model.classes().stream().map(ClassDef::qualifiedName)
                .filter(q -> q.equals(name) || q.endsWith("::" + name)).toList();
        if (candidates.size() == 1) {
            return candidates.getFirst();
        }
        diag.error(at, candidates.isEmpty() ? "unknown payload class '" + name + "'"
                : "ambiguous payload class '" + name + "'; candidates: " + String.join(", ", candidates));
        return null;
    }

    /** Checks a lock template's placeholders against the payload classes of its events. */
    private boolean checkTemplate(String template, List<String> events, List<MachineDecl.Transition> transitions, SourceLocation at) {
        int depth = 0;
        for (char ch : template.toCharArray()) {
            depth += ch == '{' ? 1 : ch == '}' ? -1 : 0;
            if (depth < 0 || depth > 1) {
                diag.error(at, "unbalanced braces in lock template '" + template + "'");
                return false;
            }
        }
        if (depth != 0) {
            diag.error(at, "unbalanced braces in lock template '" + template + "'");
            return false;
        }
        boolean ok = true;
        Matcher m = Pattern.compile("\\{([^}]*)}").matcher(template);
        while (m.find()) {
            String field = m.group(1).trim();
            if (field.equals("id")) {
                continue;
            }
            for (String ev : events) {
                for (MachineDecl.Transition t : transitions) {
                    if (!t.eventType().equals(ev)) {
                        continue;
                    }
                    if (t.payloadClass() == null) {
                        diag.error(at, "lock template '" + template + "' needs payload field '" + field + "' but event " + ev
                                + " (" + t.from() + " -> " + t.to() + ") has no payload class");
                        ok = false;
                        continue;
                    }
                    ClassDef p = model.findClass(t.payloadClass()).orElseThrow();
                    Optional<PropertyDef> prop = property(p, field);
                    if (prop.isEmpty()) {
                        diag.error(at, "lock template '" + template + "' references field '" + field + "', which is not a "
                                + "property of " + p.qualifiedName() + " (payload of " + ev + ")");
                        ok = false;
                    } else if (!prop.get().multiplicity().isToOne() || model.isClass(prop.get().type())) {
                        diag.error(at, "lock field '" + field + "' of " + p.qualifiedName() + " must be a to-one primitive or enum");
                        ok = false;
                    } else if (prop.get().multiplicity().isOptional()) {
                        diag.warn(at, "lock field '" + field + "' of " + p.qualifiedName() + " is optional: events without it "
                                + "cannot be sent");
                    }
                }
            }
        }
        return ok;
    }

    /**
     * The platform keeps per-shard order on each event's first sorted lock key, so all events of an
     * entity should share it; warns when the templates make it differ between events.
     */
    private void warnFirstKeys(String smType, Map<String, List<String>> locks, ClassDef c) {
        Map<String, String> firstByEvent = new LinkedHashMap<>();
        locks.forEach((ev, templates) -> {
            List<String> keys = new ArrayList<>(templates);
            keys.add(smType + ":{id}");
            keys.sort((a, b) -> {
                String pa = a.contains("{") ? a.substring(0, a.indexOf('{')) : a;
                String pb = b.contains("{") ? b.substring(0, b.indexOf('{')) : b;
                int cmp = pa.compareTo(pb);
                return cmp != 0 ? cmp : a.compareTo(b);
            });
            firstByEvent.put(ev, keys.getFirst());
        });
        if (new LinkedHashSet<>(firstByEvent.values()).size() > 1) {
            diag.warn(c.location(), "events of " + smType + " do not share their first sorted lock key " + firstByEvent
                    + ": the platform keeps per-shard order only on that key, so these events may be processed out of order");
        }
    }

    private Optional<PropertyDef> property(ClassDef c, String name) {
        return model.allProperties(c).stream().filter(p -> p.name().equals(name)).findFirst();
    }

    private static boolean isString(PropertyDef p) {
        return p.type().primitive() && p.type().primitiveType() == PrimitiveType.STRING;
    }
}
