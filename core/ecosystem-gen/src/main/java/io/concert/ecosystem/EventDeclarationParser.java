package io.concert.ecosystem;

import io.concert.model.pure.ClassDef;
import io.concert.model.pure.ConcertProfile;
import io.concert.model.pure.PropertyDef;
import io.concert.model.pure.ResolvedModel;
import io.concert.model.pure.SourceLocation;
import io.concert.model.pure.TaggedValue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the event-style declarations of the {@code concert::event} profile from a resolved model and validates
 * them. On a {@code <<concert::event.event>>} class (the event's payload):
 *
 * <pre>
 * domain   'orders'                              required; [a-z][a-z0-9_]* (task queue ev-orders)
 * locks    'order:{orderId}, customer:{customerId}'   lock key templates separated by ',' ';' or new lines;
 *                                                {field} = to-one primitive / enum payload field, {id} = event id;
 *                                                none: the platform's default &lt;domain&gt;:&lt;eventId&gt;
 * onError  'NON_BLOCKING'                        BLOCKING (default) | NON_BLOCKING once retries are exhausted
 * retries  '2'                                   handler retries after the first attempt, 0..20 (default 2)
 * emits    'OrderAcceptedEvent, OrderRejectedEvent'   event types (or classes) the handler may emit
 * name     'OrderCreated'                        the eventType on the wire (default: the class's simple name)
 * </pre>
 *
 * On a {@code <<concert::event.state>>} class: {@code key 'order:{orderId}'} (fields of the state class) and
 * optionally {@code name}. Errors carry the {@code file:line:col} inside the tagged value where possible.
 */
final class EventDeclarationParser {

    private static final Pattern DOMAIN = Pattern.compile("[a-z][a-z0-9_]*");
    private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([^}]*)}");
    private static final Set<String> EVENT_TAGS = Set.of(ConcertProfile.DOMAIN, ConcertProfile.EVENT_LOCKS,
            ConcertProfile.ON_ERROR, ConcertProfile.RETRIES, ConcertProfile.EVENT_NAME, ConcertProfile.EMITS);
    private static final Set<String> STATE_TAGS = Set.of(ConcertProfile.KEY, ConcertProfile.EVENT_NAME);
    private static final Set<String> REPEATABLE = Set.of(ConcertProfile.EVENT_LOCKS, ConcertProfile.EMITS);

    /** Events and states, in declaration order. */
    record Result(List<EventDecl> events, List<EventDecl.StateDecl> states) {}

    /** One comma / semicolon / line separated item of a tag value, with its offset in the value. */
    private record Item(String text, int offset, TaggedValue tag) {}

    private final ResolvedModel model;
    private final TagText text;
    private final Diagnostics diag;

    EventDeclarationParser(ResolvedModel model, TagText text, Diagnostics diag) {
        this.model = model;
        this.text = text;
        this.diag = diag;
    }

    /**
     * @param smTypes the ecosystem's state machine types (event names must differ from them: the event sender tells
     *     the two styles apart by its first argument)
     */
    Result parse(Set<String> smTypes) {
        List<EventDecl> events = new ArrayList<>();
        List<EventDecl.StateDecl> states = new ArrayList<>();
        Map<EventDecl, Map<String, Item>> emitsByEvent = new LinkedHashMap<>();
        Map<String, SourceLocation> names = new LinkedHashMap<>();
        Map<String, String> smTypeOwners = new LinkedHashMap<>();
        Set<String> invalid = new LinkedHashSet<>(); // events with errors: emits may name them without another error
        for (ClassDef c : model.classes()) {
            boolean event = hasStereotype(c, ConcertProfile.EVENT);
            boolean state = hasStereotype(c, ConcertProfile.STATE);
            List<TaggedValue> tags = c.tags().stream().filter(t -> t.profile().equals(ConcertProfile.EVENT_PROFILE)).toList();
            if (!event && !state) {
                if (!tags.isEmpty()) {
                    diag.error(loc(tags.getFirst(), c), "class " + c.qualifiedName() + " has concert::event tags but is not marked <<"
                            + ConcertProfile.EVENT_PROFILE + "." + ConcertProfile.EVENT + ">> or <<" + ConcertProfile.EVENT_PROFILE + "."
                            + ConcertProfile.STATE + ">>");
                }
                continue;
            }
            if (event && state) {
                diag.error(c.location(), "class " + c.qualifiedName() + " is marked both <<concert::event.event>> and "
                        + "<<concert::event.state>>: an event and a state document need separate classes");
                continue;
            }
            if (c.stereotypes().stream().anyMatch(s -> s.profile().equals(ConcertProfile.NAME) && s.value().equals(ConcertProfile.ROOT))) {
                diag.error(c.location(), "class " + c.qualifiedName() + " is marked <<concert::event." + (event ? "event" : "state")
                        + ">> and <<concert::sm.root>>: a class is either a state machine's root or part of the event style");
                continue;
            }
            Map<String, List<TaggedValue>> byTag = new LinkedHashMap<>();
            tags.forEach(t -> byTag.computeIfAbsent(t.tag(), k -> new ArrayList<>()).add(t));
            Set<String> allowed = event ? EVENT_TAGS : STATE_TAGS;
            boolean tagErrors = false;
            for (Map.Entry<String, List<TaggedValue>> e : byTag.entrySet()) {
                if (!allowed.contains(e.getKey())) {
                    diag.error(loc(e.getValue().getFirst(), c), "concert::event." + e.getKey() + " does not apply to "
                            + (event ? "an event" : "a state") + " class (" + c.qualifiedName() + "); "
                            + (event ? "events take " : "states take ") + new java.util.TreeSet<>(allowed));
                    tagErrors = true;
                } else if (e.getValue().size() > 1 && !REPEATABLE.contains(e.getKey())) {
                    diag.error(loc(e.getValue().get(1), c), "concert::event." + e.getKey() + " is given more than once on "
                            + c.qualifiedName());
                    tagErrors = true;
                }
            }
            String name = name(c, byTag);
            if (name == null) {
                continue;
            }
            if (event) {
                int before = diag.errors.size();
                Map<String, Item> emits = new LinkedHashMap<>();
                EventDecl d = event(c, name, byTag, emits);
                if (d == null || tagErrors || diag.errors.size() > before) {
                    invalid.addAll(List.of(name, c.simpleName(), c.qualifiedName()));
                    continue;
                }
                if (smTypes.contains(name)) {
                    diag.error(nameLoc(c, byTag), "event type " + name + " has the name of a state machine type");
                    continue;
                }
                if (!unique(names, name, c, byTag, "event type") || !uniqueSmType(smTypeOwners, d.smType(), name, c, byTag)) {
                    continue;
                }
                events.add(d);
                emitsByEvent.put(d, emits);
            } else {
                EventDecl.StateDecl s = state(c, name, byTag);
                if (s == null || tagErrors || !unique(names, name, c, byTag, "type")
                        || !uniqueSmType(smTypeOwners, s.smType(), name, c, byTag)) {
                    continue;
                }
                states.add(s);
            }
        }
        List<EventDecl> resolved = resolveEmits(events, emitsByEvent, invalid);
        warnStateKeys(resolved, states);
        return new Result(resolved, states);
    }

    // ------------------------------------------------------------------ events

    private EventDecl event(ClassDef c, String name, Map<String, List<TaggedValue>> byTag, Map<String, Item> emits) {
        TaggedValue domainTag = single(byTag, ConcertProfile.DOMAIN);
        String domain = null;
        if (domainTag == null) {
            diag.error(c.location(), "event class " + c.qualifiedName() + " has no concert::event.domain (the handler "
                    + "application, e.g. 'orders')");
        } else {
            domain = domainTag.value().trim();
            if (!DOMAIN.matcher(domain).matches()) {
                diag.error(text.at(domainTag, Math.max(0, domainTag.value().indexOf(domain))), "unknown domain '" + domain
                        + "': a domain must match [a-z][a-z0-9_]* (it names task queue ev-<domain>)");
            }
        }

        List<String> locks = new ArrayList<>();
        for (Item item : items(byTag.getOrDefault(ConcertProfile.EVENT_LOCKS, List.of()))) {
            if (locks.contains(item.text())) {
                diag.warn(at(item, 0), "lock template '" + item.text() + "' is listed twice");
            } else if (checkTemplate(item, c, "lock template", true)) {
                locks.add(item.text());
            }
        }

        String onError = "BLOCKING";
        TaggedValue onErrorTag = single(byTag, ConcertProfile.ON_ERROR);
        if (onErrorTag != null) {
            String v = onErrorTag.value().trim();
            if (v.equals("BLOCKING") || v.equals("NON_BLOCKING")) {
                onError = v;
            } else {
                diag.error(text.at(onErrorTag, Math.max(0, onErrorTag.value().indexOf(v))), "invalid onError '" + v
                        + "': expected BLOCKING or NON_BLOCKING");
            }
        }

        int retries = EventDecl.DEFAULT_RETRIES;
        TaggedValue retriesTag = single(byTag, ConcertProfile.RETRIES);
        if (retriesTag != null) {
            String v = retriesTag.value().trim();
            SourceLocation at = text.at(retriesTag, Math.max(0, retriesTag.value().indexOf(v)));
            try {
                retries = Integer.parseInt(v);
                if (retries < 0 || retries > 20) {
                    diag.error(at, "invalid retries '" + v + "': expected 0..20 (retries after the first attempt)");
                }
            } catch (NumberFormatException e) {
                diag.error(at, "invalid retries '" + v + "': expected a whole number 0..20 (retries after the first attempt)");
            }
        }

        for (Item item : items(byTag.getOrDefault(ConcertProfile.EMITS, List.of()))) {
            if (!NAME.matcher(item.text()).matches() && !item.text().contains("::")) {
                diag.error(at(item, 0), "malformed event name '" + item.text() + "' in concert::event.emits");
            } else {
                emits.putIfAbsent(item.text(), item);
            }
        }
        if (domain == null) {
            return null;
        }
        return new EventDecl(name, c, domain, List.copyOf(locks), onError, retries + 1, List.of());
    }

    /** Resolves emits entries (event names, class simple or qualified names) to event names. */
    private List<EventDecl> resolveEmits(List<EventDecl> events, Map<EventDecl, Map<String, Item>> emitsByEvent,
            Set<String> invalid) {
        Map<String, EventDecl> byName = new LinkedHashMap<>();
        events.forEach(e -> byName.put(e.name(), e));
        List<EventDecl> out = new ArrayList<>();
        for (EventDecl e : events) {
            List<String> emits = new ArrayList<>();
            for (Item item : emitsByEvent.get(e).values()) {
                String ref = item.text();
                EventDecl target = byName.get(ref);
                if (target == null) {
                    List<EventDecl> byClass = events.stream().filter(x -> x.cls().qualifiedName().equals(ref)
                            || x.cls().simpleName().equals(ref)).toList();
                    target = byClass.size() == 1 ? byClass.getFirst() : null;
                }
                if (target == null && invalid.contains(ref)) {
                    continue; // reported where it is declared
                }
                if (target == null) {
                    diag.error(at(item, 0), "unknown event '" + ref + "' in concert::event.emits of " + e.name() + "; event types are "
                            + byName.keySet());
                } else if (!emits.contains(target.name())) {
                    emits.add(target.name());
                }
            }
            out.add(new EventDecl(e.name(), e.cls(), e.domain(), e.locks(), e.onError(), e.maxAttempts(), List.copyOf(emits)));
        }
        return out;
    }

    // ------------------------------------------------------------------ states

    private EventDecl.StateDecl state(ClassDef c, String name, Map<String, List<TaggedValue>> byTag) {
        TaggedValue keyTag = single(byTag, ConcertProfile.KEY);
        if (keyTag == null) {
            diag.warn(c.location(), "state class " + c.qualifiedName() + " has no concert::event.key: handlers must pass the key "
                    + "to ctx.save(key, state)");
            return new EventDecl.StateDecl(name, c, null);
        }
        List<Item> items = items(List.of(keyTag));
        if (items.size() != 1) {
            diag.error(loc(keyTag, c), "concert::event.key takes one key template, e.g. 'order:{orderId}', not '"
                    + keyTag.value().trim() + "'");
            return null;
        }
        Item item = items.getFirst();
        if (!checkTemplate(item, c, "state key template", false)) {
            return null;
        }
        return new EventDecl.StateDecl(name, c, item.text());
    }

    /** Warns about state keys no event can lock: a handler may only load / save state under one of its event's keys. */
    private void warnStateKeys(List<EventDecl> events, List<EventDecl.StateDecl> states) {
        Set<String> lockShapes = new LinkedHashSet<>();
        events.forEach(e -> e.locks().forEach(l -> lockShapes.add(shape(l))));
        for (EventDecl.StateDecl s : states) {
            if (s.keyTemplate() != null && !lockShapes.contains(shape(s.keyTemplate()))) {
                diag.warn(s.cls().location(), "state key '" + s.keyTemplate() + "' of " + s.cls().qualifiedName() + " matches no "
                        + "event's lock template: handlers can only load and save state under one of their event's lock keys");
            }
        }
    }

    /** {@code order:{orderId}} -> {@code order:{}}. */
    private static String shape(String template) {
        return PLACEHOLDER.matcher(template).replaceAll("{}");
    }

    // ------------------------------------------------------------------ helpers

    private String name(ClassDef c, Map<String, List<TaggedValue>> byTag) {
        TaggedValue t = single(byTag, ConcertProfile.EVENT_NAME);
        if (t == null) {
            return c.simpleName();
        }
        String v = t.value().trim();
        if (!NAME.matcher(v).matches()) {
            diag.error(text.at(t, Math.max(0, t.value().indexOf(v))), "invalid name '" + v + "': expected [A-Za-z_][A-Za-z0-9_]*");
            return null;
        }
        return v;
    }

    private SourceLocation nameLoc(ClassDef c, Map<String, List<TaggedValue>> byTag) {
        TaggedValue t = single(byTag, ConcertProfile.EVENT_NAME);
        return t != null ? loc(t, c) : c.location();
    }

    private boolean unique(Map<String, SourceLocation> names, String name, ClassDef c, Map<String, List<TaggedValue>> byTag,
            String what) {
        SourceLocation first = names.putIfAbsent(name, c.location());
        if (first != null) {
            diag.error(nameLoc(c, byTag), "duplicate " + what + " name '" + name + "' (first declared at " + first + ")");
            return false;
        }
        return true;
    }

    private boolean uniqueSmType(Map<String, String> owners, String smType, String name, ClassDef c,
            Map<String, List<TaggedValue>> byTag) {
        String first = owners.putIfAbsent(smType, name);
        if (first != null) {
            diag.error(nameLoc(c, byTag), "'" + name + "' and '" + first + "' both map to analytics type " + smType
                    + "; rename one (concert::event.name)");
            return false;
        }
        return true;
    }

    /** Checks a template's braces and placeholders against the to-one scalar properties of {@code c}. */
    private boolean checkTemplate(Item item, ClassDef c, String what, boolean idAllowed) {
        String template = item.text();
        int depth = 0;
        for (char ch : template.toCharArray()) {
            depth += ch == '{' ? 1 : ch == '}' ? -1 : 0;
            if (depth < 0 || depth > 1) {
                break;
            }
        }
        if (depth != 0 || template.chars().filter(ch -> ch == '{').count() != template.chars().filter(ch -> ch == '}').count()) {
            diag.error(at(item, 0), "unbalanced braces in " + what + " '" + template + "'");
            return false;
        }
        boolean ok = true;
        Matcher m = PLACEHOLDER.matcher(template);
        while (m.find()) {
            String field = m.group(1).trim();
            SourceLocation at = at(item, m.start(1));
            if (field.equals("id") && idAllowed) {
                continue;
            }
            Optional<PropertyDef> p = model.allProperties(c).stream().filter(x -> x.name().equals(field)).findFirst();
            if (p.isEmpty()) {
                diag.error(at, what + " '" + template + "' references field '" + field + "', which is not a property of "
                        + c.qualifiedName());
                ok = false;
            } else if (!p.get().multiplicity().isToOne() || model.isClass(p.get().type())) {
                diag.error(at, "field '" + field + "' of " + c.qualifiedName() + " in " + what + " '" + template
                        + "' must be a to-one primitive or enum, not " + p.get().type() + p.get().multiplicity());
                ok = false;
            } else if (p.get().multiplicity().isOptional()) {
                diag.warn(at, "field '" + field + "' of " + c.qualifiedName() + " in " + what + " '" + template + "' is optional: "
                        + (idAllowed ? "events without it cannot be sent" : "states without it cannot be saved by key template"));
            }
        }
        return ok;
    }

    /** Splits tag values into trimmed, non-empty items on ',', ';' and new lines. */
    private static List<Item> items(List<TaggedValue> tags) {
        List<Item> out = new ArrayList<>();
        for (TaggedValue t : tags) {
            String v = t.value();
            int start = 0;
            int depth = 0;
            for (int i = 0; i <= v.length(); i++) {
                char ch = i == v.length() ? ',' : v.charAt(i);
                depth += ch == '{' ? 1 : ch == '}' ? -1 : 0;
                if ((ch == ',' || ch == ';' || ch == '\n') && depth <= 0) {
                    String raw = v.substring(start, i);
                    String trimmed = raw.strip();
                    if (!trimmed.isEmpty()) {
                        out.add(new Item(trimmed, start + raw.indexOf(trimmed), t));
                    }
                    start = i + 1;
                }
            }
        }
        return out;
    }

    private SourceLocation at(Item item, int offsetInItem) {
        return text.at(item.tag(), item.offset() + offsetInItem);
    }

    private static TaggedValue single(Map<String, List<TaggedValue>> byTag, String tag) {
        List<TaggedValue> v = byTag.get(tag);
        return v == null ? null : v.getFirst();
    }

    private static boolean hasStereotype(ClassDef c, String value) {
        return c.stereotypes().stream().anyMatch(s -> s.profile().equals(ConcertProfile.EVENT_PROFILE) && s.value().equals(value));
    }

    private static SourceLocation loc(TaggedValue t, ClassDef c) {
        return t.location() != null ? t.location() : c.location();
    }
}
