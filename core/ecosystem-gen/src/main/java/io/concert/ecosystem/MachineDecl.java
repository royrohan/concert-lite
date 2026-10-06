package io.concert.ecosystem;

import io.concert.model.pure.ClassDef;
import io.concert.model.pure.SourceLocation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One state machine as declared on a {@code <<concert::sm.root>>} class.
 *
 * @param root the root (data) class
 * @param terminal terminal states: declared with {@code concert::sm.terminal}, else the states
 *     without outgoing transitions
 * @param terminalDeclared whether {@code terminal} came from the tag (the spec then lists it)
 * @param locks lock key templates per event type, in event declaration order ({@code *} rules
 *     merged in); every event has an entry
 * @param statusProperty enum property kept equal to the state, or {@code null}
 * @param statusEnum qualified name of that property's enum, or {@code null}
 * @param idProperty String property set to the instance key, or {@code null}
 */
record MachineDecl(
        String smType,
        ClassDef root,
        String initial,
        List<Transition> transitions,
        Set<String> terminal,
        boolean terminalDeclared,
        Map<String, List<String>> locks,
        String statusProperty,
        String statusEnum,
        String idProperty) {

    /**
     * {@code from -eventType-> to : payloadClass}.
     *
     * @param payloadClass qualified Pure name of the payload class, or {@code null} if untyped
     */
    record Transition(String from, String eventType, String to, String payloadClass, SourceLocation location) {}

    /** All states: the initial state, then sources and targets in declaration order. */
    Set<String> states() {
        Set<String> s = new LinkedHashSet<>();
        s.add(initial);
        transitions.forEach(t -> {
            s.add(t.from());
            s.add(t.to());
        });
        return s;
    }

    /** Event types in declaration order. */
    List<String> eventTypes() {
        Set<String> s = new LinkedHashSet<>();
        transitions.forEach(t -> s.add(t.eventType()));
        return List.copyOf(s);
    }

    /** The payload class of an event type (its first transition's), or {@code null}. */
    String payloadClass(String eventType) {
        return transitions.stream().filter(t -> t.eventType().equals(eventType)).map(Transition::payloadClass)
                .filter(java.util.Objects::nonNull).findFirst().orElse(null);
    }

    /** Distinct payload classes in declaration order. */
    List<String> payloadClasses() {
        Set<String> s = new LinkedHashSet<>();
        transitions.forEach(t -> {
            if (t.payloadClass() != null) {
                s.add(t.payloadClass());
            }
        });
        return List.copyOf(s);
    }

    /**
     * The happy path: the shortest event sequence from the initial state to the terminal state that is
     * farthest away (ties: a state whose name does not suggest failure, e.g. REJECTED or CANCELLED, then
     * declaration order). Self-loops are not taken. Empty if no terminal state is reachable.
     */
    List<Transition> happyPath() {
        Map<String, Transition> via = new LinkedHashMap<>();
        Map<String, Integer> dist = new LinkedHashMap<>();
        dist.put(initial, 0);
        List<String> queue = new ArrayList<>(List.of(initial));
        for (int i = 0; i < queue.size(); i++) {
            String s = queue.get(i);
            for (Transition t : transitions) {
                if (t.from().equals(s) && !t.to().equals(s) && !dist.containsKey(t.to())) {
                    dist.put(t.to(), dist.get(s) + 1);
                    via.put(t.to(), t);
                    queue.add(t.to());
                }
            }
        }
        String best = null;
        for (String s : terminal) {
            if (!dist.containsKey(s) || s.equals(initial)) {
                continue;
            }
            if (best == null || dist.get(s) > dist.get(best)
                    || (dist.get(s).equals(dist.get(best)) && unhappy(best) && !unhappy(s))) {
                best = s;
            }
        }
        List<Transition> path = new ArrayList<>();
        for (String s = best; s != null && via.containsKey(s); s = via.get(s).from()) {
            path.addFirst(via.get(s));
        }
        return path;
    }

    private static boolean unhappy(String state) {
        return state.matches("(?i).*(REJECT|CANCEL|FAIL|ERROR|DECLIN|ABORT|EXPIR|VOID|DENIED|LAPSE).*");
    }
}
