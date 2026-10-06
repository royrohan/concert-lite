package io.concert.sdk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Declarative transition table: (state, eventType) -> next state, optionally with the Java type of
 * the event payload for that transition. Immutable and deterministic, so it is safe to use from
 * workflow code. A state without outgoing transitions is terminal, unless the terminal states are
 * declared explicitly ({@link Builder#terminal}). Also renders itself as a Mermaid diagram for the
 * trace UI.
 */
public final class StateMachineSpec {

    /** @param payloadType the payload's Java type, or {@code null} if the payload is untyped */
    public record Edge(String from, String eventType, String to, Class<?> payloadType) {

        public Edge(String from, String eventType, String to) {
            this(from, eventType, to, null);
        }
    }

    private final String initialState;
    private final Map<String, Map<String, Edge>> table;
    private final List<Edge> edges;
    private final Set<String> states;
    private final Set<String> terminalStates;

    private StateMachineSpec(String initialState, List<Edge> edges, List<String> declaredTerminal) {
        this.initialState = initialState;
        this.edges = List.copyOf(edges);
        Map<String, Map<String, Edge>> t = new LinkedHashMap<>();
        Set<String> s = new LinkedHashSet<>();
        s.add(initialState);
        for (Edge e : edges) {
            t.computeIfAbsent(e.from(), k -> new LinkedHashMap<>()).put(e.eventType(), e);
            s.add(e.from());
            s.add(e.to());
        }
        t.replaceAll((k, v) -> Map.copyOf(v));
        this.table = Map.copyOf(t);
        this.states = Collections.unmodifiableSet(s);
        Set<String> terminal;
        if (declaredTerminal == null) {
            terminal = new LinkedHashSet<>(s);
            terminal.removeAll(t.keySet());
        } else {
            terminal = new LinkedHashSet<>(declaredTerminal);
            for (String state : terminal) {
                if (!s.contains(state)) {
                    throw new IllegalArgumentException("terminal state " + state + " is not a state of this machine " + s);
                }
            }
        }
        this.terminalStates = Collections.unmodifiableSet(terminal);
    }

    public static Builder startingAt(String initialState) {
        return new Builder(initialState);
    }

    public String initialState() {
        return initialState;
    }

    public Optional<String> next(String state, String eventType) {
        return edge(state, eventType).map(Edge::to);
    }

    /** The transition taken from {@code state} on {@code eventType}, if any. */
    public Optional<Edge> edge(String state, String eventType) {
        return Optional.ofNullable(table.getOrDefault(state, Map.of()).get(eventType));
    }

    /** The declared payload type of that transition; empty if there is no such transition or it is untyped. */
    public Optional<Class<?>> payloadType(String state, String eventType) {
        return edge(state, eventType).map(Edge::payloadType);
    }

    /**
     * True if {@code state} is terminal: by default a state without outgoing transitions (an entity in
     * it never changes again), or one of the states given to {@link Builder#terminal}.
     */
    public boolean isTerminal(String state) {
        return terminalStates.contains(state);
    }

    /** The terminal states, in declaration order (see {@link #isTerminal}). */
    public Set<String> terminalStates() {
        return terminalStates;
    }

    public List<Edge> edges() {
        return edges;
    }

    public Set<String> states() {
        return states;
    }

    public String toMermaid(String highlightState) {
        StringBuilder sb = new StringBuilder("stateDiagram-v2\n");
        sb.append("    [*] --> ").append(initialState).append('\n');
        for (Edge e : edges) {
            sb.append("    ").append(e.from()).append(" --> ").append(e.to())
                    .append(" : ").append(e.eventType()).append('\n');
        }
        if (highlightState != null && states.contains(highlightState)) {
            sb.append("    classDef current fill:#f59e0b,color:#111,stroke-width:2px\n");
            sb.append("    class ").append(highlightState).append(" current\n");
        }
        return sb.toString();
    }

    public static final class Builder {
        private final String initialState;
        private final List<Edge> edges = new ArrayList<>();
        private List<String> terminal;

        private Builder(String initialState) {
            this.initialState = initialState;
        }

        public Builder on(String from, String eventType, String to) {
            return on(from, eventType, to, null);
        }

        /** A transition whose event payload is JSON of {@code payloadType} (see {@code ModelStateMachine}). */
        public Builder on(String from, String eventType, String to, Class<?> payloadType) {
            edges.add(new Edge(from, eventType, to, payloadType));
            return this;
        }

        /**
         * Declares the terminal states instead of deriving them (states without outgoing transitions).
         * Every terminal transition publishes a completed-entity snapshot, so a declared terminal state
         * with outgoing transitions publishes again, with a newer version, each time it is re-entered.
         */
        public Builder terminal(String... states) {
            this.terminal = List.of(states);
            return this;
        }

        public StateMachineSpec build() {
            return new StateMachineSpec(initialState, edges, terminal);
        }
    }
}
