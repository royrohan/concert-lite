package io.concert.sdk;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Declarative transition table: (state, eventType) -> next state. Immutable and deterministic, so
 * it is safe to use from workflow code. Also renders itself as a Mermaid diagram for the trace UI.
 */
public final class StateMachineSpec {

    public record Edge(String from, String eventType, String to) {}

    private final String initialState;
    private final Map<String, Map<String, String>> table;
    private final List<Edge> edges;
    private final Set<String> states;

    private StateMachineSpec(String initialState, List<Edge> edges) {
        this.initialState = initialState;
        this.edges = List.copyOf(edges);
        Map<String, Map<String, String>> t = new LinkedHashMap<>();
        Set<String> s = new LinkedHashSet<>();
        s.add(initialState);
        for (Edge e : edges) {
            t.computeIfAbsent(e.from(), k -> new LinkedHashMap<>()).put(e.eventType(), e.to());
            s.add(e.from());
            s.add(e.to());
        }
        t.replaceAll((k, v) -> Map.copyOf(v));
        this.table = Map.copyOf(t);
        this.states = Set.copyOf(s);
    }

    public static Builder startingAt(String initialState) {
        return new Builder(initialState);
    }

    public String initialState() {
        return initialState;
    }

    public Optional<String> next(String state, String eventType) {
        return Optional.ofNullable(table.getOrDefault(state, Map.of()).get(eventType));
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

        private Builder(String initialState) {
            this.initialState = initialState;
        }

        public Builder on(String from, String eventType, String to) {
            edges.add(new Edge(from, eventType, to));
            return this;
        }

        public StateMachineSpec build() {
            return new StateMachineSpec(initialState, edges);
        }
    }
}
