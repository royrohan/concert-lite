package io.concert.trading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The trading state machines as data: states, transitions with their payload classes (in
 * {@code commands.pure}), terminal states, and the lock keys every event must carry. Phase B builds
 * the {@code StateMachineSpec}s and {@code ModelStateMachine}s from this; the generator already uses it
 * to address events and render lock keys, so the two cannot drift.
 *
 * <h2>Lifecycle</h2>
 *
 * <pre>
 * trading_order (key orderId)                         every event locks account:&lt;accountId&gt;
 *
 *  PENDING_NEW
 *    | submit
 *    v
 *  NEW -------------- reject -----------------------------> [REJECTED]
 *    | ack
 *    v
 *  ACKED ------------ cancel -----------------------------> [CANCELLED]
 *    | route                                                   ^   ^
 *    v                                                         |   |
 *  WORKING (route) -- cancel ----------------------------------'   |
 *    |     \                                                       |
 *    | fill \-- complete_fill --> ALLOCATING (allocate)            |
 *    v                              ^      | close                 |
 *  PARTIALLY_FILLED ----------------'      v                       |
 *    (fill, route)  complete_fill       [FILLED]                   |
 *    | cancel                                                      |
 *    v                                                             |
 *  CANCEL_ALLOCATING (allocate) -- close --------------------------'
 *
 * trading_execution (key execId)                      every event locks trading_order:&lt;orderId&gt;
 *  PENDING_NEW -route-> ROUTED -fill-> PARTIALLY_FILLED (fill)
 *  ROUTED | PARTIALLY_FILLED -complete_fill-> [FILLED]   -cancel-> [CANCELLED]
 *  ROUTED -reject-> [REJECTED]
 *
 * trading_fill (key fillId)              locks trading_order:&lt;orderId&gt;, trading_execution:&lt;execId&gt;
 *  PENDING -book-> [BOOKED]
 *
 * trading_allocation (key allocId)       locks trading_order:&lt;orderId&gt;, account:&lt;allocAccountId&gt;
 *  PENDING -allocate-> ALLOCATED -confirm-> [CONFIRMED]
 * </pre>
 *
 * {@code (event)} = self-loop, {@code [STATE]} = terminal. A venue fill is one {@code FillCommand}
 * sent as three events: {@code book} to a new fill (terminal at once, so it reaches analytics
 * immediately), {@code fill} or {@code complete_fill} to its execution and to its order. Which of
 * {@code fill}/{@code complete_fill} applies is decided by the producer (the venue report says
 * whether the child or parent order is done), since a transition table maps (state, event) to a
 * single next state. An order reaches its terminal state only after its allocations: {@code close}
 * follows the last {@code allocate}. An order cancelled before any fill goes straight to
 * {@code CANCELLED}.
 *
 * <h2>Why these lock keys</h2>
 *
 * The platform keeps per-shard order on each event's <em>first sorted</em> lock key (README, "Known
 * limits"), so all events of one entity must share their first key, and the producer uses it as the
 * Kinesis partition key:
 *
 * <ul>
 *   <li>Order events all lock {@code account:<accountId>} (it sorts before {@code trading_order:}): it
 *       is needed anyway on {@code submit} (credit check), {@code fill} and {@code cancel} (credit
 *       consumed or released), and carrying it on every order event keeps the order's events on one
 *       first key.
 *   <li>Execution events lock their order: a child order changes only while its parent is not.
 *       {@code trading_execution:} sorts first.
 *   <li>A fill locks its order and execution, so the three events of one venue fill are serialized
 *       against everything else on that order.
 *   <li>An allocation locks its order and the receiving account (position and credit of that account).
 *       {@code account:} sorts first and is the same for both allocation events.
 * </ul>
 *
 * Lock keys are rendered from payload fields: every command extends {@code OrderEvent}, which carries
 * {@code orderId} and {@code accountId}.
 */
public final class TradingFlows {
    private TradingFlows() {}

    public static final String ORDER = "trading_order";
    public static final String EXECUTION = "trading_execution";
    public static final String FILL = "trading_fill";
    public static final String ALLOCATION = "trading_allocation";

    /** Lock namespace without a state machine behind it: an account's credit line and positions. */
    public static final String ACCOUNT = "account";

    /** Package of the payload classes in {@code commands.pure}. */
    public static final String COMMAND_PACKAGE = "trading::command::";

    /** Package of the aggregate classes in {@code order.pure}. */
    public static final String ORDER_PACKAGE = "trading::order::";

    /** @param payloadClass simple name of the payload class in {@link #COMMAND_PACKAGE} */
    public record Transition(String from, String eventType, String to, String payloadClass) {

        public String qualifiedPayloadClass() {
            return COMMAND_PACKAGE + payloadClass;
        }
    }

    /** A lock key {@code namespace:<value of payloadField>}. */
    public record LockKey(String namespace, String payloadField) {

        public String render(String value) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing " + payloadField + " for lock " + namespace);
            }
            return namespace + ":" + value;
        }

        @Override
        public String toString() {
            return namespace + ":<" + payloadField + ">";
        }
    }

    /**
     * One state machine type.
     *
     * @param keyField payload field holding the target instance key
     * @param modelClass simple name of the aggregate class in {@link #ORDER_PACKAGE}
     * @param statusEnum simple name of the enum (same package) whose values are this flow's states
     * @param locks extra lock keys per event type (the target entity key is always implied)
     */
    public record Flow(
            String smType,
            String keyField,
            String modelClass,
            String statusEnum,
            String initialState,
            List<Transition> transitions,
            Map<String, List<LockKey>> locks) {

        public Flow {
            transitions = List.copyOf(transitions);
            Map<String, List<LockKey>> l = new LinkedHashMap<>();
            locks.forEach((k, v) -> l.put(k, List.copyOf(v)));
            locks = Collections.unmodifiableMap(l);
        }

        public Set<String> states() {
            Set<String> s = new LinkedHashSet<>();
            s.add(initialState);
            transitions.forEach(t -> {
                s.add(t.from());
                s.add(t.to());
            });
            return Collections.unmodifiableSet(s);
        }

        /** States without outgoing transitions. */
        public Set<String> terminalStates() {
            Set<String> s = new LinkedHashSet<>(states());
            transitions.forEach(t -> s.remove(t.from()));
            return Collections.unmodifiableSet(s);
        }

        public Set<String> eventTypes() {
            Set<String> s = new LinkedHashSet<>();
            transitions.forEach(t -> s.add(t.eventType()));
            return Collections.unmodifiableSet(s);
        }

        public Optional<Transition> transition(String from, String eventType) {
            return transitions.stream().filter(t -> t.from().equals(from) && t.eventType().equals(eventType)).findFirst();
        }

        /** The payload class of {@code eventType}; the same on every transition of that event type. */
        public String payloadClass(String eventType) {
            return transitions.stream().filter(t -> t.eventType().equals(eventType)).map(Transition::payloadClass)
                    .findFirst().orElseThrow(() -> new NoSuchElementException(smType + " has no event " + eventType));
        }

        public List<LockKey> locksFor(String eventType) {
            List<LockKey> l = locks.get(eventType);
            if (l == null) {
                throw new NoSuchElementException(smType + " has no lock rule for " + eventType);
            }
            return l;
        }

        /** Lock keys of an event, rendered from its payload fields. */
        public List<String> renderLocks(String eventType, Function<String, String> payloadField) {
            return locksFor(eventType).stream().map(k -> k.render(payloadField.apply(k.payloadField()))).toList();
        }

        /** Mermaid {@code stateDiagram-v2}, like {@code StateMachineSpec.toMermaid}. */
        public String toMermaid() {
            StringBuilder sb = new StringBuilder("stateDiagram-v2\n");
            sb.append("    [*] --> ").append(initialState).append('\n');
            for (Transition t : transitions) {
                sb.append("    ").append(t.from()).append(" --> ").append(t.to()).append(" : ").append(t.eventType()).append('\n');
            }
            for (String s : terminalStates()) {
                sb.append("    ").append(s).append(" --> [*]\n");
            }
            return sb.toString();
        }
    }

    public static final Flow ORDER_FLOW = new Builder(ORDER, "orderId", "Order", "OrderStatus", "PENDING_NEW")
            .on("PENDING_NEW", "submit", "NEW", "NewOrderCommand")
            .on("NEW", "ack", "ACKED", "AckCommand")
            .on("NEW", "reject", "REJECTED", "RejectCommand")
            .on("ACKED", "route", "WORKING", "RouteCommand")
            .on("ACKED", "cancel", "CANCELLED", "CancelCommand")
            .on("WORKING", "route", "WORKING", "RouteCommand")
            .on("WORKING", "fill", "PARTIALLY_FILLED", "FillCommand")
            .on("WORKING", "complete_fill", "ALLOCATING", "FillCommand")
            .on("WORKING", "cancel", "CANCELLED", "CancelCommand")
            .on("PARTIALLY_FILLED", "route", "PARTIALLY_FILLED", "RouteCommand")
            .on("PARTIALLY_FILLED", "fill", "PARTIALLY_FILLED", "FillCommand")
            .on("PARTIALLY_FILLED", "complete_fill", "ALLOCATING", "FillCommand")
            .on("PARTIALLY_FILLED", "cancel", "CANCEL_ALLOCATING", "CancelCommand")
            .on("ALLOCATING", "allocate", "ALLOCATING", "AllocateCommand")
            .on("ALLOCATING", "close", "FILLED", "CloseOrderCommand")
            .on("CANCEL_ALLOCATING", "allocate", "CANCEL_ALLOCATING", "AllocateCommand")
            .on("CANCEL_ALLOCATING", "close", "CANCELLED", "CloseOrderCommand")
            .lockEvery(new LockKey(ACCOUNT, "accountId"))
            .build();

    public static final Flow EXECUTION_FLOW = new Builder(EXECUTION, "execId", "Execution", "ExecutionStatus", "PENDING_NEW")
            .on("PENDING_NEW", "route", "ROUTED", "RouteCommand")
            .on("ROUTED", "fill", "PARTIALLY_FILLED", "FillCommand")
            .on("ROUTED", "complete_fill", "FILLED", "FillCommand")
            .on("ROUTED", "cancel", "CANCELLED", "CancelCommand")
            .on("ROUTED", "reject", "REJECTED", "RejectCommand")
            .on("PARTIALLY_FILLED", "fill", "PARTIALLY_FILLED", "FillCommand")
            .on("PARTIALLY_FILLED", "complete_fill", "FILLED", "FillCommand")
            .on("PARTIALLY_FILLED", "cancel", "CANCELLED", "CancelCommand")
            .lockEvery(new LockKey(ORDER, "orderId"))
            .build();

    public static final Flow FILL_FLOW = new Builder(FILL, "fillId", "Fill", "FillStatus", "PENDING")
            .on("PENDING", "book", "BOOKED", "FillCommand")
            .lockEvery(new LockKey(ORDER, "orderId"), new LockKey(EXECUTION, "execId"))
            .build();

    public static final Flow ALLOCATION_FLOW = new Builder(ALLOCATION, "allocId", "Allocation", "AllocationStatus", "PENDING")
            .on("PENDING", "allocate", "ALLOCATED", "AllocateCommand")
            .on("ALLOCATED", "confirm", "CONFIRMED", "ConfirmAllocationCommand")
            .lockEvery(new LockKey(ORDER, "orderId"), new LockKey(ACCOUNT, "allocAccountId"))
            .build();

    private static final List<Flow> ALL = List.of(ORDER_FLOW, EXECUTION_FLOW, FILL_FLOW, ALLOCATION_FLOW);

    public static List<Flow> all() {
        return ALL;
    }

    public static Flow flow(String smType) {
        return ALL.stream().filter(f -> f.smType().equals(smType)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("unknown smType " + smType));
    }

    /** Namespaces a lock key may use: every smType plus {@link #ACCOUNT}. */
    public static Set<String> lockNamespaces() {
        Set<String> s = new LinkedHashSet<>();
        ALL.forEach(f -> s.add(f.smType()));
        s.add(ACCOUNT);
        return Collections.unmodifiableSet(s);
    }

    /** All flows as Mermaid diagrams, one fenced block per smType (for READMEs and the trace UI). */
    public static String toMermaid() {
        StringBuilder sb = new StringBuilder();
        for (Flow f : ALL) {
            sb.append("%% ").append(f.smType()).append('\n').append(f.toMermaid()).append('\n');
        }
        return sb.toString();
    }

    /** Markdown table: smType, event, payload, lock keys (beyond the target entity key). */
    public static String lockTable() {
        StringBuilder sb = new StringBuilder("| smType | event | payload | lock keys |\n|---|---|---|---|\n");
        for (Flow f : ALL) {
            for (String e : f.eventTypes()) {
                sb.append("| ").append(f.smType()).append(':').append('<').append(f.keyField()).append('>')
                        .append(" | ").append(e).append(" | ").append(f.payloadClass(e)).append(" | ")
                        .append(String.join(", ", f.locksFor(e).stream().map(LockKey::toString).toList())).append(" |\n");
            }
        }
        return sb.toString();
    }

    private static final class Builder {
        private final String smType;
        private final String keyField;
        private final String modelClass;
        private final String statusEnum;
        private final String initialState;
        private final List<Transition> transitions = new ArrayList<>();
        private List<LockKey> every = List.of();

        Builder(String smType, String keyField, String modelClass, String statusEnum, String initialState) {
            this.smType = smType;
            this.keyField = keyField;
            this.modelClass = modelClass;
            this.statusEnum = statusEnum;
            this.initialState = initialState;
        }

        Builder on(String from, String eventType, String to, String payloadClass) {
            transitions.add(new Transition(from, eventType, to, payloadClass));
            return this;
        }

        /** The same lock keys on every event type: keeps the entity's events on one first sorted key. */
        Builder lockEvery(LockKey... keys) {
            every = List.of(keys);
            return this;
        }

        Flow build() {
            Map<String, List<LockKey>> locks = new LinkedHashMap<>();
            transitions.forEach(t -> locks.put(t.eventType(), every));
            return new Flow(smType, keyField, modelClass, statusEnum, initialState, transitions, locks);
        }
    }
}
