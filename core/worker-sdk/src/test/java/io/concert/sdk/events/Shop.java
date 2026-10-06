package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import io.concert.sdk.ModelStateMachineTest.TicketMachine;
import io.concert.sdk.model.demo.ticket.Ticket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A small hand-written event-style test domain: domains {@code shop} and {@code stock}, POJO events and keyed
 * state, handlers. (The real showcase, generated from Pure, comes later.)
 */
final class Shop {
    private Shop() {}

    static final String SHOP = "shop";
    static final String STOCK = "stock";

    // ---- events

    public static class OrderPlaced {
        public String orderId;
        public String sku;
        public int qty;

        public OrderPlaced() {}

        OrderPlaced(String orderId, String sku, int qty) {
            this.orderId = orderId;
            this.sku = sku;
            this.qty = qty;
        }
    }

    public static class ReserveStock {
        public String orderId;
        public String sku;
        public int qty;

        public ReserveStock() {}

        ReserveStock(String orderId, String sku, int qty) {
            this.orderId = orderId;
            this.sku = sku;
            this.qty = qty;
        }
    }

    public static class Restock {
        public String sku;
        public int qty;

        public Restock() {}

        Restock(String sku, int qty) {
            this.sku = sku;
            this.qty = qty;
        }
    }

    public static class OrderConfirmed {
        public String orderId;

        public OrderConfirmed() {}

        OrderConfirmed(String orderId) {
            this.orderId = orderId;
        }
    }

    /** Emits the next step until {@code n == 3}: a -> b -> c. */
    public static class Step {
        public String chain;
        public int n;

        public Step() {}

        Step(String chain, int n) {
            this.chain = chain;
            this.n = n;
        }
    }

    /** Adds {@code by} to the counter of the event's first lock key, recording seq order. */
    public static class Count {
        public String key;
        public int by;
        public long seq;

        public Count() {}

        Count(String key, int by, long seq) {
            this.key = key;
            this.by = by;
            this.seq = seq;
        }
    }

    /** Counts like {@link Count}; throws BlockingError / NonBlockingError while its tag is poisoned. */
    public static class Poison {
        public String tag;

        public Poison() {}

        Poison(String tag) {
            this.tag = tag;
        }
    }

    /** NON_BLOCKING twin of {@link Poison}. */
    public static class PoisonNb extends Poison {
        public PoisonNb() {}

        PoisonNb(String tag) {
            super(tag);
        }
    }

    /** Fails its first {@code FAILS[tag]} attempts with a plain exception, then counts and emits one Count child. */
    public static class Flaky {
        public String tag;

        public Flaky() {}

        Flaky(String tag) {
            this.tag = tag;
        }
    }

    /** Like {@link Flaky} but NON_BLOCKING on exhaustion. */
    public static class FlakyNb extends Flaky {
        public FlakyNb() {}

        FlakyNb(String tag) {
            super(tag);
        }
    }

    /** Emits a Count child at {@code inMillis} from the handler's clock. */
    public static class Remind {
        public String key;
        public long atMillis;

        public Remind() {}

        Remind(String key, long atMillis) {
            this.key = key;
            this.atMillis = atMillis;
        }
    }

    /** Drives the ticket state machine (interop: handler -> entity). */
    public static class OpenTicket {
        public String ticketId;
        public String assignee;

        public OpenTicket() {}

        OpenTicket(String ticketId, String assignee) {
            this.ticketId = ticketId;
            this.assignee = assignee;
        }
    }

    // ---- keyed state

    public static class OrderState {
        public String orderId;
        public String status;
        public int events;
    }

    public static class StockState {
        public String sku;
        public int available;
    }

    public static class Counter {
        public String key;
        public long value;
        public long lastSeq = -1;
        public long outOfOrder;
        public List<String> seen = new ArrayList<>();
    }

    // ---- catalog

    static final EventCatalog CATALOG = EventCatalog.builder("shop-test")
            .event(SHOP, OrderPlaced.class, "order:{orderId}")
            .event(EventCatalog.EventType.of(STOCK, ReserveStock.class, "sku:{sku}").nonBlocking())
            .event(STOCK, Restock.class, "sku:{sku}")
            .event(SHOP, OrderConfirmed.class, "order:{orderId}")
            .event(SHOP, Step.class, "chain:{chain}")
            .event(SHOP, Count.class, "counter:{key}")
            .event(SHOP, Poison.class)
            .event(EventCatalog.EventType.of(SHOP, PoisonNb.class).nonBlocking())
            .event(SHOP, Flaky.class)
            .event(EventCatalog.EventType.of(SHOP, FlakyNb.class).nonBlocking())
            .event(SHOP, Remind.class, "counter:{key}")
            .event(SHOP, OpenTicket.class, "ticket:{ticketId}")
            .state(OrderState.class, "order:{orderId}")
            .state(StockState.class, "sku:{sku}")
            .state(Counter.class, null)
            .build();

    static void registerCatalog() {
        EventCatalogs.register(CATALOG);
    }

    // ---- test controls

    static final Set<String> POISONED = ConcurrentHashMap.newKeySet();
    static final Map<String, Integer> FAILS = new ConcurrentHashMap<>();
    static final Map<String, AtomicInteger> ATTEMPTS = new ConcurrentHashMap<>();
    /** Handler runs per event id (re-runs included). */
    static final Map<String, AtomicInteger> RUNS = new ConcurrentHashMap<>();

    static void reset() {
        POISONED.clear();
        FAILS.clear();
        ATTEMPTS.clear();
        RUNS.clear();
    }

    private static void ran(EventContext ctx) {
        RUNS.computeIfAbsent(ctx.envelope().eventId(), k -> new AtomicInteger()).incrementAndGet();
    }

    /** Adds {@code by} to the Counter of the event's first lock key. */
    static void count(EventContext ctx, int by, long seq) {
        String key = ctx.envelope().effectiveLockKeys().getFirst();
        Counter c = ctx.state(Counter.class, key).orElseGet(() -> {
            Counter n = new Counter();
            n.key = key;
            return n;
        });
        c.value += by;
        if (seq >= 0) {
            if (seq < c.lastSeq) {
                c.outOfOrder++;
            }
            c.lastSeq = Math.max(c.lastSeq, seq);
        }
        c.seen.add(ctx.envelope().eventId());
        ctx.save(key, c);
    }

    // ---- handlers

    @Handles(OrderPlaced.class)
    static final class OrderPlacedHandler implements EventHandler<OrderPlaced> {
        @Override
        public void apply(OrderPlaced e, EventContext ctx) {
            ran(ctx);
            OrderState s = ctx.state(OrderState.class, "order:" + e.orderId).orElseGet(OrderState::new);
            s.orderId = e.orderId;
            s.status = "PLACED";
            s.events++;
            ctx.save(s);
            ctx.emit(new ReserveStock(e.orderId, e.sku, e.qty));
        }
    }

    static final class ReserveStockHandler implements EventHandler<ReserveStock> {
        @Override
        public void apply(ReserveStock e, EventContext ctx) {
            ran(ctx);
            StockState s = ctx.state(StockState.class, "sku:" + e.sku).orElse(null);
            if (s == null || s.available < e.qty) {
                throw new NonBlockingError("out of stock: " + e.sku);
            }
            s.available -= e.qty;
            ctx.save(s);
            ctx.emit(new OrderConfirmed(e.orderId));
        }
    }

    static final class RestockHandler implements EventHandler<Restock> {
        @Override
        public void apply(Restock e, EventContext ctx) {
            ran(ctx);
            StockState s = ctx.state(StockState.class, "sku:" + e.sku).orElseGet(StockState::new);
            s.sku = e.sku;
            s.available += e.qty;
            ctx.save(s); // key from the StateType template sku:{sku}
        }
    }

    static final class OrderConfirmedHandler implements EventHandler<OrderConfirmed> {
        @Override
        public void apply(OrderConfirmed e, EventContext ctx) {
            ran(ctx);
            OrderState s = ctx.state(OrderState.class, "order:" + e.orderId).orElseThrow();
            s.status = "CONFIRMED";
            s.events++;
            ctx.save(s);
        }
    }

    static final class StepHandler implements EventHandler<Step> {
        @Override
        public void apply(Step e, EventContext ctx) {
            ran(ctx);
            count(ctx, 1, e.n);
            if (e.n < 3) {
                ctx.emit(new Step(e.chain, e.n + 1));
            }
        }
    }

    static final class CountHandler implements EventHandler<Count> {
        @Override
        public void apply(Count e, EventContext ctx) {
            ran(ctx);
            count(ctx, e.by, e.seq);
        }
    }

    @Handles(Poison.class)
    static final class PoisonHandler implements EventHandler<Poison> {
        @Override
        public void apply(Poison e, EventContext ctx) {
            ran(ctx);
            if (POISONED.contains(e.tag)) {
                throw new BlockingError("poisoned " + e.tag);
            }
            count(ctx, 1, -1);
        }
    }

    @Handles(PoisonNb.class)
    static final class PoisonNbHandler implements EventHandler<PoisonNb> {
        @Override
        public void apply(PoisonNb e, EventContext ctx) {
            ran(ctx);
            if (POISONED.contains(e.tag)) {
                throw new NonBlockingError("poisoned " + e.tag);
            }
            count(ctx, 1, -1);
        }
    }

    static void flaky(Flaky e, EventContext ctx) {
        ran(ctx);
        int attempt = ATTEMPTS.computeIfAbsent(e.tag, k -> new AtomicInteger()).incrementAndGet();
        if (attempt <= FAILS.getOrDefault(e.tag, 0)) {
            throw new IllegalStateException("flaky " + e.tag + " attempt " + attempt);
        }
        count(ctx, 1, -1);
        ctx.emit(new Count(e.tag + "-child", 1, -1));
    }

    @Handles(Flaky.class)
    static final class FlakyHandler implements EventHandler<Flaky> {
        @Override
        public void apply(Flaky e, EventContext ctx) {
            flaky(e, ctx);
        }
    }

    @Handles(FlakyNb.class)
    static final class FlakyNbHandler implements EventHandler<FlakyNb> {
        @Override
        public void apply(FlakyNb e, EventContext ctx) {
            flaky(e, ctx);
        }
    }

    static final class RemindHandler implements EventHandler<Remind> {
        @Override
        public void apply(Remind e, EventContext ctx) {
            ran(ctx);
            ctx.emitAt(Instant.ofEpochMilli(e.atMillis), new Count(e.key, 10, -1));
        }
    }

    static final class OpenTicketHandler implements EventHandler<OpenTicket> {
        @Override
        public void apply(OpenTicket e, EventContext ctx) {
            ran(ctx);
            ctx.emitEntity("ticket", e.ticketId, "assign", "{\"assignee\":\"" + e.assignee + "\"}", List.of());
        }
    }

    static HandlerRegistry shopHandlers() {
        return new HandlerRegistry(SHOP)
                .add(new OrderPlacedHandler())
                .add(new OrderConfirmedHandler())
                .add(new StepHandler())
                .add(new CountHandler())
                .add(new PoisonHandler())
                .add(new PoisonNbHandler())
                .add(new FlakyHandler())
                .add(new FlakyNbHandler())
                .add(new RemindHandler())
                .add(new OpenTicketHandler());
    }

    static HandlerRegistry stockHandlers() {
        return new HandlerRegistry(STOCK).add(new ReserveStockHandler()).add(new RestockHandler());
    }

    // ---- interop: an entity machine that emits an event-style event when assigned

    /** The ticket machine on smType {@code eticket}; assigning emits {@code Count} on {@code counter:<ticket>}. */
    public static class EmittingTicketMachine extends TicketMachine {
        @Override
        protected void onTransition(String from, String to, Ticket ticket, Object payload, EventEnvelope event) {
            super.onTransition(from, to, ticket, payload, event);
            if ("ASSIGNED".equals(to) && "OPEN".equals(from)) {
                emit(new Count("eticket-" + instanceKey(), 1, -1));
            }
        }
    }

    /** An envelope for a test event of this domain with explicit lock keys. */
    static EventEnvelope event(String id, String domain, Object payload, String... keys) {
        String type = EventCatalogs.byClass(payload.getClass()).map(EventCatalog.EventType::name)
                .orElse(payload.getClass().getSimpleName());
        return EventEnvelope.event(id, domain, type, List.of(keys), EventEmissions.payloadJson(payload),
                System.currentTimeMillis()).withIngestTs(System.currentTimeMillis());
    }
}
