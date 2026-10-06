package io.concert.samples.events;

import io.concert.sdk.events.BlockingError;
import io.concert.sdk.events.EventCatalog;
import io.concert.sdk.events.EventCatalogs;
import io.concert.sdk.events.EventContext;
import io.concert.sdk.events.EventHandler;
import io.concert.sdk.events.HandlerRegistry;
import io.concert.sdk.events.NonBlockingError;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal event-style domain {@code demo} for the integration tests and smoke runs (the real showcase, generated
 * from Pure, comes later). Send it with {@code "style":"event","smType":"demo"}:
 *
 * <ul>
 *   <li>{@code Step {chain, n, max}} on {@code chain:{chain}}: counts on the chain and emits the next step until
 *       {@code n == max} (default 3): {@code id -> id.1 -> id.1.1};
 *   <li>{@code Count {key, by}} on {@code counter:{key}}: adds {@code by} to the counter document;
 *   <li>{@code Poison {key, mode}} on {@code counter:{key}}: {@code mode} {@code block} throws a BlockingError,
 *       {@code park} a NonBlockingError (until {@link #healed} is set), anything else counts 1. An operator clears
 *       it with {@code skip}, or with {@code retry} after healing;
 *   <li>{@code Tick {key, seq, chain}} on {@code evc:{key}} (the event-style chaos load, {@code tools loadgen --style
 *       event}): counts into the {@link SeqCounter} document {@code state:evc:<key>}, recording duplicates and
 *       out-of-order sequence numbers; with {@code chain} it emits a {@code ChainTick} on {@code chain:{key}}, counted
 *       into {@code state:chain:<key>}.
 * </ul>
 */
public final class DemoEvents {
    private DemoEvents() {}

    public static final String DOMAIN = "demo";

    public static class Step {
        public String chain;
        public int n = 1;
        public int max = 3;
    }

    public static class Count {
        public String key;
        public int by = 1;
    }

    public static class Poison {
        public String key;
        public String mode;
    }

    public static class Tick {
        public String key;
        public long seq;
        public boolean chain;
    }

    public static class ChainTick {
        public String key;
        public long seq;
    }

    /** Keyed state of the chaos load: what arrived on one key, in which order. */
    public static class SeqCounter {
        public String key;
        public long count;
        public long lastSeq = -1;
        public long outOfOrder;
        public long duplicates;
    }

    /** Keyed state {@code state:<key>}. */
    public static class Counter {
        public String key;
        public long value;
        public List<String> seen = new ArrayList<>();
    }

    public static final EventCatalog CATALOG = EventCatalog.builder("demo")
            .event(DOMAIN, Step.class, "chain:{chain}")
            .event(DOMAIN, Count.class, "counter:{key}")
            .event(DOMAIN, Poison.class, "counter:{key}")
            .event(DOMAIN, Tick.class, "evc:{key}")
            .event(DOMAIN, ChainTick.class, "chain:{key}")
            .state(Counter.class, null)
            .state(SeqCounter.class, null)
            .build();

    /** Set by an operator (or test) to make poisoned events succeed on retry. */
    public static volatile boolean healed;

    static void count(EventContext ctx, String key, long by) {
        Counter c = ctx.state(Counter.class, key).orElseGet(() -> {
            Counter n = new Counter();
            n.key = key;
            return n;
        });
        c.value += by;
        c.seen.add(ctx.envelope().eventId());
        ctx.save(key, c);
    }

    public static final class StepHandler implements EventHandler<Step> {
        @Override
        public void apply(Step e, EventContext ctx) {
            count(ctx, "chain:" + e.chain, 1);
            if (e.n < e.max) {
                Step next = new Step();
                next.chain = e.chain;
                next.n = e.n + 1;
                next.max = e.max;
                ctx.emit(next);
            }
        }
    }

    public static final class CountHandler implements EventHandler<Count> {
        @Override
        public void apply(Count e, EventContext ctx) {
            count(ctx, "counter:" + e.key, e.by);
        }
    }

    public static final class PoisonHandler implements EventHandler<Poison> {
        @Override
        public void apply(Poison e, EventContext ctx) {
            if (!healed && "block".equals(e.mode)) {
                throw new BlockingError("poison " + e.key + " (blocking)");
            }
            if (!healed && "park".equals(e.mode)) {
                throw new NonBlockingError("poison " + e.key + " (non-blocking)");
            }
            count(ctx, "counter:" + e.key, 1);
        }
    }

    static SeqCounter seq(EventContext ctx, String key) {
        return ctx.state(SeqCounter.class, key).orElseGet(() -> {
            SeqCounter n = new SeqCounter();
            n.key = key;
            return n;
        });
    }

    public static final class TickHandler implements EventHandler<Tick> {
        @Override
        public void apply(Tick e, EventContext ctx) {
            String key = "evc:" + e.key;
            SeqCounter c = seq(ctx, key);
            if (e.seq == c.lastSeq) {
                c.duplicates++;
            } else if (e.seq < c.lastSeq) {
                c.outOfOrder++;
            } else {
                c.count++;
                c.lastSeq = e.seq;
            }
            ctx.save(key, c);
            if (e.chain) {
                ChainTick next = new ChainTick();
                next.key = e.key;
                next.seq = e.seq;
                ctx.emit(next);
            }
        }
    }

    public static final class ChainTickHandler implements EventHandler<ChainTick> {
        @Override
        public void apply(ChainTick e, EventContext ctx) {
            String key = "chain:" + e.key;
            SeqCounter c = seq(ctx, key);
            c.count++;
            c.lastSeq = Math.max(c.lastSeq, e.seq);
            ctx.save(key, c);
        }
    }

    /** Registers the catalog and returns the handlers. */
    public static HandlerRegistry handlers() {
        EventCatalogs.register(CATALOG);
        return new HandlerRegistry(DOMAIN).add(new StepHandler()).add(new CountHandler()).add(new PoisonHandler())
                .add(new TickHandler()).add(new ChainTickHandler());
    }
}
