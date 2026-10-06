package io.concert.trading;

import io.concert.common.EventEnvelope;
import io.concert.marketdata.MarketUniverse;
import io.concert.marketdata.PriceModel;
import io.concert.trading.command.AckCommand;
import io.concert.trading.command.AllocateCommand;
import io.concert.trading.command.CancelCommand;
import io.concert.trading.command.CloseOrderCommand;
import io.concert.trading.command.ConfirmAllocationCommand;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.NewOrderCommand;
import io.concert.trading.command.RejectCommand;
import io.concert.trading.command.RouteCommand;
import io.concert.trading.common.Liquidity;
import io.concert.trading.common.OrderType;
import io.concert.trading.common.Side;
import io.concert.trading.common.TimeInForce;
import io.concert.trading.marketdata.Quote;
import io.concert.trading.refdata.Account;
import io.concert.trading.refdata.Instrument;
import io.concert.trading.refdata.Venue;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SplittableRandom;

/**
 * Realistic equities order flow as concert events, without any I/O: an iterator of
 * {@link EventEnvelope}s that {@link TradingLoadGen} publishes (or prints).
 *
 * <p>Per order: {@code submit} → {@code ack} (or {@code reject}, ~3%) → 1-3 {@code route}s to distinct
 * venues → partial fills interleaved across the executions until filled, or until a {@code cancel}
 * (~10%) → {@code allocate} to 1-3 accounts → {@code confirm} each → {@code close}. All quantities are
 * whole lots. Up to {@code concurrency} orders are in flight; each step picks a random in-flight order
 * and emits its next event, so orders interleave but each entity's events stay in order.
 *
 * <p>Payloads are the classes generated from {@code commands.pure} ({@link TradingModels}), written with
 * {@code ModelJson}, so the JSON is exactly what phase B's machines read.
 *
 * <p>Event {@code i} is stamped {@code start + i / eventsPerSecond}. Fill prices come from
 * {@link PriceModel} at that timestamp (aggressive fills at the far touch, passive at the near touch,
 * capped by the limit price), the same quotes {@code marketdata-sim} publishes for the same seed. So
 * the output is a pure function of the config, and slippage against {@code md.ticks} is meaningful
 * when both run with the same seed and vol multiplier. The order flow itself is drawn from the seed
 * mixed with the run id ({@link #flowSeed}), so repeated runs (new run ids) send different orders
 * against the same price model.
 */
public final class TradingFlowGenerator implements Iterator<EventEnvelope> {

    /**
     * @param runId prefix of every id (orders {@code <runId>-O000001}, events {@code <runId>-e1}); a new
     *     run id per run keeps the platform's dedupe from swallowing a re-run
     * @param symbols / accounts {@link MarketUniverse#selectInstruments} specs
     * @param concurrency orders in flight at once
     */
    public record Config(long seed, double volMultiplier, String runId, int orders, String symbols, String accounts,
            int concurrency, long startMillis, double eventsPerSecond, double rejectRate, double cancelRate, double limitRate) {

        public Config {
            if (orders < 0 || concurrency < 1 || !(eventsPerSecond > 0)) {
                throw new IllegalArgumentException("need orders >= 0, concurrency >= 1, eventsPerSecond > 0");
            }
        }

        public static Config defaults(String runId, long startMillis) {
            return new Config(PriceModel.DEFAULT_SEED, PriceModel.DEFAULT_VOL_MULTIPLIER, runId, 100, "all", "all", 20,
                    startMillis, 50, 0.03, 0.10, 0.40);
        }
    }

    private enum Kind {
        SUBMIT, REJECT, ACK, ROUTE_EXEC, ROUTE_ORDER, BOOK, EXEC_FILL, ORDER_FILL, CANCEL_EXEC, CANCEL_ORDER,
        ALLOC, ORDER_ALLOC, CONFIRM, CLOSE
    }

    /** @param index execution, fill or allocation index, depending on the kind */
    private record Step(Kind kind, int index) {}

    private record PlannedExec(String execId, String venue, long quantity) {}

    private record PlannedFill(String fillId, int exec, long quantity, Liquidity liquidity) {}

    private record PlannedAlloc(String allocId, String accountId, long quantity) {}

    private final Config config;
    private final PriceModel prices;
    private final List<Instrument> instruments;
    private final List<Account> accounts;
    private final List<Venue> venues;
    private final SplittableRandom rnd;
    private final List<OrderFlow> inFlight = new ArrayList<>();
    private int started;
    private long emitted;

    public TradingFlowGenerator(Config config) {
        this(config, MarketUniverse.create(config.seed()));
    }

    public TradingFlowGenerator(Config config, MarketUniverse universe) {
        this.config = config;
        this.prices = new PriceModel(config.seed(), config.volMultiplier());
        this.instruments = universe.selectInstruments(config.symbols());
        this.accounts = universe.selectAccounts(config.accounts());
        this.venues = universe.venues();
        this.rnd = new SplittableRandom(flowSeed(config.seed(), config.runId()));
        refill();
    }

    /**
     * Seed of the order-flow RNG (which orders, sides, sizes, venues, fills, cancels, allocations): the
     * price seed mixed with the run id, so every run (a new run id by default) produces different orders,
     * while instruments, accounts and prices still come from {@code seed} alone and stay aligned with
     * {@code marketdata-sim}. The same seed and run id reproduce a run exactly.
     */
    static long flowSeed(long seed, String runId) {
        long h = 0xcbf29ce484222325L; // FNV-1a over the run id's UTF-16 units, then a SplitMix64 finalizer
        for (int i = 0; i < runId.length(); i++) {
            h = (h ^ runId.charAt(i)) * 0x100000001b3L;
        }
        long z = seed ^ h;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    @Override
    public boolean hasNext() {
        return !inFlight.isEmpty();
    }

    @Override
    public EventEnvelope next() {
        if (inFlight.isEmpty()) {
            throw new NoSuchElementException();
        }
        int pick = rnd.nextInt(inFlight.size());
        OrderFlow flow = inFlight.get(pick);
        long ts = config.startMillis() + (long) (emitted * 1000.0 / config.eventsPerSecond());
        EventEnvelope e = flow.emit(flow.steps.removeFirst(), config.runId() + "-e" + (++emitted), ts);
        if (flow.steps.isEmpty()) {
            inFlight.remove(pick);
            refill();
        }
        return e;
    }

    private void refill() {
        while (inFlight.size() < config.concurrency() && started < config.orders()) {
            inFlight.add(new OrderFlow(++started, rnd.split()));
        }
    }

    /** One order's plan (decided up front) and running state (prices are taken as events are emitted). */
    private final class OrderFlow {
        final String orderId;
        final Instrument instrument;
        final Account account;
        final Side side;
        final OrderType orderType;
        final TimeInForce tif;
        final long quantity;
        final List<PlannedExec> execs = new ArrayList<>();
        final List<PlannedFill> fills = new ArrayList<>();
        final List<PlannedAlloc> allocs = new ArrayList<>();
        final Deque<Step> steps = new ArrayDeque<>();

        BigDecimal limitPrice;
        final List<FillCommand> booked = new ArrayList<>();
        final long[] execFilled;
        long filled;
        BigDecimal notional = BigDecimal.ZERO;

        OrderFlow(int n, SplittableRandom r) {
            orderId = "%s-O%06d".formatted(config.runId(), n);
            instrument = instruments.get(r.nextInt(instruments.size()));
            account = accounts.get(r.nextInt(accounts.size()));
            double s = r.nextDouble();
            side = s < 0.5 ? Side.BUY : s < 0.9 ? Side.SELL : Side.SELL_SHORT;
            orderType = r.nextDouble() < config.limitRate() ? OrderType.LIMIT : OrderType.MARKET;
            tif = r.nextDouble() < 0.9 ? TimeInForce.DAY : TimeInForce.GTC;
            long lot = instrument.getLotSize();
            long lots = 1 + r.nextInt(100);
            quantity = lots * lot;

            steps.add(new Step(Kind.SUBMIT, 0));
            if (r.nextDouble() < config.rejectRate()) {
                steps.add(new Step(Kind.REJECT, 0));
                execFilled = new long[0];
                return;
            }
            steps.add(new Step(Kind.ACK, 0));

            // Child orders: split the lots over 1-3 distinct venues.
            List<Venue> shuffled = new ArrayList<>(venues);
            Collections.shuffle(shuffled, new java.util.Random(r.nextLong()));
            long[] execLots = split(lots, 1 + r.nextInt((int) Math.min(3, lots)), r);
            for (int e = 0; e < execLots.length; e++) {
                execs.add(new PlannedExec(orderId + "-E" + (e + 1), shuffled.get(e).getMic(), execLots[e] * lot));
                steps.add(new Step(Kind.ROUTE_EXEC, e));
                steps.add(new Step(Kind.ROUTE_ORDER, e));
            }
            execFilled = new long[execs.size()];

            // Partial fills: 1-4 per execution, interleaved across executions, each execution in order.
            List<Deque<Long>> pending = new ArrayList<>();
            for (long el : execLots) {
                Deque<Long> parts = new ArrayDeque<>();
                for (long p : split(el, 1 + r.nextInt((int) Math.min(4, el)), r)) {
                    parts.add(p * lot);
                }
                pending.add(parts);
            }
            int totalFills = pending.stream().mapToInt(Deque::size).sum();
            boolean cancel = r.nextDouble() < config.cancelRate();
            int executedFills = cancel ? r.nextInt(totalFills) : totalFills;
            long cumFilled = 0;
            long[] execCum = new long[execs.size()];
            for (int f = 0; f < executedFills; f++) {
                int e;
                do {
                    e = r.nextInt(pending.size());
                } while (pending.get(e).isEmpty());
                long q = pending.get(e).removeFirst();
                fills.add(new PlannedFill(orderId + "-F" + (f + 1), e, q, r.nextDouble() < 0.65 ? Liquidity.REMOVE : Liquidity.ADD));
                steps.add(new Step(Kind.BOOK, f));
                steps.add(new Step(Kind.EXEC_FILL, f));
                steps.add(new Step(Kind.ORDER_FILL, f));
                cumFilled += q;
                execCum[e] += q;
            }
            if (cancel) {
                for (int e = 0; e < execs.size(); e++) {
                    if (execCum[e] < execs.get(e).quantity()) {
                        steps.add(new Step(Kind.CANCEL_EXEC, e));
                    }
                }
                steps.add(new Step(Kind.CANCEL_ORDER, 0));
            }

            // Allocations of the filled quantity to 1-3 distinct accounts.
            if (cumFilled > 0) {
                long filledLots = cumFilled / lot;
                List<Account> targets = new ArrayList<>(accounts);
                Collections.shuffle(targets, new java.util.Random(r.nextLong()));
                long[] allocLots = split(filledLots, 1 + r.nextInt((int) Math.min(Math.min(3, filledLots), targets.size())), r);
                for (int a = 0; a < allocLots.length; a++) {
                    allocs.add(new PlannedAlloc(orderId + "-A" + (a + 1), targets.get(a).getAccountId(), allocLots[a] * lot));
                    steps.add(new Step(Kind.ALLOC, a));
                    steps.add(new Step(Kind.ORDER_ALLOC, a));
                }
                for (int a = 0; a < allocLots.length; a++) {
                    steps.add(new Step(Kind.CONFIRM, a));
                }
                steps.add(new Step(Kind.CLOSE, 0));
            }
        }

        EventEnvelope emit(Step step, String eventId, long ts) {
            Instant at = Instant.ofEpochMilli(ts);
            String acct = account.getAccountId();
            return switch (step.kind()) {
                case SUBMIT -> {
                    Quote q = prices.quote(instrument, ts);
                    if (orderType == OrderType.LIMIT) {
                        // Marketable limit 20 bp through the far touch, on the tick grid.
                        BigDecimal tick = instrument.getTickSize();
                        limitPrice = side == Side.BUY
                                ? roundToTick(q.getAsk().multiply(new BigDecimal("1.002")), tick, RoundingMode.CEILING)
                                : roundToTick(q.getBid().multiply(new BigDecimal("0.998")), tick, RoundingMode.FLOOR);
                    }
                    yield TradingEvents.envelope(TradingFlows.ORDER, "submit", eventId, new NewOrderCommand()
                            .setOrderId(orderId).setAccountId(acct).setTs(at)
                            .setClientId("CL-" + acct.substring(acct.indexOf('-') + 1)).setSymbol(instrument.getSymbol())
                            .setSide(side).setOrderType(orderType).setLimitPrice(limitPrice).setTimeInForce(tif)
                            .setCurrency(instrument.getCurrency()).setQuantity(quantity)
                            .setArrivalPx(q.getMid()), ts);
                }
                case REJECT -> TradingEvents.envelope(TradingFlows.ORDER, "reject", eventId, new RejectCommand()
                        .setOrderId(orderId).setAccountId(acct).setTs(at).setReason("credit limit exceeded"), ts);
                case ACK -> TradingEvents.envelope(TradingFlows.ORDER, "ack", eventId,
                        new AckCommand().setOrderId(orderId).setAccountId(acct).setTs(at).setAckedBy("OMS"), ts);
                case ROUTE_EXEC, ROUTE_ORDER -> {
                    PlannedExec x = execs.get(step.index());
                    RouteCommand cmd = new RouteCommand().setOrderId(orderId).setAccountId(acct).setTs(at).setExecId(x.execId())
                            .setVenue(x.venue()).setSymbol(instrument.getSymbol()).setSide(side).setQuantity(x.quantity())
                            .setLimitPrice(limitPrice);
                    yield TradingEvents.envelope(step.kind() == Kind.ROUTE_EXEC ? TradingFlows.EXECUTION : TradingFlows.ORDER,
                            "route", eventId, cmd, ts);
                }
                case BOOK -> {
                    PlannedFill f = fills.get(step.index());
                    PlannedExec x = execs.get(f.exec());
                    BigDecimal px = fillPrice(prices.quote(instrument, ts), f.liquidity());
                    Venue v = venues.stream().filter(vv -> vv.getMic().equals(x.venue())).findFirst().orElseThrow();
                    BigDecimal feeRate = f.liquidity() == Liquidity.ADD ? v.getMakerFeePerShare() : v.getTakerFeePerShare();
                    FillCommand cmd = new FillCommand().setOrderId(orderId).setAccountId(acct).setTs(at).setFillId(f.fillId())
                            .setExecId(x.execId()).setSymbol(instrument.getSymbol()).setSide(side).setQuantity(f.quantity())
                            .setPrice(px).setVenue(x.venue()).setLiquidity(f.liquidity())
                            .setFee(feeRate.multiply(BigDecimal.valueOf(f.quantity())).setScale(4, RoundingMode.HALF_EVEN));
                    booked.add(cmd);
                    yield TradingEvents.envelope(TradingFlows.FILL, "book", eventId, cmd, ts);
                }
                case EXEC_FILL -> {
                    PlannedFill f = fills.get(step.index());
                    execFilled[f.exec()] += f.quantity();
                    boolean done = execFilled[f.exec()] == execs.get(f.exec()).quantity();
                    yield TradingEvents.envelope(TradingFlows.EXECUTION, done ? "complete_fill" : "fill", eventId,
                            booked.get(step.index()), ts);
                }
                case ORDER_FILL -> {
                    FillCommand cmd = booked.get(step.index());
                    filled += cmd.getQuantity();
                    notional = notional.add(cmd.getPrice().multiply(BigDecimal.valueOf(cmd.getQuantity())));
                    yield TradingEvents.envelope(TradingFlows.ORDER, filled == quantity ? "complete_fill" : "fill", eventId, cmd, ts);
                }
                case CANCEL_EXEC -> TradingEvents.envelope(TradingFlows.EXECUTION, "cancel", eventId, new CancelCommand()
                        .setOrderId(orderId).setAccountId(acct).setTs(at).setReason("parent order cancelled")
                        .setExecId(execs.get(step.index()).execId()), ts);
                case CANCEL_ORDER -> TradingEvents.envelope(TradingFlows.ORDER, "cancel", eventId, new CancelCommand()
                        .setOrderId(orderId).setAccountId(acct).setTs(at).setReason("client cancel"), ts);
                case ALLOC, ORDER_ALLOC -> {
                    PlannedAlloc a = allocs.get(step.index());
                    AllocateCommand cmd = new AllocateCommand().setOrderId(orderId).setAccountId(acct).setTs(at)
                            .setAllocId(a.allocId()).setAllocAccountId(a.accountId()).setSymbol(instrument.getSymbol()).setSide(side)
                            .setQuantity(a.quantity()).setAvgPx(avgPx());
                    yield TradingEvents.envelope(step.kind() == Kind.ALLOC ? TradingFlows.ALLOCATION : TradingFlows.ORDER,
                            "allocate", eventId, cmd, ts);
                }
                case CONFIRM -> {
                    PlannedAlloc a = allocs.get(step.index());
                    yield TradingEvents.envelope(TradingFlows.ALLOCATION, "confirm", eventId, new ConfirmAllocationCommand()
                            .setOrderId(orderId).setAccountId(acct).setTs(at).setAllocId(a.allocId())
                            .setAllocAccountId(a.accountId()).setConfirmedBy("middle-office"), ts);
                }
                case CLOSE -> TradingEvents.envelope(TradingFlows.ORDER, "close", eventId, new CloseOrderCommand()
                        .setOrderId(orderId).setAccountId(acct).setTs(at)
                        .setAllocatedQty(allocs.stream().mapToLong(PlannedAlloc::quantity).sum())
                        .setAllocationCount((long) allocs.size()), ts);
            };
        }

        BigDecimal avgPx() {
            return notional.divide(BigDecimal.valueOf(filled), 6, RoundingMode.HALF_EVEN);
        }

        /** Aggressive (REMOVE) fills cross to the far touch, passive (ADD) rest at the near touch; capped by the limit. */
        BigDecimal fillPrice(Quote q, Liquidity liquidity) {
            boolean buy = side == Side.BUY;
            BigDecimal px = (liquidity == Liquidity.REMOVE) == buy ? q.getAsk() : q.getBid();
            if (limitPrice != null) {
                px = buy ? px.min(limitPrice) : px.max(limitPrice);
            }
            return px;
        }
    }

    /** Splits {@code total} into {@code parts} positive integers, randomly. */
    static long[] split(long total, int parts, SplittableRandom r) {
        if (parts < 1 || parts > total) {
            throw new IllegalArgumentException("cannot split " + total + " into " + parts);
        }
        // Choose parts-1 distinct cut points in 1..total-1.
        java.util.TreeSet<Long> cuts = new java.util.TreeSet<>();
        while (cuts.size() < parts - 1) {
            cuts.add(1 + r.nextLong(total - 1));
        }
        long[] out = new long[parts];
        long prev = 0;
        int i = 0;
        for (long c : cuts) {
            out[i++] = c - prev;
            prev = c;
        }
        out[i] = total - prev;
        return out;
    }

    static BigDecimal roundToTick(BigDecimal px, BigDecimal tick, RoundingMode mode) {
        return px.divide(tick, 0, mode).multiply(tick);
    }
}
