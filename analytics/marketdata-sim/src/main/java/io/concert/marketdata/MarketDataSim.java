package io.concert.marketdata;

import io.concert.model.runtime.ModelJson;
import io.concert.trading.marketdata.Bar;
import io.concert.trading.marketdata.Tick;
import io.concert.trading.refdata.Account;
import io.concert.trading.refdata.Instrument;
import io.concert.trading.refdata.Venue;
import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The simulator loop, independent of where records go. Every value is a class generated from
 * {@code trading-model} ({@code Instrument}, {@code Account}, {@code Venue}, {@code Tick}, {@code Bar})
 * written with {@code ModelJson}: sorted properties, ISO-8601 UTC instants, decimals as plain JSON
 * numbers, absent optional properties omitted. The simulator: publishes the reference data once, then ticks
 * at a fixed total rate (symbols chosen at random, so about {@code tickRate / symbols} per symbol per
 * second) and, optionally, 1-minute bars as minutes close. Tick {@code i} is stamped
 * {@code start + i / tickRate}, so the stream is evenly spaced and reproducible for a given start.
 */
public final class MarketDataSim {

    /**
     * @param symbols {@link MarketUniverse#selectInstruments} spec
     * @param tickRate ticks per second over all symbols
     */
    public record Config(long seed, double volMultiplier, String symbols, int accounts, double tickRate, boolean bars) {

        public Config {
            if (!(tickRate > 0)) {
                throw new IllegalArgumentException("tickRate must be > 0");
            }
        }

        public static Config defaults() {
            return new Config(PriceModel.DEFAULT_SEED, PriceModel.DEFAULT_VOL_MULTIPLIER, "all",
                    MarketUniverse.DEFAULT_ACCOUNTS, 200, true);
        }
    }

    private final Config config;
    private final RecordSink sink;
    private final MarketUniverse universe;
    private final List<Instrument> instruments;
    private final TickGenerator ticks;
    private final BarAggregator bars = new BarAggregator();
    private long ticksSent;
    private long barsSent;

    public MarketDataSim(Config config, RecordSink sink) {
        this.config = config;
        this.sink = sink;
        this.universe = MarketUniverse.create(config.seed(), config.accounts());
        this.instruments = universe.selectInstruments(config.symbols());
        this.ticks = new TickGenerator(instruments, universe.venues(), new PriceModel(config.seed(), config.volMultiplier()),
                config.seed());
    }

    public MarketUniverse universe() {
        return universe;
    }

    public List<Instrument> instruments() {
        return instruments;
    }

    public long ticksSent() {
        return ticksSent;
    }

    public long barsSent() {
        return barsSent;
    }

    /**
     * Publishes every selected instrument, every account and every venue, stamped {@code updatedAt =
     * now} (compacted topics, so republishing is idempotent). The universe's objects are not modified.
     */
    public void publishReference(Instant now) {
        instruments.forEach(i -> sink.send(MdTopics.INSTRUMENTS, i.getSymbol(), stamped(i, now)));
        universe.accounts().forEach(a -> sink.send(MdTopics.ACCOUNTS, a.getAccountId(), stamped(a, now)));
        universe.venues().forEach(v -> sink.send(MdTopics.VENUES, v.getMic(), stamped(v, now)));
        sink.flush();
    }

    // A JSON round trip is a deep copy that keeps decimals exact.
    private static String stamped(Instrument i, Instant now) {
        return ModelJson.write(ModelJson.read(ModelJson.write(i), Instrument.class).setUpdatedAt(now));
    }

    private static String stamped(Account a, Instant now) {
        return ModelJson.write(ModelJson.read(ModelJson.write(a), Account.class).setUpdatedAt(now));
    }

    private static String stamped(Venue v, Instant now) {
        return ModelJson.write(ModelJson.read(ModelJson.write(v), Venue.class).setUpdatedAt(now));
    }

    /** Emits one tick stamped {@code tsMillis} and any bars it closes. */
    public Tick emitTick(long tsMillis) {
        Tick t = ticks.next(tsMillis);
        sink.send(MdTopics.TICKS, t.getSymbol(), ModelJson.write(t));
        ticksSent++;
        if (config.bars()) {
            bars.onTick(t).forEach(this::sendBar);
        }
        return t;
    }

    /** Emits {@code count} ticks on a virtual clock starting at {@code startMillis}, without sleeping. */
    public void runVirtual(long startMillis, long count) {
        for (long i = 0; i < count; i++) {
            emitTick(startMillis + scheduledOffset(i));
        }
        sink.flush();
    }

    /** Emits ticks in real time until {@code keepRunning} turns false, then flushes. */
    public void runRealtime(LongSupplier clockMillis, BooleanSupplier keepRunning) throws InterruptedException {
        runRealtime(clockMillis, keepRunning, new Control(true, config.tickRate()));
    }

    /**
     * Start / stop and tick rate of a realtime run, changed from another thread (the control endpoint).
     * A pause emits nothing; resuming or changing the rate re-anchors the schedule, so there is no burst.
     */
    public static final class Control {
        private volatile boolean running;
        private volatile double tickRate;
        private volatile long generation;

        public Control(boolean running, double tickRate) {
            this.running = running;
            this.tickRate = tickRate;
        }

        public boolean running() {
            return running;
        }

        public double tickRate() {
            return tickRate;
        }

        public synchronized void start(Double rate) {
            if (rate != null && rate > 0) {
                tickRate = Math.min(rate, 20_000);
            }
            running = true;
            generation++;
        }

        public synchronized void stop() {
            running = false;
            generation++;
        }
    }

    /** Emits ticks in real time while {@code control} is running, until {@code keepRunning} turns false. */
    public void runRealtime(LongSupplier clockMillis, BooleanSupplier keepRunning, Control control) throws InterruptedException {
        long start = clockMillis.getAsLong();
        long i = 0;
        long seen = control.generation;
        while (keepRunning.getAsBoolean()) {
            if (control.generation != seen) {
                seen = control.generation;
                start = clockMillis.getAsLong();
                i = 0;
                sink.flush();
            }
            if (control.running()) {
                double rate = control.tickRate();
                long elapsed = clockMillis.getAsLong() - start;
                while (scheduledOffset(i, rate) <= elapsed) {
                    emitTick(start + scheduledOffset(i++, rate));
                }
            }
            Thread.sleep(5);
        }
        sink.flush();
    }

    private long scheduledOffset(long i) {
        return scheduledOffset(i, config.tickRate());
    }

    private static long scheduledOffset(long i, double rate) {
        return (long) (i * 1000.0 / rate);
    }

    private void sendBar(Bar b) {
        sink.send(MdTopics.BARS_1M, b.getSymbol(), ModelJson.write(b));
        barsSent++;
    }
}
