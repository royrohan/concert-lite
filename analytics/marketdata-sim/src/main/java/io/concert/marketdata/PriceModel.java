package io.concert.marketdata;

import io.concert.trading.common.Sector;
import io.concert.trading.marketdata.Quote;
import io.concert.trading.refdata.Instrument;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic price paths: the quote of a symbol at an instant is a pure function of
 * {@code (seed, volMultiplier, symbol, time)}. The market data simulator and the trading generator
 * each evaluate it independently and agree to the tick, so fills land on the bid/ask that
 * {@code md.ticks} shows at the fill's timestamp, and slippage analytics (fills as-of-joined to
 * ticks) are meaningful.
 *
 * <p>Model, per symbol:
 *
 * <ul>
 *   <li>Within a UTC day, the log price follows a Brownian motion (GBM with zero log drift) sampled on
 *       a 100 ms grid. It is generated as a Brownian bridge over 2^20 steps with counter-based normals,
 *       so any instant is evaluated in O(20) without simulating the path up to it.
 *   <li>Each day opens at the previous close pulled 20% back toward the reference price (a log-space
 *       mean reversion), which keeps prices realistic for any wall-clock date; the pull shows up as a
 *       small overnight gap. The opening level starts from the reference price 30 days earlier, so it
 *       too is a pure function of the date.
 *   <li>Volatility = sector vol ({@link #SECTOR_VOL}) x a per-symbol factor in [0.8, 1.2) x {@code volMultiplier}, which
 *       speeds the market up for demos (the default {@value #DEFAULT_VOL_MULTIPLIER} makes a 10 minute
 *       session show visible moves).
 *   <li>Quotes: the spread is about 1 bp of price (at least one tick) plus 0-2 random ticks; bid and
 *       ask are on the tick grid around the fair price; sizes are 1-20 lots.
 * </ul>
 */
public final class PriceModel {

    public static final long DEFAULT_SEED = 42;
    public static final double DEFAULT_VOL_MULTIPLIER = 4.0;
    /** Quotes change at most this often per symbol. */
    public static final long STEP_MILLIS = 100;

    private static final int LEVELS = 20;
    private static final long BRIDGE_STEPS = 1L << LEVELS; // 2^20 x 100 ms = 29 h, covers a day
    private static final long DAY_MILLIS = 86_400_000L;
    private static final long STEPS_PER_DAY = DAY_MILLIS / STEP_MILLIS;
    private static final double YEAR_MILLIS = 365.25 * DAY_MILLIS;
    private static final int WARMUP_DAYS = 30;
    private static final double REVERSION = 0.2;

    private static final long SALT_VOL = 1;
    private static final long SALT_PATH = 2;
    private static final long SALT_SPREAD = 3;
    private static final long SALT_SIZE = 4;

    /** Typical annualized volatility per sector: the base of each symbol's volatility. */
    public static final Map<Sector, Double> SECTOR_VOL = sectorVols();

    private static Map<Sector, Double> sectorVols() {
        Map<Sector, Double> m = new EnumMap<>(Sector.class);
        m.put(Sector.TECHNOLOGY, 0.32);
        m.put(Sector.COMMUNICATION, 0.28);
        m.put(Sector.CONSUMER_DISCRETIONARY, 0.30);
        m.put(Sector.CONSUMER_STAPLES, 0.16);
        m.put(Sector.FINANCIALS, 0.24);
        m.put(Sector.HEALTHCARE, 0.22);
        m.put(Sector.ENERGY, 0.30);
        m.put(Sector.INDUSTRIALS, 0.22);
        m.put(Sector.UTILITIES, 0.16);
        m.put(Sector.MATERIALS, 0.26);
        m.put(Sector.REAL_ESTATE, 0.24);
        m.put(Sector.BROAD_MARKET, 0.15);
        if (m.size() != Sector.values().length) {
            throw new IllegalStateException("a sector has no volatility");
        }
        return java.util.Collections.unmodifiableMap(m);
    }

    private final long seed;
    private final double volMultiplier;
    private final Map<String, Double> dayOpenCache = new ConcurrentHashMap<>();

    public PriceModel(long seed, double volMultiplier) {
        if (!(volMultiplier > 0)) {
            throw new IllegalArgumentException("volMultiplier must be > 0");
        }
        this.seed = seed;
        this.volMultiplier = volMultiplier;
    }

    public PriceModel(long seed) {
        this(seed, DEFAULT_VOL_MULTIPLIER);
    }

    public long seed() {
        return seed;
    }

    public double volMultiplier() {
        return volMultiplier;
    }

    /** Annualized volatility of the symbol before the multiplier. */
    public double annualVol(Instrument i) {
        double factor = 0.8 + 0.4 * Hashing.uniform(Hashing.hash(seed, Hashing.fnv(i.getSymbol()), SALT_VOL, 0));
        return SECTOR_VOL.get(i.getSector()) * factor;
    }

    /** The unrounded fair price at {@code tsMillis}; always positive. */
    public double fairPrice(Instrument i, long tsMillis) {
        long day = Math.floorDiv(tsMillis, DAY_MILLIS);
        long step = Math.floorMod(tsMillis, DAY_MILLIS) / STEP_MILLIS;
        double logPx = dayOpen(i, day) + sigmaPerStep(i) * bridge(i.getSymbol(), day, step);
        return i.getRefPrice().doubleValue() * Math.exp(logPx);
    }

    /** Top of book at {@code tsMillis}: bid < ask, both positive multiples of the tick size. */
    public Quote quote(Instrument i, long tsMillis) {
        long sym = Hashing.fnv(i.getSymbol());
        long step = Math.floorDiv(tsMillis, STEP_MILLIS);
        double fair = fairPrice(i, tsMillis);
        BigDecimal tick = i.getTickSize();
        double t = tick.doubleValue();
        long baseTicks = Math.max(1, Math.round(fair * 0.0001 / t));
        long spreadTicks = baseTicks + Long.remainderUnsigned(Hashing.hash(seed, sym, SALT_SPREAD, step), 3);
        long bidTicks = Math.max(1, (long) Math.floor((fair - spreadTicks * t / 2) / t));
        long sizes = Hashing.hash(seed, sym, SALT_SIZE, step);
        long bidLots = 1 + Long.remainderUnsigned(sizes, 20);
        long askLots = 1 + Long.remainderUnsigned(sizes >>> 32, 20);
        BigDecimal bid = tick.multiply(BigDecimal.valueOf(bidTicks));
        BigDecimal ask = tick.multiply(BigDecimal.valueOf(bidTicks + spreadTicks));
        return new Quote().setSymbol(i.getSymbol()).setTs(Instant.ofEpochMilli(tsMillis))
                .setBid(bid).setBidSize(bidLots * i.getLotSize()).setAsk(ask).setAskSize(askLots * i.getLotSize())
                .setMid(bid.add(ask).divide(BigDecimal.TWO));
    }

    private double sigmaPerStep(Instrument i) {
        return annualVol(i) * volMultiplier * Math.sqrt(STEP_MILLIS / YEAR_MILLIS);
    }

    /** Log price (relative to the reference) at the start of {@code day}. */
    private double dayOpen(Instrument i, long day) {
        return dayOpenCache.computeIfAbsent(i.getSymbol() + '@' + day, k -> {
            double sigma = sigmaPerStep(i);
            double x = 0;
            for (long d = day - WARMUP_DAYS; d < day; d++) {
                x = (1 - REVERSION) * (x + sigma * bridge(i.getSymbol(), d, STEPS_PER_DAY));
            }
            return x;
        });
    }

    /**
     * Standard Brownian motion (unit variance per step) of the given symbol and day at step {@code n}.
     * W(2^20) is drawn first; each midpoint is then drawn from its bridge distribution given the two
     * ends, descending only along the path to {@code n}. Every dyadic point has its own counter, so
     * the path is the same whichever points are evaluated, in whatever order.
     */
    private double bridge(String symbol, long day, long n) {
        long key = Hashing.hash(seed, Hashing.fnv(symbol), SALT_PATH, day);
        long a = 0;
        long b = BRIDGE_STEPS;
        double wa = 0;
        double wb = Math.sqrt(BRIDGE_STEPS) * Hashing.gaussian(Hashing.mix(key ^ b));
        while (true) {
            if (n == a) {
                return wa;
            }
            if (n == b) {
                return wb;
            }
            long c = (a + b) >>> 1;
            double wc = (wa + wb) / 2 + Math.sqrt((b - a) / 4.0) * Hashing.gaussian(Hashing.mix(key ^ c));
            if (n < c) {
                b = c;
                wb = wc;
            } else {
                a = c;
                wa = wc;
            }
        }
    }
}
