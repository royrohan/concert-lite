package io.concert.marketdata;

import io.concert.trading.marketdata.Quote;
import io.concert.trading.marketdata.Tick;
import io.concert.trading.refdata.Instrument;
import io.concert.trading.refdata.Venue;
import io.concert.trading.refdata.VenueType;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Produces {@link Tick}s: picks a symbol, takes its {@link PriceModel} quote at the given instant and,
 * with probability {@value #TRADE_PROBABILITY}, a trade of 1-10 lots at the bid or ask on a random lit
 * venue. Symbol choice and trades come from a seeded RNG, so a given seed and sequence of timestamps
 * always yields the same ticks; the quotes themselves only depend on (seed, symbol, time).
 */
public final class TickGenerator {

    static final double TRADE_PROBABILITY = 0.4;

    private final List<Instrument> instruments;
    private final List<Venue> litVenues;
    private final PriceModel prices;
    private final SplittableRandom rnd;
    private final Map<String, long[]> seqAndVolume = new HashMap<>();
    private final Map<String, BigDecimal> lastPx = new HashMap<>();

    public TickGenerator(List<Instrument> instruments, List<Venue> venues, PriceModel prices, long seed) {
        if (instruments.isEmpty()) {
            throw new IllegalArgumentException("no instruments");
        }
        this.instruments = List.copyOf(instruments);
        this.litVenues = venues.stream().filter(v -> v.getType() == VenueType.LIT).toList();
        this.prices = prices;
        this.rnd = new SplittableRandom(seed ^ 0x5DEECE66DL);
    }

    public Tick next(long tsMillis) {
        Instrument i = instruments.get(rnd.nextInt(instruments.size()));
        String symbol = i.getSymbol();
        Quote q = prices.quote(i, tsMillis);
        long[] sv = seqAndVolume.computeIfAbsent(symbol, s -> new long[2]);
        sv[0]++;
        long lastSize = 0;
        String venue = null;
        if (rnd.nextDouble() < TRADE_PROBABILITY) {
            lastSize = (1 + rnd.nextInt(10)) * i.getLotSize();
            lastPx.put(symbol, rnd.nextBoolean() ? q.getBid() : q.getAsk());
            venue = litVenues.get(rnd.nextInt(litVenues.size())).getMic();
            sv[1] += lastSize;
        }
        return new Tick().setSymbol(symbol).setTs(q.getTs()).setTsMillis(tsMillis).setSeq(sv[0])
                .setBid(q.getBid()).setBidSize(q.getBidSize()).setAsk(q.getAsk()).setAskSize(q.getAskSize()).setMid(q.getMid())
                .setLast(lastPx.getOrDefault(symbol, q.getMid())).setLastSize(lastSize).setVolume(sv[1]).setTradeVenue(venue);
    }
}
