package io.concert.marketdata;

import io.concert.trading.marketdata.Bar;
import io.concert.trading.marketdata.Tick;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Rolls ticks into 1-minute bars per symbol; a bar closes when a tick of a later minute arrives. */
public final class BarAggregator {

    private static final long MINUTE = 60_000L;

    /** Running state of one symbol's current minute. */
    private static final class Open {
        final long minute;
        BigDecimal open;
        BigDecimal high;
        BigDecimal low;
        BigDecimal close;
        long volume;
        BigDecimal notional = BigDecimal.ZERO;
        long trades;
        long ticks;
        boolean traded;

        Open(long minute) {
            this.minute = minute;
        }
    }

    private final Map<String, Open> open = new LinkedHashMap<>();

    /** Adds a tick; returns the bars (of any symbol) whose minute ended before this tick's minute. */
    public List<Bar> onTick(Tick t) {
        long minute = Math.floorDiv(t.getTsMillis(), MINUTE);
        List<Bar> closed = closeBefore(minute);
        Open o = open.computeIfAbsent(t.getSymbol(), s -> new Open(minute));
        o.ticks++;
        boolean trade = t.getLastSize() > 0;
        BigDecimal px = trade ? t.getLast() : t.getMid();
        if (trade && !o.traded) {
            // First trade of the minute: restart OHLC from trades only.
            o.traded = true;
            o.open = o.high = o.low = null;
        }
        if (trade || !o.traded) {
            if (o.open == null) {
                o.open = o.high = o.low = px;
            }
            o.high = o.high.max(px);
            o.low = o.low.min(px);
            o.close = px;
        }
        if (trade) {
            o.volume += t.getLastSize();
            o.notional = o.notional.add(px.multiply(BigDecimal.valueOf(t.getLastSize())));
            o.trades++;
        }
        return closed;
    }

    /** Closes and returns every open bar whose minute is before {@code minute}. */
    public List<Bar> closeBefore(long minute) {
        List<Bar> out = new ArrayList<>();
        var it = open.entrySet().iterator();
        while (it.hasNext()) {
            var e = it.next();
            Open o = e.getValue();
            if (o.minute < minute) {
                out.add(new Bar().setSymbol(e.getKey()).setStart(Instant.ofEpochMilli(o.minute * MINUTE))
                        .setEnd(Instant.ofEpochMilli((o.minute + 1) * MINUTE)).setOpen(o.open).setHigh(o.high).setLow(o.low)
                        .setClose(o.close).setVolume(o.volume)
                        .setVwap(o.volume == 0 ? null : o.notional.divide(BigDecimal.valueOf(o.volume), 6, RoundingMode.HALF_EVEN))
                        .setTrades(o.trades).setTicks(o.ticks));
                it.remove();
            }
        }
        return out;
    }
}
