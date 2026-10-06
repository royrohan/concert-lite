package io.concert.marketdata;

import io.concert.trading.marketdata.Bar;
import io.concert.trading.marketdata.Tick;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class BarAggregatorTest {

    private static final long T0 = Instant.parse("2026-10-05T14:30:00Z").toEpochMilli();

    private static Tick tick(long ts, String mid, String last, long lastSize) {
        BigDecimal m = new BigDecimal(mid);
        return new Tick().setSymbol("X").setTs(Instant.ofEpochMilli(ts)).setTsMillis(ts).setSeq(1L).setBid(m).setBidSize(100L)
                .setAsk(m).setAskSize(100L).setMid(m).setLast(new BigDecimal(last)).setLastSize(lastSize).setVolume(0L)
                .setTradeVenue(lastSize > 0 ? "XNAS" : null);
    }

    @Test
    void tradeBasedOhlcAndVwap() {
        BarAggregator agg = new BarAggregator();
        assertTrue(agg.onTick(tick(T0, "10.00", "10.00", 0)).isEmpty());
        agg.onTick(tick(T0 + 1_000, "10.05", "10.10", 100));
        agg.onTick(tick(T0 + 2_000, "10.05", "9.90", 300));
        agg.onTick(tick(T0 + 3_000, "10.20", "9.90", 0));
        List<Bar> bars = agg.onTick(tick(T0 + 60_000, "10.00", "10.00", 0));
        assertEquals(1, bars.size());
        Bar b = bars.getFirst();
        assertEquals(Instant.parse("2026-10-05T14:30:00Z"), b.getStart());
        assertEquals(new BigDecimal("10.10"), b.getOpen());
        assertEquals(new BigDecimal("10.10"), b.getHigh());
        assertEquals(new BigDecimal("9.90"), b.getLow());
        assertEquals(new BigDecimal("9.90"), b.getClose());
        assertEquals(400L, b.getVolume());
        assertEquals(new BigDecimal("9.950000"), b.getVwap());
        assertEquals(2L, b.getTrades());
        assertEquals(4L, b.getTicks());
    }

    @Test
    void quietMinuteUsesMids() {
        BarAggregator agg = new BarAggregator();
        agg.onTick(tick(T0, "10.00", "0", 0));
        agg.onTick(tick(T0 + 10, "10.30", "0", 0));
        Bar b = agg.closeBefore(Long.MAX_VALUE).getFirst();
        assertEquals(new BigDecimal("10.00"), b.getOpen());
        assertEquals(new BigDecimal("10.30"), b.getClose());
        assertNull(b.getVwap());
    }
}
