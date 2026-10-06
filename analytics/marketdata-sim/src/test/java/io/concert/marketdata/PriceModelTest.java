package io.concert.marketdata;

import io.concert.trading.marketdata.Quote;
import io.concert.trading.refdata.Instrument;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class PriceModelTest {

    private static final long T0 = Instant.parse("2026-10-05T14:30:00Z").toEpochMilli();
    private final MarketUniverse universe = MarketUniverse.create(42);

    @Test
    void sameSeedSameQuotesInAnyEvaluationOrder() {
        PriceModel a = new PriceModel(42);
        PriceModel b = new PriceModel(42);
        Instrument aapl = universe.instrument("AAPL");
        // b evaluates other points first: the path must not depend on what was computed before.
        for (int i = 0; i < 500; i++) {
            b.quote(aapl, T0 + 7_919L * i);
        }
        for (int i = 0; i < 200; i++) {
            long ts = T0 + 1_234L * i;
            assertEquals(a.quote(aapl, ts), b.quote(aapl, ts));
        }
    }

    @Test
    void differentSeedsGiveDifferentPaths() {
        Instrument msft = universe.instrument("MSFT");
        assertNotEquals(new PriceModel(1).fairPrice(msft, T0), new PriceModel(2).fairPrice(msft, T0));
    }

    @Test
    void quotesRespectTickSizeAndBidBelowAsk() {
        PriceModel model = new PriceModel(7);
        SplittableRandom rnd = new SplittableRandom(1);
        for (Instrument i : universe.instruments()) {
            for (int k = 0; k < 200; k++) {
                long ts = T0 + rnd.nextLong(-40L * 86_400_000L, 40L * 86_400_000L);
                Quote q = model.quote(i, ts);
                assertTrue(q.getBid().signum() > 0, () -> "bid > 0 " + q);
                assertTrue(q.getBid().compareTo(q.getAsk()) < 0, () -> "bid < ask " + q);
                assertOnTick(q.getBid(), i.getTickSize());
                assertOnTick(q.getAsk(), i.getTickSize());
                assertEquals(0, q.getBidSize() % i.getLotSize());
                assertEquals(0, q.getAskSize() % i.getLotSize());
                assertTrue(q.getMid().compareTo(q.getBid()) > 0 && q.getMid().compareTo(q.getAsk()) < 0);
            }
        }
    }

    @Test
    void midStaysPositiveAndNearReferenceEvenWithHighVolatility() {
        PriceModel wild = new PriceModel(3, 20);
        for (Instrument i : universe.instruments()) {
            double ref = i.getRefPrice().doubleValue();
            for (int day = -400; day <= 400; day += 13) {
                double px = wild.fairPrice(i, T0 + day * 86_400_000L);
                assertTrue(px > 0, i.getSymbol());
                // Mean reversion bounds the drift for any date (no compounding since an epoch).
                assertTrue(px > ref / 100 && px < ref * 100, () -> i.getSymbol() + " " + px + " vs " + ref);
            }
        }
    }

    @Test
    void pathIsContinuousWithinADay() {
        PriceModel model = new PriceModel(42);
        Instrument nvda = universe.instrument("NVDA");
        double prev = model.fairPrice(nvda, T0);
        for (int s = 1; s < 3_000; s++) {
            double px = model.fairPrice(nvda, T0 + s * PriceModel.STEP_MILLIS);
            assertTrue(Math.abs(Math.log(px / prev)) < 0.01, "100 ms moves stay small");
            prev = px;
        }
    }

    @Test
    void realizedVolatilityMatchesTheModel() {
        PriceModel model = new PriceModel(11, 1);
        Instrument spy = universe.instrument("SPY");
        int n = 20_000;
        long stepMs = 1_000;
        double sumSq = 0;
        double prev = Math.log(model.fairPrice(spy, T0));
        for (int s = 1; s <= n; s++) {
            double x = Math.log(model.fairPrice(spy, T0 + s * stepMs));
            sumSq += (x - prev) * (x - prev);
            prev = x;
        }
        double annualized = Math.sqrt(sumSq / n * (365.25 * 86_400_000.0 / stepMs));
        double expected = model.annualVol(spy);
        assertEquals(expected, annualized, expected * 0.1);
    }

    private static void assertOnTick(BigDecimal px, BigDecimal tick) {
        assertEquals(0, px.remainder(tick).signum(), () -> px + " not a multiple of " + tick);
    }
}
