package io.concert.marketdata;

import io.concert.trading.refdata.Instrument;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class MarketUniverseTest {

    @Test
    void deterministicFromSeed() {
        assertEquals(MarketUniverse.create(5), MarketUniverse.create(5));
        assertTrue(!MarketUniverse.create(5).instruments().equals(MarketUniverse.create(6).instruments()));
    }

    @Test
    void aboutFiftySymbolsAcrossAllSectors() {
        MarketUniverse u = MarketUniverse.create(42);
        assertTrue(u.instruments().size() >= 50);
        assertEquals(u.instruments().size(), new HashSet<>(u.symbols()).size());
        assertEquals(io.concert.trading.common.Sector.values().length, u.instruments().stream().map(Instrument::getSector).distinct().count());
        u.instruments().forEach(i -> assertEquals(MarketUniverse.isinCheckDigit(i.getIsin().substring(0, 11)),
                i.getIsin().charAt(11) - '0', i.getIsin()));
    }

    @Test
    void isinCheckDigitMatchesKnownIsins() {
        assertEquals(5, MarketUniverse.isinCheckDigit("US037833100"));
        assertEquals(5, MarketUniverse.isinCheckDigit("US594918104"));
    }

    @Test
    void selectionSpecs() {
        MarketUniverse u = MarketUniverse.create(42);
        assertEquals(u.instruments(), u.selectInstruments("all"));
        assertEquals(5, u.selectInstruments("5").size());
        assertEquals(List.of("MSFT", "AAPL"), u.selectInstruments("MSFT, AAPL").stream().map(Instrument::getSymbol).toList());
        assertThrows(IllegalArgumentException.class, () -> u.selectInstruments("NOPE"));
        assertEquals(3, u.selectAccounts("3").size());
    }
}
