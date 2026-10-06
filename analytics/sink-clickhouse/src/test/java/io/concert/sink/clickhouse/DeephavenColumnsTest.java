package io.concert.sink.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The Deephaven script's market-data column file must match the trading Pure models. */
class DeephavenColumnsTest {

    static final Path FILE = Path.of(System.getProperty("concert.deephavenColumns", "../../infra/deephaven/app.d/marketdata_columns.json"));

    @Test
    void checkedInFileMatchesTheModels() throws IOException {
        String expected = DeephavenColumns.json(Fixtures.trading());
        if (Boolean.getBoolean("concert.regenerate")) {
            Files.writeString(FILE, expected);
        }
        assertEquals(expected, Files.readString(FILE),
                FILE + " is out of date: run ./gradlew :sink-clickhouse:test --tests '*DeephavenColumnsTest' -Dconcert.regenerate=true");
        assertTrue(expected.contains("[ \"TradeVenue\", \"tradeVenue\", \"string\" ]"), expected);
        assertTrue(expected.contains("[ \"Ts\", \"ts\", \"instant\" ]"), expected);
        assertTrue(expected.contains("[ \"ArrivalPx\", \"/model/arrivalPx\", \"double\" ]"), expected);
        assertTrue(expected.contains("\"trading_allocation\""), expected);
    }
}
