package io.concert.marketdata;

import io.concert.model.runtime.ModelJson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MarketDataSimTest {

    record Rec(String topic, String key, String value) {}

    private static List<Rec> run(long seed) {
        List<Rec> out = new ArrayList<>();
        MarketDataSim sim = new MarketDataSim(new MarketDataSim.Config(seed, 4, "10", 5, 100, true),
                (t, k, v) -> out.add(new Rec(t, k, v)));
        long start = Instant.parse("2026-10-05T14:30:00Z").toEpochMilli();
        sim.publishReference(Instant.ofEpochMilli(start));
        sim.runVirtual(start, 6_500); // 65 s at 100 ticks/s
        return out;
    }

    @Test
    void reproducibleAndWellFormed() throws Exception {
        List<Rec> recs = run(9);
        assertEquals(recs, run(9));
        assertEquals(10, recs.stream().filter(r -> r.topic().equals(MdTopics.INSTRUMENTS)).count());
        assertEquals(5, recs.stream().filter(r -> r.topic().equals(MdTopics.ACCOUNTS)).count());
        assertEquals(6_500, recs.stream().filter(r -> r.topic().equals(MdTopics.TICKS)).count());
        assertEquals(10, recs.stream().filter(r -> r.topic().equals(MdTopics.BARS_1M)).count());
        for (Rec r : recs) {
            if (r.topic().equals(MdTopics.TICKS)) {
                JsonNode t = ModelJson.mapper().readTree(r.value());
                assertEquals(r.key(), t.get("symbol").asText());
                assertTrue(t.get("bid").isNumber() && t.get("bid").decimalValue().compareTo(t.get("ask").decimalValue()) < 0);
                assertEquals(t.get("tsMillis").asLong(), Instant.parse(t.get("ts").asText()).toEpochMilli());
            }
        }
    }
}
