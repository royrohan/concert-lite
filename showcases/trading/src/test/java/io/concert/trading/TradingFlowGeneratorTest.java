package io.concert.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.marketdata.MarketUniverse;
import io.concert.marketdata.PriceModel;
import io.concert.trading.marketdata.Quote;
import io.concert.model.runtime.ModelJson;
import io.concert.model.runtime.ModelObject;
import io.concert.trading.command.AllocateCommand;
import io.concert.trading.command.FillCommand;
import io.concert.trading.command.NewOrderCommand;
import io.concert.trading.common.Liquidity;
import io.concert.trading.common.Side;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Generator output is valid against TradingFlows and internally consistent, without Kinesis. */
class TradingFlowGeneratorTest {

    private static final long START = Instant.parse("2026-10-05T14:30:00Z").toEpochMilli();
    private static final TradingFlowGenerator.Config CONFIG = new TradingFlowGenerator.Config(
            7, PriceModel.DEFAULT_VOL_MULTIPLIER, "t", 600, "all", "all", 25, START, 200, 0.05, 0.15, 0.4);

    private static List<EventEnvelope> events;
    private static Map<String, List<EventEnvelope>> byEntity;

    @BeforeAll
    static void generate() {
        events = generate(CONFIG);
        byEntity = new LinkedHashMap<>();
        events.forEach(e -> byEntity.computeIfAbsent(e.entityKey(), k -> new ArrayList<>()).add(e));
    }

    private static List<EventEnvelope> generate(TradingFlowGenerator.Config config) {
        List<EventEnvelope> out = new ArrayList<>();
        new TradingFlowGenerator(config).forEachRemaining(out::add);
        return out;
    }

    private static JsonNode payload(EventEnvelope e) {
        try {
            return Json.MAPPER.readTree(e.payload());
        } catch (java.io.IOException ex) {
            throw new AssertionError(ex);
        }
    }

    private static List<EventEnvelope> ofType(String smType, String... eventTypes) {
        Set<String> types = Set.of(eventTypes);
        return events.stream().filter(e -> e.smType().equals(smType) && types.contains(e.eventType())).toList();
    }

    @Test
    void deterministicForASeed() {
        assertEquals(events, generate(CONFIG));
        TradingFlowGenerator.Config other = new TradingFlowGenerator.Config(8, CONFIG.volMultiplier(), "t", 600, "all", "all",
                25, START, 200, 0.05, 0.15, 0.4);
        assertTrue(!events.equals(generate(other)));
    }

    @Test
    void runIdChangesTheOrderFlowButNotThePriceModel() {
        TradingFlowGenerator.Config rerun = new TradingFlowGenerator.Config(CONFIG.seed(), CONFIG.volMultiplier(), "u", 600, "all",
                "all", 25, START, 200, 0.05, 0.15, 0.4);
        // Same flow modulo the id prefix would mean the run id only renames; it must reshuffle the orders.
        List<String> a = events.stream().map(e -> e.smType() + "." + e.eventType() + " " + e.payload().replaceAll("\\bt-", "")).toList();
        List<String> b = generate(rerun).stream().map(e -> e.smType() + "." + e.eventType() + " " + e.payload().replaceAll("\\bu-", ""))
                .toList();
        assertTrue(!a.equals(b));
        assertEquals(TradingFlowGenerator.flowSeed(7, "t"), TradingFlowGenerator.flowSeed(7, "t"));
        assertTrue(TradingFlowGenerator.flowSeed(7, "t") != TradingFlowGenerator.flowSeed(7, "u"));
    }

    @Test
    void everyEntityReplaysThroughItsFlowToATerminalState() {
        assertEquals(600, byEntity.keySet().stream().filter(k -> k.startsWith(TradingFlows.ORDER + ":")).count());
        Map<String, Integer> finals = new HashMap<>();
        for (List<EventEnvelope> es : byEntity.values()) {
            TradingFlows.Flow flow = TradingFlows.flow(es.getFirst().smType());
            String state = flow.initialState();
            for (EventEnvelope e : es) {
                String from = state;
                state = flow.transition(state, e.eventType())
                        .orElseThrow(() -> new AssertionError("no " + e.eventType() + " from " + from + " for " + e.entityKey()))
                        .to();
            }
            assertTrue(flow.terminalStates().contains(state), es.getFirst().entityKey() + " ends in " + state);
            finals.merge(flow.smType() + "." + state, 1, Integer::sum);
        }
        // The mix exercises every terminal state the generator can reach.
        for (String s : List.of("trading_order.FILLED", "trading_order.CANCELLED", "trading_order.REJECTED",
                "trading_execution.FILLED", "trading_execution.CANCELLED", "trading_fill.BOOKED", "trading_allocation.CONFIRMED")) {
            assertTrue(finals.getOrDefault(s, 0) > 0, s + " in " + finals);
        }
    }

    @Test
    void eventsOfAnEntityShareAPartitionKeyAndAreInTimeOrder() {
        Set<String> ids = new HashSet<>();
        for (List<EventEnvelope> es : byEntity.values()) {
            assertEquals(1, es.stream().map(TradingEvents::partitionKey).distinct().count(), es.getFirst().entityKey());
            for (int i = 1; i < es.size(); i++) {
                assertTrue(es.get(i).sourceTsMillis() > es.get(i - 1).sourceTsMillis());
            }
        }
        events.forEach(e -> assertTrue(ids.add(e.eventId()), "duplicate " + e.eventId()));
        for (int i = 0; i < events.size(); i++) {
            assertEquals(START + (long) (i * 1000.0 / CONFIG.eventsPerSecond()), events.get(i).sourceTsMillis());
        }
    }

    @Test
    void quantitiesAddUp() {
        Map<String, Long> orderQty = new HashMap<>();
        ofType(TradingFlows.ORDER, "submit").forEach(e -> orderQty.put(e.instanceKey(), payload(e).get("quantity").asLong()));
        Map<String, Long> routed = sum(ofType(TradingFlows.EXECUTION, "route"), "orderId");
        Map<String, Long> filled = sum(ofType(TradingFlows.FILL, "book"), "orderId");
        Map<String, Long> execFilled = sum(ofType(TradingFlows.FILL, "book"), "execId");
        Map<String, Long> allocated = sum(ofType(TradingFlows.ALLOCATION, "allocate"), "orderId");
        Set<String> cancelled = new HashSet<>();
        ofType(TradingFlows.ORDER, "cancel").forEach(e -> cancelled.add(e.instanceKey()));
        Set<String> rejected = new HashSet<>();
        ofType(TradingFlows.ORDER, "reject").forEach(e -> rejected.add(e.instanceKey()));

        for (var o : orderQty.entrySet()) {
            String id = o.getKey();
            long qty = o.getValue();
            long f = filled.getOrDefault(id, 0L);
            if (rejected.contains(id)) {
                assertEquals(0, routed.getOrDefault(id, 0L));
                continue;
            }
            assertEquals(qty, routed.get(id), "routes cover the order " + id);
            assertTrue(f <= qty, "fills <= order qty " + id);
            assertEquals(cancelled.contains(id) ? f < qty : f == qty, true, "fully filled unless cancelled " + id);
            assertEquals(f, allocated.getOrDefault(id, 0L).longValue(), "allocations = filled " + id);
        }
        for (EventEnvelope r : ofType(TradingFlows.EXECUTION, "route")) {
            assertTrue(execFilled.getOrDefault(r.instanceKey(), 0L) <= payload(r).get("quantity").asLong());
        }
        // close reports what was allocated
        for (EventEnvelope c : ofType(TradingFlows.ORDER, "close")) {
            assertEquals(allocated.get(c.instanceKey()).longValue(), payload(c).get("allocatedQty").asLong());
        }
        // all quantities are whole lots
        events.stream().map(TradingFlowGeneratorTest::payload).filter(p -> p.has("quantity"))
                .forEach(p -> assertEquals(0, p.get("quantity").asLong() % 100));
    }

    private static Map<String, Long> sum(List<EventEnvelope> es, String groupField) {
        Map<String, Long> out = new HashMap<>();
        es.forEach(e -> out.merge(payload(e).get(groupField).asText(), payload(e).get("quantity").asLong(), Long::sum));
        return out;
    }

    /** Every payload binds back to its generated class, re-serializes identically and satisfies its multiplicities. */
    @Test
    void payloadsRoundTripThroughTheGeneratedClasses() throws Exception {
        for (EventEnvelope e : events) {
            String cls = TradingFlows.flow(e.smType()).payloadClass(e.eventType());
            Class<?> type = Class.forName("io.concert.trading.command." + cls);
            ModelObject cmd = (ModelObject) ModelJson.read(e.payload(), type);
            assertEquals(List.of(), cmd.validationErrors(), e.eventId());
            assertEquals(e.payload(), ModelJson.write(cmd), e.eventId());
        }
    }

    @Test
    void fillsTradeAtTheSimulatedQuotesAndAllocationsAtTheAveragePrice() {
        MarketUniverse universe = MarketUniverse.create(CONFIG.seed());
        PriceModel prices = new PriceModel(CONFIG.seed(), CONFIG.volMultiplier());
        Map<String, NewOrderCommand> submits = new HashMap<>();
        ofType(TradingFlows.ORDER, "submit").forEach(e -> submits.put(e.instanceKey(), ModelJson.read(e.payload(), NewOrderCommand.class)));
        Map<String, BigDecimal[]> notionalQty = new HashMap<>();
        for (EventEnvelope e : ofType(TradingFlows.FILL, "book")) {
            FillCommand f = ModelJson.read(e.payload(), FillCommand.class);
            assertEquals(e.sourceTsMillis(), f.getTs().toEpochMilli());
            Quote q = prices.quote(universe.instrument(f.getSymbol()), e.sourceTsMillis());
            BigDecimal limit = submits.get(f.getOrderId()).getLimitPrice();
            boolean buy = f.getSide() == Side.BUY;
            BigDecimal touch = (f.getLiquidity() == Liquidity.REMOVE) == buy ? q.getAsk() : q.getBid();
            BigDecimal expected = limit == null ? touch : buy ? touch.min(limit) : touch.max(limit);
            assertEquals(0, expected.compareTo(f.getPrice()), e.eventId());
            notionalQty.merge(f.getOrderId(), new BigDecimal[] {f.getPrice().multiply(BigDecimal.valueOf(f.getQuantity())),
                    BigDecimal.valueOf(f.getQuantity())}, (a, b) -> new BigDecimal[] {a[0].add(b[0]), a[1].add(b[1])});
        }
        for (EventEnvelope e : ofType(TradingFlows.ALLOCATION, "allocate")) {
            AllocateCommand a = ModelJson.read(e.payload(), AllocateCommand.class);
            BigDecimal[] nq = notionalQty.get(a.getOrderId());
            assertEquals(0, nq[0].divide(nq[1], 6, RoundingMode.HALF_EVEN).compareTo(a.getAvgPx()));
        }
    }

    @Test
    void splitIsAPartitionIntoPositiveParts() {
        SplittableRandom r = new SplittableRandom(1);
        for (int i = 0; i < 1_000; i++) {
            long total = 1 + r.nextInt(200);
            int parts = 1 + r.nextInt((int) Math.min(5, total));
            long[] s = TradingFlowGenerator.split(total, parts, r);
            assertEquals(parts, s.length);
            assertEquals(total, java.util.Arrays.stream(s).sum());
            assertTrue(java.util.Arrays.stream(s).allMatch(x -> x > 0));
        }
    }
}
