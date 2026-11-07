package io.concert.ecosystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SampleGeneratorTest {

    @TempDir
    Path tmp;

    @Test
    void insuranceSamplesUseConsistentIdsAndSensibleValues() {
        Path repo = Path.of(System.getProperty("concert.repoRoot"));
        Ecosystem eco = Ecosystem.load(repo.resolve("examples/insurance"), "insurance", "io.concert.eco.insurance");
        SampleGenerator g = new SampleGenerator(eco);
        MachineDecl claim = eco.machines().stream().filter(m -> m.smType().equals("claim")).findFirst().orElseThrow();
        MachineDecl payout = eco.machines().stream().filter(m -> m.smType().equals("payout")).findFirst().orElseThrow();
        Map<String, JsonNode> s = g.samples(claim);
        assertEquals("POLICY-1001", s.get("file").path("policyId").asText()); // the policy machine's sample key
        assertEquals("POLICY-1001", s.get("approve").path("policyId").asText());
        assertEquals("USD", s.get("file").path("claimed").path("currency").asText()); // the Pure default
        assertEquals("FIRE", s.get("file").path("lossType").asText()); // first enum value
        assertEquals("2026-01-15", s.get("file").path("lossDate").asText());
        assertTrue(s.get("file").path("claimed").path("amount").isNumber());
        // ids agree across machines: the payout's claimId is the claim's sample key
        assertEquals(Ecosystem.sampleKey(claim), g.samples(payout).get("schedule").path("claimId").asText());
        assertEquals(s.get("pay").path("payeeAccount").asText(), g.samples(payout).get("schedule").path("payeeAccount").asText());

        JsonNode flow = g.happyPath(claim);
        assertEquals(List.of("file", "assess", "approve", "pay"), events(flow));
        assertEquals("NEW -file-> OPEN -assess-> ASSESSED -approve-> APPROVED -pay-> PAID", flow.path("description").asText());
        assertEquals(List.of("schedule", "disburse"), events(g.happyPath(payout)));
    }

    @Test
    void polymorphicCommandsCarryTheirTypeAndHappyPathsPreferSuccess() {
        Path repo = Path.of(System.getProperty("concert.repoRoot"));
        Ecosystem eco = Ecosystem.load(repo.resolve("examples/trading-gen"), "trading-gen", "io.concert.eco.trading_gen");
        SampleGenerator g = new SampleGenerator(eco);
        java.util.function.Function<String, MachineDecl> machine = t -> eco.machines().stream().filter(m -> m.smType().equals(t)).findFirst().orElseThrow();
        MachineDecl order = machine.apply("gen_trading_order");
        JsonNode submit = g.samples(order).get("submit");
        assertEquals("trading::command::NewOrderCommand", submit.path("@type").asText());
        assertEquals("GEN-TRADING-ORDER-1001", submit.path("orderId").asText());
        assertEquals("2026-01-15T10:00:00Z", submit.path("ts").asText());
        assertEquals(List.of("submit", "ack", "route", "complete_fill", "close"), events(g.happyPath(order)));
        MachineDecl execution = machine.apply("gen_trading_execution");
        assertEquals(List.of("route", "complete_fill"), events(g.happyPath(execution))); // FILLED over CANCELLED / REJECTED
        // the trading system only submits: the rest of its path is emitted by the chain (concert::sm.emits)
        JsonNode system = g.happyPath(machine.apply("gen_trading_system"));
        assertEquals(List.of("submit"), events(system));
        assertEquals(system.path("events").get(0).path("payload").path("orderId").asText(),
                system.path("events").get(0).path("instanceKey").asText(), "a trade is keyed by its orderId");
    }

    private static List<String> events(JsonNode flow) {
        List<String> out = new java.util.ArrayList<>();
        flow.path("events").forEach(e -> out.add(e.path("eventType").asText()));
        return out;
    }
}
