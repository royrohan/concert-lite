package io.concert.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.orchestration.OrchestrationWorker;
import io.concert.orchestration.OverlapProbe;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Kinesis (LocalStack) -> Temporal -> Java workers -> Postgres (DSQL stand-in), all real. */
class EndToEndIT {

    @BeforeEach
    void resetProbe() {
        OverlapProbe.reset();
    }

    private static long ledgerCount(Harness h, String ledger) {
        String data = h.stateData("ledger:" + ledger);
        return data == null ? 0 : Json.read(data, JsonNode.class).get("count").asLong();
    }

    private static long ledgerOutOfOrder(Harness h, String ledger) {
        return Json.read(h.stateData("ledger:" + ledger), JsonNode.class).get("outOfOrder").asLong();
    }

    @Test
    void ordersEveryKeyAndDropsDuplicates() {
        try (Harness h = new Harness(2)) {
            h.startCoordinator();
            h.startWorker("ledger");
            h.startIngest();

            int ledgers = 20, perLedger = 50;
            List<EventEnvelope> events = new ArrayList<>();
            for (int s = 0; s < perLedger; s++) {
                for (int l = 0; l < ledgers; l++) {
                    events.add(Harness.event("ord-" + l + "-" + s, "ledger", "L" + l, "append", List.of(), "{\"seq\":" + s + "}"));
                }
            }
            // Producer retries: 100 events published twice.
            List<EventEnvelope> withDupes = new ArrayList<>(events);
            withDupes.addAll(events.subList(0, 100));
            h.publish(withDupes);

            Harness.await(Duration.ofSeconds(120), () -> {
                long total = 0;
                for (int l = 0; l < ledgers; l++) {
                    total += ledgerCount(h, "L" + l);
                }
                return total == (long) ledgers * perLedger;
            });
            for (int l = 0; l < ledgers; l++) {
                assertEquals(perLedger, ledgerCount(h, "L" + l), "ledger L" + l);
                assertEquals(0, ledgerOutOfOrder(h, "L" + l), "out of order on L" + l);
            }
            assertEquals(ledgers * perLedger, h.countProcessed("ord-"));
            Harness.await(Duration.ofSeconds(10), () -> h.countTrace("ord-", "DROPPED") == 100);
            System.out.println("[ordering] " + h.stats("ord-").format());
        }
    }

    @Test
    void multiKeyEventsAcrossTypesAreSerialized() {
        try (Harness h = new Harness(4)) {
            h.startCoordinator();
            h.startWorker("order");
            h.startWorker("payment");
            h.startIngest();

            Random rnd = new Random(7);
            List<EventEnvelope> events = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String account = "account:" + rnd.nextInt(5);
                // order created->paid while holding the account, then the payment authorized/captured
                events.add(Harness.event("mk-o-" + i, "order", "o" + i, "pay", List.of(account), null));
                events.add(Harness.event("mk-p-" + i, "payment", "p" + i, "authorize", List.of(account, "order:o" + i), null));
                events.add(Harness.event("mk-c-" + i, "payment", "p" + i, "capture", List.of(account), null));
            }
            h.publish(events);

            Harness.await(Duration.ofSeconds(180), () -> h.countTrace("mk-", "DONE") == events.size());
            assertEquals(0, OverlapProbe.violations(), "events sharing a key overlapped");
            for (int i = 0; i < 100; i++) {
                assertEquals("PAID", h.store.loadState("order:o" + i).orElseThrow().state());
                assertEquals("CAPTURED", h.store.loadState("payment:p" + i).orElseThrow().state(), "payment p" + i);
            }
            System.out.println("[multi-key] " + h.stats("mk-").format());
        }
    }

    @Test
    void survivesCoordinatorFailover() {
        try (Harness h = new Harness(2)) {
            OrchestrationWorker a = h.startCoordinator();
            h.startCoordinator();
            h.startWorker("ledger");
            h.startIngest();

            int total = 1500;
            List<EventEnvelope> events = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                events.add(Harness.event("fo-" + i, "ledger", "F" + (i % 10), "append", List.of(), "{\"seq\":" + i + "}"));
            }
            h.publish(events.subList(0, 500));
            Harness.await(Duration.ofSeconds(60), () -> h.countTrace("fo-", "DONE") >= 300);
            h.stopCoordinator(a); // shard consumers on A time out their heartbeat and resume on B
            h.publish(events.subList(500, total));

            Harness.await(Duration.ofSeconds(180), () -> {
                long sum = 0;
                for (int l = 0; l < 10; l++) {
                    sum += ledgerCount(h, "F" + l);
                }
                return sum == total;
            });
            for (int l = 0; l < 10; l++) {
                assertEquals(total / 10, ledgerCount(h, "F" + l), "no loss and no double-apply on F" + l);
                assertEquals(0, ledgerOutOfOrder(h, "F" + l));
            }
            assertNotNull(h.stateData("ledger:F0"));
            System.out.println("[failover] " + h.stats("fo-").format());
        }
    }

    @Test
    void eventStyleChainsAndKeyedStateThroughKinesis() {
        try (Harness h = new Harness(2)) {
            h.startCoordinator();
            h.startEventWorker();
            h.startIngest();

            int chains = 20, keys = 10, counts = 300;
            List<EventEnvelope> events = new ArrayList<>();
            for (int c = 0; c < chains; c++) {
                events.add(EventEnvelope.event("ev-s" + c, "demo", "Step", List.of("chain:c" + c),
                        "{\"chain\":\"c" + c + "\",\"n\":1,\"max\":3}", System.currentTimeMillis()));
            }
            for (int i = 0; i < counts; i++) {
                events.add(EventEnvelope.event("ev-n" + i, "demo", "Count", List.of("counter:k" + (i % keys)),
                        "{\"key\":\"k" + (i % keys) + "\",\"by\":1}", System.currentTimeMillis()));
            }
            List<EventEnvelope> withDupes = new ArrayList<>(events);
            withDupes.addAll(events.subList(0, 50)); // producer retries
            h.publish(withDupes);

            int total = chains * 3 + counts;
            Harness.await(Duration.ofSeconds(120), () -> h.countTrace("ev-", "DONE") >= total);
            for (int c = 0; c < chains; c++) {
                JsonNode chain = Json.read(h.stateData("state:chain:c" + c), JsonNode.class);
                assertEquals(3, chain.get("value").asLong(), "chain c" + c);
                List<String> seen = new ArrayList<>();
                chain.get("seen").forEach(n -> seen.add(n.asText()));
                assertEquals(List.of("ev-s" + c, "ev-s" + c + ".1", "ev-s" + c + ".1.1"), seen);
                assertEquals("DONE", h.store.loadState("event:ev-s" + c + ".1.1").orElseThrow().state());
            }
            long sum = 0;
            for (int k = 0; k < keys; k++) {
                JsonNode counter = Json.read(h.stateData("state:counter:k" + k), JsonNode.class);
                assertEquals(counts / keys, counter.get("value").asLong(), "no loss / no double apply on k" + k);
                assertEquals(counts / keys, counter.get("seen").size());
                sum += counter.get("value").asLong();
            }
            assertEquals(counts, sum);
            Harness.await(Duration.ofSeconds(10), () -> h.countTrace("ev-", "DROPPED") == 50);
            Harness.sleepQuietly(1000);
            assertEquals(total, h.countTrace("ev-", "DONE"), "every event DONE exactly once");
            System.out.println("[event-style] " + h.stats("ev-").format());
        }
    }
}
