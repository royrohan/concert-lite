package io.concert.sdk.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventRouting;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.ProcessorStatus;
import io.concert.common.api.SmStateRow;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowExecutionDescription;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Future-dated events (durable timers, time skipping) and continue-as-new of the processor. */
class ScheduledEventTest {

    private EventFixture f;

    @BeforeEach
    void setUp() {
        f = new EventFixture(true, 3); // continue-as-new after 3 applied events
    }

    @AfterEach
    void tearDown() {
        f.close();
    }

    private ProcessorStatus status(String key) {
        return EventRouting.processor(f.client, Shop.SHOP, key).status();
    }

    private boolean lockRunning(String key) {
        try {
            WorkflowExecutionDescription d = f.client.newUntypedWorkflowStub(WorkflowIds.lock(key)).describe();
            return d.getWorkflowExecutionInfo().getStatus()
                    == io.temporal.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING;
        } catch (RuntimeException notFound) {
            return false;
        }
    }

    @Test
    void scheduledEventFiresAtItsTimeAndHoldsNoLocksBefore() {
        long at = f.env.currentTimeMillis() + Duration.ofHours(1).toMillis();
        f.send(Shop.event("sch-1", Shop.SHOP, new Shop.Count("x", 10, -1), "sch:1").withScheduledAt(at));
        f.awaitStatus("sch-1", "SCHEDULED");
        assertEquals(List.of("sch-1"), status("sch:1").scheduled().stream().map(ProcessorStatus.Item::eventId).toList());
        assertFalse(lockRunning("sch:1"), "a scheduled event takes no lock");

        // a due event on the same key is not held up by the scheduled one
        f.send(Shop.event("now-1", Shop.SHOP, new Shop.Count("x", 1, -1), "sch:1"));
        f.awaitStatus("now-1", "DONE");
        assertEquals(1, f.counterValue("sch:1"));

        f.env.sleep(Duration.ofMinutes(59));
        assertEquals("SCHEDULED", f.status("sch-1"));
        f.env.sleep(Duration.ofMinutes(2));
        f.awaitStatus("sch-1", "DONE");
        assertEquals(11, f.counterValue("sch:1"));
        assertEquals(List.of("now-1", "sch-1"), f.counter("sch:1").seen);
    }

    @Test
    void handlerEmitAtSchedulesTheChild() {
        long at = f.env.currentTimeMillis() + Duration.ofMinutes(30).toMillis();
        f.send(Shop.event("rem-1", Shop.SHOP, new Shop.Remind("r1", at), "counter:r1"));
        f.awaitStatus("rem-1", "DONE");
        f.awaitStatus("rem-1.1", "SCHEDULED");
        assertEquals(0, f.counterValue("counter:r1"));
        f.env.sleep(Duration.ofMinutes(31));
        f.awaitStatus("rem-1.1", "DONE");
        assertEquals(10, f.counterValue("counter:r1"));
        SmStateRow child = f.row("rem-1.1").orElseThrow();
        assertTrue(child.data().contains("\"parentEventId\":\"rem-1\""), child.data());
    }

    @Test
    void continueAsNewCarriesScheduledAndParkedEvents() throws Exception {
        String key = "can:1";
        String wf = WorkflowIds.processor(Shop.SHOP, key);
        long at = f.env.currentTimeMillis() + Duration.ofHours(2).toMillis();
        f.send(Shop.event("cs", Shop.SHOP, new Shop.Count("x", 100, -1), key).withScheduledAt(at));
        f.awaitStatus("cs", "SCHEDULED");
        Shop.POISONED.add("pn");
        f.send(Shop.event("pn", Shop.SHOP, new Shop.PoisonNb("pn"), key));
        f.awaitStatus("pn", "ERROR_NON_BLOCKING");
        String firstRun = f.client.newUntypedWorkflowStub(wf).describe().getExecution().getRunId();
        for (int i = 0; i < 3; i++) {
            f.send(Shop.event("cn-" + i, Shop.SHOP, new Shop.Count("x", 1, -1), key));
        }
        f.awaitStatus("cn-2", "DONE");
        EventFixture.await("continue-as-new", Duration.ofSeconds(20), () -> {
            List<HistoryEvent> events = f.client.fetchHistory(wf, firstRun).getEvents();
            return events.getLast().getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW;
        });
        f.replayProcessor(f.client.fetchHistory(wf, firstRun));

        ProcessorStatus s = status(key);
        assertEquals(List.of("cs"), s.scheduled().stream().map(ProcessorStatus.Item::eventId).toList());
        assertEquals(List.of("pn"), s.parked().stream().map(ProcessorStatus.Item::eventId).toList());

        Shop.POISONED.remove("pn");
        assertEquals(EventLifecycle.ERROR_NON_BLOCKING, EventRouting.processor(f.client, Shop.SHOP, key).retry("pn").status());
        f.awaitStatus("pn", "DONE");
        assertEquals(4, f.counterValue(key));

        f.env.sleep(Duration.ofHours(2).plusMinutes(1));
        f.awaitStatus("cs", "DONE");
        assertEquals(104, f.counterValue(key));
        assertNull(f.row("cs.1").orElse(null));
    }
}
