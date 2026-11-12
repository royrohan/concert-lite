package io.concert.eco.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.WorkflowIds;
import io.concert.common.api.TransitionResult;
import io.concert.sdk.MachineHarness;
import io.concert.store.InMemoryStateStore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** The loan machine's rules, applied as the worker applies them, on an in-memory store. */
class LoanMachineTest {

    private static final String BORROW = "{\"memberId\":\"M-1\",\"bookId\":\"B-1\",\"dueDate\":\"2026-12-01\"}";

    private final InMemoryStateStore store = new InMemoryStateStore();
    private final MachineHarness harness = new MachineHarness(store).register("loan", LoanMachine.class);
    private final AtomicInteger ids = new AtomicInteger();

    @Test
    void borrowThenReturnClosesTheLoan() {
        assertTrue(send("L-1", "borrow", BORROW).accepted());
        TransitionResult r = send("L-1", "return", "{\"condition\":\"good\"}");

        assertEquals("RETURNED", r.toState());
        JsonNode loan = data("L-1");
        assertEquals("B-1", loan.path("bookId").asText());
        assertEquals("good", loan.path("note").asText());
        assertFalse(loan.path("closedAt").isMissingNode());
    }

    @Test
    void aLoanCanBeRenewedTwice() {
        send("L-2", "borrow", BORROW);
        assertTrue(send("L-2", "renew", "{\"dueDate\":\"2026-12-15\"}").accepted());
        assertTrue(send("L-2", "renew", "{\"dueDate\":\"2026-12-29\"}").accepted());

        TransitionResult third = send("L-2", "renew", "{\"dueDate\":\"2027-01-12\"}");

        assertFalse(third.accepted());
        assertTrue(third.message().contains("at most 2 times"), third.message());
        assertEquals("2026-12-29", data("L-2").path("dueDate").asText());
    }

    @Test
    void aRenewalMustMoveTheDueDateForward() {
        send("L-3", "borrow", BORROW);

        TransitionResult r = send("L-3", "renew", "{\"dueDate\":\"2026-11-20\"}");

        assertFalse(r.accepted());
        assertEquals(0, data("L-3").path("renewals").asInt());
    }

    @Test
    void returningABookThatWasNeverBorrowedIsRejected() {
        assertFalse(send("L-4", "return", "{}").accepted());
    }

    private TransitionResult send(String loanId, String eventType, String payload) {
        EventEnvelope e = LibraryEvents.envelope("loan", eventType, loanId, payload);
        String eventId = "test-" + ids.incrementAndGet();
        EventEnvelope event = new EventEnvelope(eventId, e.smType(), e.instanceKey(), e.eventType(), e.lockKeys(), e.payload(), 0, 0);
        return harness.apply(event);
    }

    private JsonNode data(String loanId) {
        try {
            return Json.MAPPER.readTree(store.loadState(WorkflowIds.entity("loan", loanId)).orElseThrow().data());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
