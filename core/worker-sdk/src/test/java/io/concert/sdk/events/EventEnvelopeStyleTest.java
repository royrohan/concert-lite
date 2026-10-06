package io.concert.sdk.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The envelope's event-style fields and lock-key rule, and their wire compatibility. */
class EventEnvelopeStyleTest {

    @Test
    void oldRecordsAreEntityStyle() {
        EventEnvelope e = Json.read("""
                {"eventId":"e1","smType":"order","instanceKey":"7","eventType":"pay","lockKeys":["account:9"],
                 "payload":null,"sourceTsMillis":1,"ingestTsMillis":0}""", EventEnvelope.class);
        assertFalse(e.isEventStyle());
        assertEquals(EventEnvelope.STYLE_ENTITY, e.style());
        assertEquals(List.of("account:9", "order:7"), e.effectiveLockKeys());
        assertEquals("order:7", e.traceWorkflowId());
        assertEquals(e, Json.read(Json.write(e), EventEnvelope.class));
        assertFalse(Json.write(e).contains("scheduledAtMillis"), "defaults stay off the wire");
        assertFalse(Json.write(e).contains("parentEventId"));
    }

    @Test
    void eventStyleUsesDeclaredKeysOnly() {
        EventEnvelope e = Json.read("""
                {"eventId":"e2","smType":"orders","eventType":"OrderCreate","lockKeys":["order:7","account:9","order:7"],
                 "style":"event"}""", EventEnvelope.class);
        assertTrue(e.isEventStyle());
        assertEquals("e2", e.instanceKey(), "instanceKey defaults to the event id");
        assertEquals(List.of("account:9", "order:7"), e.effectiveLockKeys());
        assertEquals("account:9", e.processorKey());
        assertEquals("evproc:orders:account:9", e.traceWorkflowId());
        assertEquals(List.of("orders:e3"), EventEnvelope.event("e3", "orders", "X", List.of(), null, 0).effectiveLockKeys());
        EventEnvelope child = EventEnvelope.event("e2.1", "orders", "Y", List.of(), null, 0).withParent(e);
        assertEquals("e2", child.parentEventId());
        assertEquals("e2", child.causationRoot());
        assertEquals("e2", EventEnvelope.event("e2.1.1", "orders", "Z", List.of(), null, 0).withParent(child).causationRoot());
        assertNull(e.parentEventId());
        assertThrows(IllegalArgumentException.class, () -> new EventEnvelope("x", "d", "x", "t", List.of(), null, 0, 0,
                "bogus", 0, null, null));
    }

    @Test
    void rowNaming() {
        assertEquals("evt_order_create_event", EventRows.eventSmType("OrderCreateEvent"));
        assertEquals("st_http_session", EventRows.stateSmType("HTTPSession"));
        assertEquals("evt_reserve_stock", EventRows.eventSmType("reserve-stock"));
    }
}
