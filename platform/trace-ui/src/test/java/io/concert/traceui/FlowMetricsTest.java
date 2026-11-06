package io.concert.traceui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.TraceRow;
import io.concert.common.TraceRow.Stage;
import io.concert.sdk.MachineCatalog;
import io.concert.store.InMemoryStateStore;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Flow attribution of trace rows and Kinesis records, the flow catalog, and the windowed per-flow aggregation. */
class FlowMetricsTest {

    @Test
    void traceRowsAndRecordsMapToTheSameFlowIds() throws Exception {
        assertEquals("order", FlowMetrics.flowOf("order:o1"));
        assertEquals("ev:orders", FlowMetrics.flowOf("evproc:orders:o-17"));
        assertEquals("ev:orders", FlowMetrics.flowOf("evproc:orders"));
        assertNull(FlowMetrics.flowOf(null));
        var mapper = io.concert.common.Json.MAPPER;
        assertEquals("order", KinesisTail.flowOf(mapper.readTree("{\"smType\":\"order\",\"style\":\"entity\"}")));
        assertEquals("ev:orders", KinesisTail.flowOf(mapper.readTree("{\"smType\":\"orders\",\"style\":\"event\"}")));
        assertEquals("unknown", KinesisTail.flowOf(mapper.readTree("{}")));
    }

    @Test
    void catalogHasEntityFlowsPerMachineAndEventFlowsPerDomain() {
        Map<String, Object> eventTypes = Map.of("catalogs", List.of(Map.of("name", "order-events/orders", "events", List.of(
                Map.of("name", "OrderCreateEvent", "domain", "orders"), Map.of("name", "PaymentCaptureEvent", "domain", "orders")))));
        Map<String, FlowMetrics.Flow> flows = FlowMetrics.catalog(MachineCatalog.load(), eventTypes);
        assertEquals("sm-order", flows.get("order").taskQueue());
        assertEquals("entity", flows.get("payment").style());
        FlowMetrics.Flow ev = flows.get("ev:orders");
        assertEquals("order-events", ev.group());
        assertEquals(List.of("evt_order_create_event", "evt_payment_capture_event"), ev.snapshotTypes());
        assertEquals("ev-orders", ev.taskQueue());
    }

    @Test
    void windowCountsStagesAndTimingsPerFlow() {
        InMemoryStateStore store = new InMemoryStateStore();
        long t = System.currentTimeMillis() - 5_000;
        store.appendTrace(List.of(
                new TraceRow("e1", t, Stage.RECEIVED, "order:o1", null, "kinesisMs=40 dedupeMs=3 keys=[order:o1]"),
                new TraceRow("e1", t + 5, Stage.DONE, "order:o1", null, "accepted e2eMs=12 srcMs=55"),
                new TraceRow("e2", t + 6, Stage.RECEIVED, "order:o2", null, "kinesisMs=60 dedupeMs=4 keys=[order:o2]"),
                new TraceRow("e2", t + 9, Stage.FAILED, "order:o2", null, "boom"),
                new TraceRow("e3", t + 10, Stage.DONE, "evproc:orders:o9", null, "done attempts=1 e2eMs=30"),
                new TraceRow("old", t - 120_000, Stage.DONE, "order:o0", null, "accepted e2eMs=999"),
                new TraceRow("e1", t + 7, Stage.RELEASED, null, "order:o1", null)));
        FlowMetrics m = new FlowMetrics(store, null, null, null, null, "", "", new KinesisTail(null, "s"), Map.of(), null);
        FlowMetrics.Window w = m.window(60, null);
        FlowMetrics.Agg order = w.aggs().get("order");
        assertEquals(2, order.count(Stage.RECEIVED));
        assertEquals(1, order.count(Stage.DONE), "the row outside the window is not counted");
        assertEquals(1, order.errorCount());
        assertEquals(List.of(12L), order.e2e);
        assertTrue(order.kinesis.containsAll(List.of(40L, 60L)));
        assertEquals(1, order.errors.size());
        assertEquals(1, w.aggs().get("ev:orders").count(Stage.DONE));
        assertEquals(2, w.aggs().size(), "rows without a workflow id belong to no flow");
        assertEquals(1, m.window(60, "ev:orders").aggs().size());
    }

    @Test
    void percentileOfSmallSamples() {
        assertNull(FlowMetrics.percentile(List.of(), 0.5));
        assertEquals(3L, FlowMetrics.percentile(List.of(5L, 1L, 3L), 0.5));
        assertEquals(5L, FlowMetrics.percentile(List.of(5L, 1L, 3L), 0.95));
    }
}
