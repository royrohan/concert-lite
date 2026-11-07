package io.concert.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.TraceRow;
import io.concert.common.TraceRow.Stage;
import io.concert.sdk.MachineCatalog;
import io.concert.store.InMemoryStateStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

/** What the dashboard publishes to CloudWatch, how it reads the responses, and the Logs tab's filter patterns. */
class CloudWatchTest {

    @Test
    void samplesCountPerIntervalAndLifetimesOnceWhenTheyEnd() {
        io.concert.sdk.StateMachineSpec spec = MachineCatalog.all().get("order").spec();
        String initial = spec.initialState();
        var first = spec.edges().stream().filter(e -> e.from().equals(initial)).findFirst().orElseThrow();
        var last = spec.edges().stream().filter(e -> spec.isTerminal(e.to()) && !e.from().equals(initial)).findFirst().orElseThrow();
        InMemoryStateStore store = new InMemoryStateStore();
        long now = System.currentTimeMillis();
        store.appendTrace(List.of(
                // o1 started 2 min ago and finished 5 s ago: its lifetime is in this interval
                new TraceRow("a1", now - 120_000, Stage.TRANSITION, "order:o1", null, initial + " -> " + first.to() + " (" + first.eventType() + ")"),
                new TraceRow("a2", now - 5_000, Stage.TRANSITION, "order:o1", null, last.from() + " -> " + last.to() + " (" + last.eventType() + ")"),
                new TraceRow("a2", now - 4_990, Stage.DONE, "order:o1", null, "accepted e2eMs=20"),
                new TraceRow("b1", now - 4_000, Stage.RECEIVED, "order:o2", null, "kinesisMs=7"),
                new TraceRow("b1", now - 3_990, Stage.FAILED, "order:o2", null, "boom"),
                // o3 finished 90 s ago: already published by an earlier interval
                new TraceRow("c1", now - 200_000, Stage.TRANSITION, "order:o3", null, initial + " -> " + first.to() + " (" + first.eventType() + ")"),
                new TraceRow("c2", now - 90_000, Stage.TRANSITION, "order:o3", null, last.from() + " -> " + last.to() + " (" + last.eventType() + ")")));
        FlowMetrics m = new FlowMetrics(store, null, null, null, null, "", "", new KinesisTail(null, "s"), Map.of(), null);
        List<FlowMetrics.MachineSample> samples = m.samples(30, 900);
        assertEquals(1, samples.size());
        FlowMetrics.MachineSample s = samples.get(0);
        assertEquals("order", s.machine());
        assertEquals(1L, s.counts().get("EventsProcessed"));
        assertEquals(1L, s.counts().get("EventsFailed"));
        assertEquals(1L, s.counts().get("EventsDispatched"));
        assertEquals(1L, s.counts().get("Completions"));
        assertEquals(20L, s.millis().get("EventLatencyP99"));
        assertEquals(115_000L, s.millis().get("InstanceLifetimeP50"), "o1 only: o3's lifetime ended before the interval");
        assertNull(s.millis().get("LockWaitP95"), "nothing measured, nothing published");
    }

    @Test
    void readsGetMetricDataResponses() {
        String xml = """
            <GetMetricDataResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/"><GetMetricDataResult>
              <MetricDataResults><member><Id>m0</Id><Label>EventsProcessed</Label>
                <Timestamps><member>2026-11-07T15:00:00Z</member><member>2026-11-07T15:00:30Z</member></Timestamps>
                <Values><member>12.0</member><member>3.5</member></Values><StatusCode>Complete</StatusCode></member></MetricDataResults>
              <NextToken>abc</NextToken></GetMetricDataResult></GetMetricDataResponse>""";
        Element root = CloudWatchApi.parse(xml.getBytes(StandardCharsets.UTF_8));
        List<Element> results = CloudWatchApi.children(CloudWatchApi.first(root, "MetricDataResults"), "member");
        assertEquals(1, results.size());
        assertEquals("m0", CloudWatchApi.text(CloudWatchApi.first(results.get(0), "Id")));
        assertEquals(2, CloudWatchApi.children(CloudWatchApi.first(results.get(0), "Values"), "member").size());
        assertEquals("abc", CloudWatchApi.text(CloudWatchApi.first(root, "NextToken")));
        assertNull(CloudWatchApi.parse(new byte[0]));
    }

    @Test
    void filterPatternsFollowCloudWatchTextSemantics() {
        String line = "[worker] WARN io.concert.Worker - lock wait on order:o1";
        assertTrue(CloudWatchLogs.matches("", line));
        assertTrue(CloudWatchLogs.matches("WARN", line));
        assertFalse(CloudWatchLogs.matches("ERROR", line));
        assertFalse(CloudWatchLogs.matches("warn", line), "case-sensitive");
        assertTrue(CloudWatchLogs.matches("WARN order", line), "every term");
        assertTrue(CloudWatchLogs.matches("\"lock wait\"", line));
        assertFalse(CloudWatchLogs.matches("\"wait lock\"", line));
        assertTrue(CloudWatchLogs.matches("?ERROR ?WARN", line));
        assertFalse(CloudWatchLogs.matches("?ERROR ?FATAL", line));
        assertFalse(CloudWatchLogs.matches("WARN -order", line));
        assertTrue(CloudWatchLogs.matches("{ $.level = \"x\" }", line), "JSON patterns are CloudWatch's job");
    }
}
