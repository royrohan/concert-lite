package io.concert.sdk.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.concert.common.api.SmStateRow;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sdk.StateMachineSpec;
import io.concert.sink.EntitySnapshot;
import io.concert.store.InMemoryStateStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** {@link Reconciler} with an in-memory store, a fake sink and a capturing publisher. */
class ReconcilerTest {

    /** CLOSED is terminal; PARKED is not. */
    static final StateMachineSpec SPEC = StateMachineSpec.startingAt("OPEN")
            .on("OPEN", "close", "CLOSED")
            .on("OPEN", "park", "PARKED")
            .on("PARKED", "resume", "OPEN")
            .build();

    static final class FakeSink implements SinkVersions {
        final Map<String, Long> rows = new HashMap<>();
        final List<Integer> requestSizes = new ArrayList<>();
        boolean root = true;
        Runnable onRequest = () -> {};

        @Override
        public Optional<Map<String, Long>> versions(String smType, List<String> entityIds) {
            requestSizes.add(entityIds.size());
            onRequest.run();
            if (!root) {
                return Optional.empty();
            }
            Map<String, Long> out = new HashMap<>();
            entityIds.forEach(id -> {
                if (rows.containsKey(id)) {
                    out.put(id, rows.get(id));
                }
            });
            return Optional.of(out);
        }
    }

    static final class Capture implements EntitySnapshotPublisher {
        final List<EntitySnapshot> published = new CopyOnWriteArrayList<>();

        @Override
        public void publish(EntitySnapshot snapshot) {
            published.add(snapshot);
        }
    }

    final InMemoryStateStore store = new InMemoryStateStore();
    final FakeSink sink = new FakeSink();
    final Capture publisher = new Capture();

    void put(String key, String state, long version) {
        store.upsertState(new SmStateRow("ticket:" + key, "ticket", state, "{\"id\":\"" + key + "\",\"v\":" + version + "}",
                version, "e-" + key, 1_759_600_000_000L + version), null);
    }

    ReconcileReport run(boolean dryRun, int chunk) {
        return new Reconciler(store, SPEC, sink, publisher).run(new ReconcileRequest("ticket", dryRun, chunk), r -> {});
    }

    @Test
    void republishesMissingAndStaleTerminalEntitiesOnly() {
        put("ok", "CLOSED", 3);
        sink.rows.put("ticket:ok", 3L);
        put("missing", "CLOSED", 2);
        put("stale", "CLOSED", 5);
        sink.rows.put("ticket:stale", 4L);
        put("ahead", "CLOSED", 2);
        sink.rows.put("ticket:ahead", 9L);
        put("open", "OPEN", 1);       // not terminal: never published, never counted
        put("parked", "PARKED", 2);
        store.upsertState(new SmStateRow("other:x", "other", "CLOSED", "{}", 1, "e", 1), null); // other smType

        ReconcileReport r = run(false, 500);
        assertEquals(6, r.scanned());
        assertEquals(4, r.terminal());
        assertEquals(1, r.missing());
        assertEquals(1, r.stale());
        assertEquals(1, r.ahead());
        assertEquals(2, r.republished());
        assertNull(r.skipped());
        Map<String, EntitySnapshot> byId = new HashMap<>();
        publisher.published.forEach(s -> byId.put(s.entityId(), s));
        assertEquals(2, byId.size());
        EntitySnapshot stale = byId.get("ticket:stale");
        assertEquals(5, stale.version(), "the store's current version, never the sink's");
        assertEquals("CLOSED", stale.state());
        assertEquals("ticket", stale.smType());
        assertEquals("{\"id\":\"stale\",\"v\":5}", stale.modelJson());
        assertNotNull(stale.completedAt());
        assertEquals(2, byId.get("ticket:missing").version());
    }

    @Test
    void dryRunCountsButPublishesNothing() {
        put("a", "CLOSED", 1);
        put("b", "CLOSED", 1);
        ReconcileReport r = run(true, 500);
        assertEquals(2, r.missing());
        assertEquals(0, r.republished());
        assertTrue(r.dryRun());
        assertTrue(publisher.published.isEmpty());
    }

    @Test
    void comparesInChunks() {
        for (int i = 0; i < 25; i++) {
            put("t" + i, "CLOSED", 1);
        }
        ReconcileReport r = run(false, 10);
        assertEquals(List.of(10, 10, 5), sink.requestSizes);
        assertEquals(25, r.republished());
    }

    @Test
    void neverPublishesAnEntityThatLeftTheTerminalStateOrAnOlderVersion() {
        put("moved", "CLOSED", 3);
        // The entity changes between the scan and the publish (a declared terminal state it left again):
        // the re-read sees it non-terminal at a newer version.
        sink.onRequest = () -> put("moved", "OPEN", 4);
        ReconcileReport r = new Reconciler(store, SPEC, sink, publisher).run(new ReconcileRequest("ticket", false, 0), x -> {});
        assertEquals(1, r.missing());
        assertEquals(1, r.changed());
        assertEquals(0, r.republished());
        assertTrue(publisher.published.isEmpty());
    }

    @Test
    void skipsTypesTheSinkDoesNotStore() {
        put("a", "CLOSED", 1);
        sink.root = false;
        ReconcileReport r = run(false, 500);
        assertNotNull(r.skipped());
        assertTrue(publisher.published.isEmpty());
    }
}
