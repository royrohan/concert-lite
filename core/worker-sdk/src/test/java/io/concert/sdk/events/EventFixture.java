package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import io.concert.common.Json;
import io.concert.common.TaskQueues;
import io.concert.common.api.SmStateRow;
import io.concert.orchestration.DispatchActivitiesImpl;
import io.concert.orchestration.IngestDispatcher;
import io.concert.orchestration.KeyLockWorkflowImpl;
import io.concert.sdk.EntityPersistenceActivitiesImpl;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sdk.ModelStateMachineTest.TicketMachine;
import io.concert.sink.EntitySnapshot;
import io.concert.store.InMemoryStateStore;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/**
 * The whole event-style pipeline on Temporal's in-memory test server: ingest dispatcher, lock chain
 * (orchestration), processors and handlers of the {@link Shop} domains, the ticket entity machines.
 */
final class EventFixture implements AutoCloseable {

    /** Captures snapshots. */
    static final class CapturingPublisher implements EntitySnapshotPublisher {
        final List<EntitySnapshot> published = new CopyOnWriteArrayList<>();

        @Override
        public void publish(EntitySnapshot snapshot) {
            published.add(snapshot);
        }

        List<EntitySnapshot> of(String entityId) {
            return published.stream().filter(s -> s.entityId().equals(entityId)).toList();
        }
    }

    static final ProcessorConfig.Policy FAST = new ProcessorConfig.Policy(OnError.BLOCKING, 3, Duration.ofMillis(10),
            Duration.ofMillis(50), Duration.ofSeconds(5));

    final TestWorkflowEnvironment env;
    final WorkflowClient client;
    final InMemoryStateStore store = new InMemoryStateStore();
    final TraceWriter traces;
    final CapturingPublisher publisher = new CapturingPublisher();
    final IngestDispatcher dispatcher;
    private final int continueAfter;

    EventFixture(boolean timeSkipping, int continueAfter) {
        this.continueAfter = continueAfter;
        System.setProperty("TEMPORAL_SEARCH_ATTRIBUTES", "false");
        Shop.registerCatalog();
        Shop.reset();
        env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setUseTimeskipping(timeSkipping).build());
        client = env.getWorkflowClient();
        traces = new TraceWriter(store, 100_000, 500, 10);
        DispatchActivitiesImpl dispatch = new DispatchActivitiesImpl(client, traces);

        Worker orch = env.newWorker(TaskQueues.ORCHESTRATION);
        orch.registerWorkflowImplementationTypes(KeyLockWorkflowImpl.class);
        orch.registerActivitiesImplementations(dispatch);

        EventWorkerBootstrap.Options options = EventWorkerBootstrap.Options.defaults()
                .withPublisher(publisher).withPolicy(FAST).withContinueAfter(continueAfter);
        EventWorkerBootstrap.registerOn(env.newWorker(TaskQueues.events(Shop.SHOP)), client, store, Shop.shopHandlers(), options);
        EventWorkerBootstrap.registerOn(env.newWorker(TaskQueues.events(Shop.STOCK)), client, store, Shop.stockHandlers(), options);

        Worker ticket = env.newWorker(TaskQueues.stateMachine("ticket"));
        ticket.registerWorkflowImplementationTypes(TicketMachine.class);
        ticket.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store), EntitySnapshotPublisher.logging());
        Worker eticket = env.newWorker(TaskQueues.stateMachine("eticket"));
        eticket.registerWorkflowImplementationTypes(Shop.EmittingTicketMachine.class);
        eticket.registerActivitiesImplementations(new EntityPersistenceActivitiesImpl(store), EntitySnapshotPublisher.logging(),
                new EventEnqueueActivitiesImpl(client, traces));

        env.start();
        dispatcher = new IngestDispatcher(store, traces, dispatch, Set.of(), 64);
    }

    /** Replays a processor history with the same processor config as the live workers (it decides CAN points). */
    void replayProcessor(io.temporal.common.WorkflowExecutionHistory history) throws Exception {
        ProcessorConfig config = ProcessorConfig.forDomain(Shop.SHOP, FAST).withContinueAfter(continueAfter);
        try (TestWorkflowEnvironment replayEnv = TestWorkflowEnvironment.newInstance()) {
            Worker w = replayEnv.newWorker("replay");
            w.registerWorkflowImplementationFactory(io.concert.common.api.EventProcessorWorkflow.class,
                    () -> new EventProcessorWorkflowImpl(config));
            io.temporal.testing.WorkflowReplayer.replayWorkflowExecution(history, w);
        }
    }

    void send(EventEnvelope... events) {
        dispatcher.dispatchBatch(List.of(events));
    }

    Optional<SmStateRow> row(String eventId) {
        return store.loadState(EventRows.eventRowId(eventId));
    }

    String status(String eventId) {
        return row(eventId).map(SmStateRow::state).orElse(null);
    }

    void awaitStatus(String eventId, String status) {
        try {
            await(eventId + " " + status, Duration.ofSeconds(30), () -> status.equals(status(eventId)));
        } catch (AssertionError e) {
            throw new AssertionError(e.getMessage() + "\n" + dump(), e);
        }
    }

    /** States and traces, for failure messages. */
    String dump() {
        StringBuilder sb = new StringBuilder("--- states\n");
        store.scanStates("").forEach(r -> sb.append(r.workflowId()).append(' ').append(r.state()).append(" v")
                .append(r.version()).append(' ').append(r.data()).append('\n'));
        sb.append("--- traces\n");
        store.recentEvents(0);
        for (String prefix : List.of("")) {
            for (io.concert.common.TraceRow.Stage st : io.concert.common.TraceRow.Stage.values()) {
                store.scanTraces(prefix, st).forEach(t -> sb.append(t).append('\n'));
            }
        }
        return sb.toString();
    }

    Shop.Counter counter(String key) {
        return store.loadState(EventRows.stateRowId(key)).map(r -> Json.read(r.data(), Shop.Counter.class)).orElse(null);
    }

    long counterValue(String key) {
        Shop.Counter c = counter(key);
        return c == null ? 0 : c.value;
    }

    <T> T state(String key, Class<T> type) {
        return store.loadState(EventRows.stateRowId(key)).map(r -> Json.read(r.data(), type)).orElse(null);
    }

    static void await(String what, Duration timeout, BooleanSupplier cond) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (cond.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException notYet) {
                // keep polling
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted");
            }
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        dispatcher.close();
        traces.close();
        env.close();
    }
}
