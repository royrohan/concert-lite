package io.concert.sdk.events;

import io.concert.common.Env;
import io.concert.common.TaskQueues;
import io.concert.common.WorkerTuning;
import io.concert.common.api.EventProcessorWorkflow;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.store.StateStore;
import io.concert.store.TraceWriter;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.LoggerFactory;

/**
 * Starts the worker of one event-style handler application ("domain") on task queue {@code ev-<domain>}: the
 * processor workflow, the handler / persistence local activities, the enqueue activities and an
 * {@link EntitySnapshotPublisher} ({@link EntitySnapshotPublisher#fromEnv()} unless given).
 *
 * <pre>
 * EventWorkerBootstrap.start(client, store, "orders", new OrderCreateHandler(), new OrderAcceptedHandler());
 * </pre>
 *
 * Per-type policies (attempts, onError) come from the domain's {@link EventCatalog} entries, so register the
 * catalogs ({@link EventCatalogs#register}) before starting.
 */
public final class EventWorkerBootstrap {
    private EventWorkerBootstrap() {}

    /**
     * Optional settings.
     *
     * @param publisher snapshot publisher ({@code null}: {@link EntitySnapshotPublisher#fromEnv()})
     * @param policy handler policy for types without a catalog entry, and the base of those with one
     * @param continueAfter events per processor run before it continues as new
     */
    public record Options(EntitySnapshotPublisher publisher, ProcessorConfig.Policy policy, int localActivitySlots,
            int continueAfter) {

        public static Options defaults() {
            return new Options(null, ProcessorConfig.defaultPolicy(), Env.getInt("WORKER_LA_SLOTS", 2000),
                    ProcessorConfig.DEFAULT_CONTINUE_AFTER);
        }

        public Options withPublisher(EntitySnapshotPublisher p) {
            return new Options(p, policy, localActivitySlots, continueAfter);
        }

        public Options withPolicy(ProcessorConfig.Policy p) {
            return new Options(publisher, p, localActivitySlots, continueAfter);
        }

        public Options withContinueAfter(int n) {
            return new Options(publisher, policy, localActivitySlots, Math.max(1, n));
        }
    }

    public static WorkerFactory start(WorkflowClient client, StateStore store, String domain, EventHandler<?>... handlers) {
        HandlerRegistry registry = new HandlerRegistry(domain);
        for (EventHandler<?> h : handlers) {
            registry.add(h);
        }
        return start(client, store, registry, Options.defaults());
    }

    public static WorkerFactory start(WorkflowClient client, StateStore store, HandlerRegistry handlers, Options options) {
        WorkerFactory factory = WorkerFactory.newInstance(client, WorkerTuning.factoryOptions());
        register(factory, client, store, handlers, options);
        factory.start();
        return factory;
    }

    /** Registers the domain's worker on an existing factory (not started), e.g. to host several domains. */
    public static Worker register(WorkerFactory factory, WorkflowClient client, StateStore store, HandlerRegistry handlers,
            Options options) {
        Worker worker = factory.newWorker(TaskQueues.events(handlers.domain()), WorkerTuning.workerOptions(
                        Env.getInt("WORKER_WF_POLLERS_MAX", 64), Env.getInt("WORKER_ACTIVITY_POLLERS_MAX", 4))
                .setMaxConcurrentLocalActivityExecutionSize(options.localActivitySlots())
                .build());
        registerOn(worker, client, store, handlers, options);
        return worker;
    }

    /**
     * Registers the processor and activities on a worker polling {@code ev-<domain>} (tests create it with
     * {@code TestWorkflowEnvironment.newWorker}).
     */
    public static void registerOn(Worker worker, WorkflowClient client, StateStore store, HandlerRegistry handlers,
            Options options) {
        String domain = handlers.domain();
        if (!worker.getTaskQueue().equals(TaskQueues.events(domain))) {
            throw new IllegalArgumentException("worker polls " + worker.getTaskQueue() + ", not " + TaskQueues.events(domain));
        }
        ProcessorConfig config = ProcessorConfig.forDomain(domain, options.policy()).withContinueAfter(options.continueAfter());
        worker.registerWorkflowImplementationFactory(EventProcessorWorkflow.class, () -> new EventProcessorWorkflowImpl(config));
        TraceWriter traces = new TraceWriter(store);
        EntitySnapshotPublisher publisher = options.publisher() != null ? options.publisher() : EntitySnapshotPublisher.fromEnv();
        worker.registerActivitiesImplementations(
                new EventHandlerActivitiesImpl(store, traces, handlers),
                new EventEnqueueActivitiesImpl(client, traces),
                publisher);
        LoggerFactory.getLogger(EventWorkerBootstrap.class).info("event worker for domain {} on {}: handlers {}",
                domain, TaskQueues.events(domain), handlers.entries().stream().map(HandlerRegistry.Entry::eventType).toList());
    }
}
