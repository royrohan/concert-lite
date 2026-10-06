package io.concert.sdk.events;

import io.concert.common.EventEnvelope;
import io.concert.common.TraceRow;
import io.concert.common.WorkflowIds;
import io.concert.common.api.EventLifecycle;
import io.concert.common.api.EventOutcome;
import io.concert.common.api.EventProcessorWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.ProcessRequest;
import io.concert.common.api.ProcessorState;
import io.concert.common.api.ProcessorState.Pending;
import io.concert.common.api.ProcessorStatus;
import io.concert.common.api.SmStateRow;
import io.concert.sdk.EntitySnapshotPublisher;
import io.concert.sink.EntitySnapshot;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

/**
 * The event processor: applies event-style events of one domain whose first lock key is {@code key}
 * ({@code evproc:<domain>:<key>}, task queue {@code ev-<domain>}).
 *
 * <ul>
 *   <li><b>Gate.</b> {@code process}, {@code retry} and {@code skip} are applied one at a time in update
 *       acceptance order (tickets, as in {@code AbstractStateMachine}). Every event that reaches the processor
 *       shares its first key, which its lock chain holds, so they cannot overlap anyway; the gate also orders
 *       operator calls.
 *   <li><b>Apply.</b> The handler runs in a local activity with the type's retry policy ({@link ProcessorConfig})
 *       and returns its writes and emits; exhaustion is classified by {@code onError} (default BLOCKING).
 *   <li><b>Persist.</b> A local activity writes the state documents (version-guarded, {@code lastEventId} =
 *       event id) and then the lifecycle row {@code event:<id>}. The handler activity checks that row first, so a
 *       re-run after a crash (or a duplicate delivery) never applies twice.
 *   <li><b>Emit.</b> Children (deterministic ids {@code <id>.<n>}) are handed to a regular activity that is
 *       scheduled in the same workflow task that completes the update, i.e. atomically with the outcome, and is
 *       not awaited (it may enqueue on the very lock that is waiting for this update).
 *   <li><b>Snapshots.</b> Lifecycle rows (every status change: SCHEDULED, NEW when due, DONE, ERROR_*) and saved state documents are published through
 *       {@link EntitySnapshotPublisher} (regular activity, unlimited retries), awaited before continue-as-new.
 *   <li><b>Blocked.</b> ERROR_BLOCKING is returned to the lock, which keeps the chain's keys. A successful
 *       operator {@code retry} (or a {@code skip}) sends {@code unblock(requestId)} to that lock through a
 *       regular activity.
 *   <li><b>Parked.</b> ERROR_NON_BLOCKING releases the keys; {@code retry} re-enters the lock chain with request
 *       id {@code <id>~r<n>}.
 *   <li><b>Scheduled.</b> Future-dated events wait on a durable timer here (holding no locks) and then join
 *       their lock chain as NEW.
 *   <li><b>Lifetime.</b> Completes after {@code idleSeconds} without work unless something is scheduled, blocked
 *       or parked; continues-as-new every {@link ProcessorConfig#continueAfter()} events carrying all of that.
 * </ul>
 */
public class EventProcessorWorkflowImpl implements EventProcessorWorkflow {

    static final int RECENT_LIMIT = 50;
    static final int DONE_WINDOW = 2000;

    private static final Logger log = Workflow.getLogger(EventProcessorWorkflowImpl.class);

    private final ProcessorConfig config;
    private final Map<String, EventHandlerActivities> handlerStubs = new HashMap<>();

    private final EventHandlerActivities persistence = Workflow.newLocalActivityStub(EventHandlerActivities.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .setScheduleToCloseTimeout(Duration.ofMinutes(10))
                    .build());

    /** Unlimited retries: a child event or an unblock must eventually get through. */
    private final EventEnqueueActivities enqueuer = Workflow.newActivityStub(EventEnqueueActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofMillis(100))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setMaximumAttempts(0)
                            .build())
                    .build());

    private final EntitySnapshotPublisher snapshots = Workflow.newActivityStub(EntitySnapshotPublisher.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2)
                            .setMaximumInterval(Duration.ofSeconds(60))
                            .setMaximumAttempts(0)
                            .build())
                    .build());

    private String domain;
    private String key;
    private int idleSeconds;
    private final List<EventEnvelope> scheduled = new ArrayList<>();
    private final LinkedHashMap<String, Pending> blocked = new LinkedHashMap<>();
    private final LinkedHashMap<String, Pending> parked = new LinkedHashMap<>();
    private final Deque<ProcessorStatus.Recent> recent = new ArrayDeque<>();
    private final LinkedHashSet<String> recentDone = new LinkedHashSet<>();
    private final List<Promise<Void>> pending = new ArrayList<>();
    private long lastVersion;
    private long processedThisRun;
    private boolean initialized;
    /** Set by every handler: the main loop re-evaluates timers and idleness. */
    private boolean dirty;
    private long ticketsIssued;
    private long ticketServed;

    public EventProcessorWorkflowImpl() {
        this(new ProcessorConfig(ProcessorConfig.defaultPolicy(), Map.of()));
    }

    public EventProcessorWorkflowImpl(ProcessorConfig config) {
        this.config = config;
    }

    @Override
    public void run(ProcessorState state) {
        domain = state.domain();
        key = state.key();
        idleSeconds = state.idleSeconds() > 0 ? state.idleSeconds() : 600;
        // Updates accepted before run() (same task as the start) may already have added entries.
        for (EventEnvelope e : state.scheduled()) {
            if (scheduled.stream().noneMatch(s -> s.eventId().equals(e.eventId()))) {
                scheduled.add(e);
            }
        }
        sortScheduled();
        state.blocked().forEach(p -> blocked.putIfAbsent(p.event().eventId(), p));
        state.parked().forEach(p -> parked.putIfAbsent(p.event().eventId(), p));
        state.recent().forEach(recent::addLast);
        state.recentDone().forEach(this::rememberDone);
        lastVersion = Math.max(lastVersion, state.lastVersion());
        initialized = true;

        while (true) {
            fireDue();
            pending.removeIf(Promise::isCompleted);
            if (processedThisRun >= config.continueAfter() || Workflow.getInfo().isContinueAsNewSuggested()) {
                quiesce();
                fireDue();
                quiesce();
                Workflow.continueAsNew(new ProcessorState(domain, key, idleSeconds, List.copyOf(scheduled),
                        List.copyOf(blocked.values()), List.copyOf(parked.values()), List.copyOf(recent),
                        List.copyOf(recentDone), lastVersion));
            }
            dirty = false;
            if (!scheduled.isEmpty()) {
                long wait = Math.max(1, scheduled.getFirst().scheduledAtMillis() - Workflow.currentTimeMillis());
                Workflow.await(Duration.ofMillis(wait), () -> dirty || Workflow.getInfo().isContinueAsNewSuggested());
            } else if (!blocked.isEmpty() || !parked.isEmpty()) {
                Workflow.await(() -> dirty || Workflow.getInfo().isContinueAsNewSuggested());
            } else if (!Workflow.await(Duration.ofSeconds(idleSeconds), () -> dirty)) {
                // Idle: finish outstanding work; an update racing with the completion is rejected and its
                // caller's UpdateWithStart retries against a fresh run.
                quiesce();
                if (!dirty && scheduled.isEmpty() && blocked.isEmpty() && parked.isEmpty()) {
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------------ updates

    @Override
    public void validateProcess(ProcessRequest request) {
        if (request == null || request.event() == null || request.requestId() == null) {
            throw new IllegalArgumentException("request, requestId and event are required");
        }
        checkTarget(request.event());
    }

    @Override
    public EventOutcome process(ProcessRequest request) {
        long ticket = ticketsIssued++;
        dirty = true;
        Workflow.await(() -> initialized && ticketServed == ticket);
        try {
            EventEnvelope e = request.event();
            String id = e.eventId();
            if (recentDone.contains(id)) {
                return outcome(id, EventLifecycle.DONE, "already done", 0, List.of());
            }
            Pending b = blocked.get(id);
            if (b != null) {
                // The same blocked request dispatched again (its lock's dispatch was retried): still blocked.
                return outcome(id, EventLifecycle.ERROR_BLOCKING, b.error(), b.attempts(), List.of());
            }
            Pending p = parked.get(id);
            if (p != null && !request.requestId().equals(p.requestId())) {
                // A stale delivery of a parked event: only the operator's retry request runs it again.
                return outcome(id, EventLifecycle.ERROR_NON_BLOCKING, p.error(), p.attempts(), List.of());
            }
            return apply(e, request.requestId(), request.lockKey(), p == null ? 0 : p.attempts(), p == null ? 0 : p.retries());
        } finally {
            ticketServed++;
            dirty = true;
        }
    }

    @Override
    public void validateSchedule(EventEnvelope event) {
        if (event == null) {
            throw new IllegalArgumentException("event is required");
        }
        checkTarget(event);
    }

    @Override
    public EventOutcome schedule(EventEnvelope e) {
        dirty = true;
        Workflow.await(() -> initialized);
        String id = e.eventId();
        if (recentDone.contains(id) || blocked.containsKey(id) || parked.containsKey(id)
                || scheduled.stream().anyMatch(s -> s.eventId().equals(id))) {
            return outcome(id, status(id), "already known", 0, List.of());
        }
        long now = Workflow.currentTimeMillis();
        if (e.scheduledAtMillis() <= now) {
            startEnqueue(EnqueueRequest.lock(LockRequest.first(e, now)));
            return outcome(id, EventLifecycle.NEW, "due", 0, List.of());
        }
        scheduled.add(e);
        sortScheduled();
        SmStateRow row = lifecycleRow(e, EventLifecycle.SCHEDULED, "scheduled", null, 0, 0, null, List.of(), now);
        persist(List.of(row), List.of(trace(e, now, TraceRow.Stage.SCHEDULED, "at " + Instant.ofEpochMilli(e.scheduledAtMillis()))));
        publish(row, e, now);
        dirty = true;
        return outcome(id, EventLifecycle.SCHEDULED, "at " + Instant.ofEpochMilli(e.scheduledAtMillis()), 0, List.of());
    }

    @Override
    public void validateRetry(String eventId) {
        if (initialized && !blocked.containsKey(eventId) && !parked.containsKey(eventId)) {
            throw new IllegalArgumentException("event " + eventId + " is neither blocked nor parked in " + workflowId());
        }
    }

    @Override
    public EventOutcome retry(String eventId) {
        long ticket = ticketsIssued++;
        dirty = true;
        Workflow.await(() -> initialized && ticketServed == ticket);
        try {
            Pending b = blocked.get(eventId);
            if (b != null) {
                // Its keys are still held by the blocked lock chain: run it right here.
                EventOutcome o = apply(b.event(), b.requestId(), b.lockKey(), b.attempts(), b.retries() + 1);
                if (!o.blocked()) {
                    startUnblock(b.lockKey(), b.requestId());
                }
                return o;
            }
            Pending p = parked.get(eventId);
            if (p != null) {
                // Keys were released: go through the lock chain again under a fresh request id.
                int n = p.retries() + 1;
                String requestId = eventId + "~r" + n;
                long now = Workflow.currentTimeMillis();
                parked.put(eventId, new Pending(p.event(), requestId, null, p.error(), p.attempts(), n, p.sinceMillis()));
                startEnqueue(EnqueueRequest.lock(LockRequest.retry(p.event(), requestId, now)));
                return outcome(eventId, EventLifecycle.ERROR_NON_BLOCKING, "retrying as " + requestId, p.attempts(), List.of());
            }
            throw ApplicationFailure.newNonRetryableFailure("event " + eventId + " is neither blocked nor parked", "NotPending");
        } finally {
            ticketServed++;
            dirty = true;
        }
    }

    @Override
    public void validateSkip(String eventId, String reason) {
        validateRetry(eventId);
    }

    @Override
    public EventOutcome skip(String eventId, String reason) {
        long ticket = ticketsIssued++;
        dirty = true;
        Workflow.await(() -> initialized && ticketServed == ticket);
        try {
            Pending b = blocked.remove(eventId);
            Pending p = b != null ? b : parked.remove(eventId);
            if (p == null) {
                throw ApplicationFailure.newNonRetryableFailure("event " + eventId + " is neither blocked nor parked", "NotPending");
            }
            long now = Workflow.currentTimeMillis();
            String detail = "skipped" + (reason == null || reason.isBlank() ? "" : ": " + reason);
            SmStateRow row = lifecycleRow(p.event(), EventLifecycle.DONE, "skipped", reason, p.attempts(), p.retries(),
                    p.requestId(), List.of(), now);
            persist(List.of(row), List.of(trace(p.event(), now, TraceRow.Stage.DONE, detail + doneTiming(p.event(), now))));
            publish(row, p.event(), now);
            rememberDone(eventId);
            remember(p.event(), EventLifecycle.DONE, detail, now);
            if (b != null) {
                startUnblock(b.lockKey(), b.requestId());
            }
            return outcome(eventId, EventLifecycle.DONE, detail, p.attempts(), List.of());
        } finally {
            ticketServed++;
            dirty = true;
        }
    }

    @Override
    public ProcessorStatus status() {
        List<ProcessorStatus.Item> s = scheduled.stream().map(e -> new ProcessorStatus.Item(
                e.eventId(), e.eventType(), e.scheduledAtMillis(), null, null, 0)).toList();
        List<ProcessorStatus.Item> b = blocked.values().stream().map(EventProcessorWorkflowImpl::item).toList();
        List<ProcessorStatus.Item> p = parked.values().stream().map(EventProcessorWorkflowImpl::item).toList();
        return new ProcessorStatus(domain, key, s, b, p, List.copyOf(recent), processedThisRun);
    }

    // ------------------------------------------------------------------ apply

    /** Runs the handler and applies its result; called inside the gate. */
    private EventOutcome apply(EventEnvelope e, String requestId, String lockKey, int priorAttempts, int retries) {
        String id = e.eventId();
        ProcessorConfig.Policy policy = config.policy(e.eventType());
        HandlerResult r;
        try {
            r = handler(policy).apply(e);
        } catch (ActivityFailure f) {
            HandlerResult.Status s = policy.onError() == OnError.NON_BLOCKING
                    ? HandlerResult.Status.NON_BLOCKING : HandlerResult.Status.BLOCKING;
            r = HandlerResult.failed(s, "retries exhausted (" + policy.maxAttempts() + "): " + message(f), policy.maxAttempts());
        }
        processedThisRun++;
        long now = Workflow.currentTimeMillis();
        int attempts = priorAttempts + r.attempt();
        switch (r.status()) {
            case ALREADY_DONE -> {
                blocked.remove(id);
                parked.remove(id);
                rememberDone(id);
                if (!r.emitted().isEmpty()) {
                    // Normally already enqueued; after a crash between persist and the outcome they were not.
                    startEnqueue(EnqueueRequest.children(id, r.emitted(), now));
                }
                return outcome(id, EventLifecycle.DONE, "already done", 0, ids(r.emitted()));
            }
            case OK -> {
                List<SmStateRow> rows = new ArrayList<>();
                for (StateWrite w : r.writes()) {
                    rows.add(new SmStateRow(EventRows.stateRowId(w.key()), EventRows.stateSmType(w.stateType()),
                            EventRows.STATE_CURRENT, w.data(), w.version(), id, now));
                }
                SmStateRow row = lifecycleRow(e, EventLifecycle.DONE, "ok", null, attempts, retries, requestId, r.emitted(), now);
                rows.add(row);
                persist(rows, List.of(trace(e, now, TraceRow.Stage.DONE, "done attempts=" + attempts
                        + (r.emitted().isEmpty() ? "" : " children=" + r.emitted().size()) + doneTiming(e, now))));
                if (!r.emitted().isEmpty()) {
                    startEnqueue(EnqueueRequest.children(id, r.emitted(), now));
                }
                for (SmStateRow s : rows) {
                    publish(s, e, now);
                }
                blocked.remove(id);
                parked.remove(id);
                rememberDone(id);
                remember(e, EventLifecycle.DONE, null, now);
                return outcome(id, EventLifecycle.DONE, null, attempts, ids(r.emitted()));
            }
            case BLOCKING -> {
                parked.remove(id);
                blocked.put(id, new Pending(e, requestId, lockKey, r.error(), attempts, retries, now));
                SmStateRow row = lifecycleRow(e, EventLifecycle.ERROR_BLOCKING, "error", r.error(), attempts, retries,
                        requestId, List.of(), now);
                persist(List.of(row), List.of(trace(e, now, TraceRow.Stage.BLOCKED, r.error())));
                publish(row, e, now);
                remember(e, EventLifecycle.ERROR_BLOCKING, r.error(), now);
                return outcome(id, EventLifecycle.ERROR_BLOCKING, r.error(), attempts, List.of());
            }
            default -> { // NON_BLOCKING, NO_HANDLER
                blocked.remove(id);
                parked.put(id, new Pending(e, null, null, r.error(), attempts, retries, now));
                SmStateRow row = lifecycleRow(e, EventLifecycle.ERROR_NON_BLOCKING, "error", r.error(), attempts, retries,
                        requestId, List.of(), now);
                persist(List.of(row), List.of(trace(e, now, TraceRow.Stage.PARKED, r.error())));
                publish(row, e, now);
                remember(e, EventLifecycle.ERROR_NON_BLOCKING, r.error(), now);
                return outcome(id, EventLifecycle.ERROR_NON_BLOCKING, r.error(), attempts, List.of());
            }
        }
    }

    private EventHandlerActivities handler(ProcessorConfig.Policy p) {
        String k = p.maxAttempts() + "/" + p.initialInterval() + "/" + p.maxInterval() + "/" + p.startToClose();
        return handlerStubs.computeIfAbsent(k, x -> Workflow.newLocalActivityStub(EventHandlerActivities.class,
                LocalActivityOptions.newBuilder()
                        .setStartToCloseTimeout(p.startToClose())
                        .setRetryOptions(RetryOptions.newBuilder()
                                .setMaximumAttempts(p.maxAttempts())
                                .setInitialInterval(p.initialInterval())
                                .setBackoffCoefficient(2)
                                .setMaximumInterval(p.maxInterval())
                                .build())
                        .build()));
    }

    // ------------------------------------------------------------------ timers, activities

    /** Moves due scheduled events onto their lock chains. */
    private void fireDue() {
        long now = Workflow.currentTimeMillis();
        while (!scheduled.isEmpty() && scheduled.getFirst().scheduledAtMillis() <= now) {
            EventEnvelope e = scheduled.removeFirst();
            startEnqueue(EnqueueRequest.lock(LockRequest.first(e, now)));
            SmStateRow row = lifecycleRow(e, EventLifecycle.NEW, "due", null, 0, 0, null, List.of(), now);
            persist(List.of(row), List.of(trace(e, now, TraceRow.Stage.LOCK_WAIT, "timer fired, enqueued on " + e.processorKey())));
            publish(row, e, now);
        }
    }

    private void startEnqueue(EnqueueRequest r) {
        if (!r.isEmpty()) {
            pending.add(Async.procedure(enqueuer::enqueue, r));
        }
    }

    private void startUnblock(String lockKey, String requestId) {
        pending.add(Async.procedure(enqueuer::unblock, lockKey, requestId));
    }

    private void publish(SmStateRow row, EventEnvelope e, long now) {
        Instant created = e.sourceTsMillis() > 0 ? Instant.ofEpochMilli(e.sourceTsMillis()) : null;
        EntitySnapshot s = new EntitySnapshot(row.workflowId(), row.smType(), row.state(), row.version(), created,
                Instant.ofEpochMilli(now), row.data());
        pending.add(Async.procedure(snapshots::publish, s));
    }

    private void persist(List<SmStateRow> rows, List<TraceRow> traces) {
        persistence.persist(rows, traces);
    }

    /** Waits until no update is running and every started activity has finished. */
    private void quiesce() {
        Workflow.await(() -> Workflow.isEveryHandlerFinished() && pending.stream().allMatch(Promise::isCompleted));
        for (Promise<Void> p : pending) {
            try {
                p.get();
            } catch (ActivityFailure f) {
                log.warn("activity of {} failed: {}", workflowId(), f.getMessage());
            }
        }
        pending.clear();
    }

    // ------------------------------------------------------------------ helpers

    private SmStateRow lifecycleRow(EventEnvelope e, EventLifecycle status, String outcome, String error, int attempts,
            int retries, String requestId, List<EventEnvelope> emitted, long now) {
        String data = EventRows.lifecycle(e, status, outcome, error, attempts, retries, requestId, emitted);
        return new SmStateRow(EventRows.eventRowId(e.eventId()), EventRows.eventSmType(e.eventType()), status.name(),
                data, nextVersion(now), e.eventId(), now);
    }

    /** Lifecycle row versions: monotonic, roughly workflow time, so later writes always win. */
    private long nextVersion(long now) {
        lastVersion = Math.max(lastVersion + 1, now);
        return lastVersion;
    }

    private TraceRow trace(EventEnvelope e, long now, TraceRow.Stage stage, String detail) {
        return new TraceRow(e.eventId(), now, stage, workflowId(), null, detail);
    }

    /** {@code e2eMs} (ingest -> done) and {@code srcMs} (producer -> done), as entity DONE rows have them. */
    private static String doneTiming(EventEnvelope e, long now) {
        long e2e = e.ingestTsMillis() > 0 ? now - e.ingestTsMillis() : -1;
        long src = e.sourceTsMillis() > 0 ? now - e.sourceTsMillis() : -1;
        return " e2eMs=" + e2e + " srcMs=" + src;
    }

    private EventLifecycle status(String id) {
        if (recentDone.contains(id)) {
            return EventLifecycle.DONE;
        }
        if (blocked.containsKey(id)) {
            return EventLifecycle.ERROR_BLOCKING;
        }
        if (parked.containsKey(id)) {
            return EventLifecycle.ERROR_NON_BLOCKING;
        }
        return EventLifecycle.SCHEDULED;
    }

    private void checkTarget(EventEnvelope e) {
        if (!e.isEventStyle()) {
            throw new IllegalArgumentException("event " + e.eventId() + " is not event style");
        }
        String expected = WorkflowIds.processor(e.domain(), e.processorKey());
        if (!expected.equals(workflowId())) {
            throw new IllegalArgumentException("event " + e.eventId() + " belongs to " + expected + ", not " + workflowId());
        }
    }

    private void sortScheduled() {
        scheduled.sort(Comparator.comparingLong(EventEnvelope::scheduledAtMillis).thenComparing(EventEnvelope::eventId));
    }

    private void rememberDone(String id) {
        recentDone.add(id);
        if (recentDone.size() > DONE_WINDOW) {
            recentDone.remove(recentDone.iterator().next());
        }
    }

    private void remember(EventEnvelope e, EventLifecycle status, String detail, long now) {
        recent.addLast(new ProcessorStatus.Recent(e.eventId(), e.eventType(), status, detail, now));
        while (recent.size() > RECENT_LIMIT) {
            recent.removeFirst();
        }
    }

    private static ProcessorStatus.Item item(Pending p) {
        return new ProcessorStatus.Item(p.event().eventId(), p.event().eventType(), p.sinceMillis(), p.requestId(),
                p.error(), p.attempts());
    }

    private static EventOutcome outcome(String id, EventLifecycle status, String detail, int attempts, List<String> children) {
        return new EventOutcome(id, status, detail, attempts, children);
    }

    private static List<String> ids(List<EventEnvelope> events) {
        return events.stream().map(EventEnvelope::eventId).toList();
    }

    private static String message(ActivityFailure f) {
        Throwable c = f.getCause();
        if (c instanceof ApplicationFailure af) {
            return af.getType() + ": " + af.getOriginalMessage();
        }
        return c != null ? String.valueOf(c.getMessage()) : f.getMessage();
    }

    private String workflowId() {
        return Workflow.getInfo().getWorkflowId();
    }
}
