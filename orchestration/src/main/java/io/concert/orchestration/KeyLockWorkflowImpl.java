package io.concert.orchestration;

import io.concert.common.TraceRow;
import io.concert.common.api.DispatchActivities;
import io.concert.common.api.KeyLockWorkflow;
import io.concert.common.api.LockRequest;
import io.concert.common.api.LockSnapshot;
import io.concert.common.api.LockState;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import org.slf4j.Logger;

/**
 * FIFO lock for one key; see {@link KeyLockWorkflow} for the chain protocol. The head of the queue
 * owns the key:
 *
 * <ul>
 *   <li>last key of its chain: dispatched to the entity right here (local activity), earlier locks
 *       of the chain are released, and it is popped;
 *   <li>otherwise: forwarded to the next key's lock, and held until that chain releases it.
 * </ul>
 *
 * Completes after {@code idleSeconds} with an empty queue (the next UpdateWithStart starts a fresh run)
 * and continues-as-new every {@link #OPS_BEFORE_CONTINUE} operations, carrying the queue.
 */
public class KeyLockWorkflowImpl implements KeyLockWorkflow {

    /** Safety net only: a forwarded holder is released by its chain, even when dispatch fails. */
    static final Duration FORWARD_LEASE = Duration.ofHours(1);
    static final int OPS_BEFORE_CONTINUE = 1000;
    static final int RECENT_IDS = 4000;

    private static final Logger log = Workflow.getLogger(KeyLockWorkflowImpl.class);

    private final DispatchActivities dispatch = Workflow.newLocalActivityStub(
            DispatchActivities.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setScheduleToCloseTimeout(Duration.ofHours(1))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofMillis(50))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setDoNotRetry(EntityRejectedException.TYPE)
                            .build())
                    .build());

    /**
     * Releases go to <em>lower</em> keys, so they must not block this workflow task: a local activity
     * would hold the task open while the lock it releases may itself be blocked forwarding to us
     * (A forwards to B while B releases A = deadlock). A regular activity lets the task complete;
     * eager execution hands it straight back to this worker, so it stays fast.
     */
    private final DispatchActivities releaser = Workflow.newActivityStub(
            DispatchActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofMillis(50))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .build())
                    .build());

    private String lockKey;
    private final Deque<LockRequest> queue = new ArrayDeque<>();
    private final LinkedHashSet<String> recentIds = new LinkedHashSet<>();
    private final List<Promise<Void>> pendingReleases = new ArrayList<>();
    private long headForwardedAt;
    private long ops;
    private int idleSeconds;
    private boolean initialized;

    @Override
    public void run(LockState state) {
        lockKey = state.lockKey();
        headForwardedAt = state.headForwardedAtMillis();
        idleSeconds = state.idleSeconds() > 0 ? state.idleSeconds() : 600;
        // Updates accepted before run() may already be queued; carried requests go in front.
        List<LockRequest> carried = state.queue() == null ? List.of() : state.queue();
        for (int i = carried.size() - 1; i >= 0; i--) {
            LockRequest c = carried.get(i);
            if (!contains(c.requestId())) {
                queue.addFirst(c);
            }
        }
        if (state.recentRequestIds() != null) {
            state.recentRequestIds().forEach(this::remember);
        }
        initialized = true;

        while (true) {
            if (!Workflow.await(Duration.ofSeconds(idleSeconds), () -> !queue.isEmpty())) {
                drainReleases();
                // An update racing with this completion makes the server reject it; the caller's
                // UpdateWithStart then retries against a fresh run, so nothing is lost.
                Workflow.await(Workflow::isEveryHandlerFinished);
                if (queue.isEmpty()) {
                    return;
                }
                continue;
            }
            serveHead(queue.peekFirst());
            pendingReleases.removeIf(Promise::isCompleted);
            if (ops >= OPS_BEFORE_CONTINUE || Workflow.getInfo().isContinueAsNewSuggested()) {
                drainReleases();
                Workflow.await(Workflow::isEveryHandlerFinished);
                Workflow.continueAsNew(new LockState(lockKey, List.copyOf(queue), headForwardedAt, List.copyOf(recentIds), idleSeconds));
            }
        }
    }

    @Override
    public void validateAcquire(LockRequest request) {
        if (request == null || request.event() == null) {
            throw new IllegalArgumentException("request and event are required");
        }
        if (initialized && (recentIds.contains(request.requestId()) || contains(request.requestId()))) {
            throw new IllegalStateException(DUPLICATE);
        }
    }

    static final String DUPLICATE = "duplicate request";

    @Override
    public void acquire(LockRequest request) {
        if (!recentIds.contains(request.requestId()) && !contains(request.requestId())) {
            queue.addLast(request);
        }
    }

    @Override
    public void release(String requestId) {
        if (isHead(requestId)) {
            pop(requestId);
        } else {
            queue.removeIf(r -> r.requestId().equals(requestId));
        }
    }

    @Override
    public LockSnapshot snapshot() {
        LockRequest head = queue.peekFirst();
        List<String> waiting = new ArrayList<>();
        queue.stream().skip(1).forEach(r -> waiting.add(r.requestId()));
        String state = head == null ? null
                : headForwardedAt > 0 ? "forwarded to " + head.keys().get(head.keyIndex() + 1) : "dispatching";
        return new LockSnapshot(lockKey, head == null ? null : head.requestId(), state, waiting, ops);
    }

    private void serveHead(LockRequest head) {
        String id = head.requestId();
        if (head.isLastKey()) {
            long now = Workflow.currentTimeMillis();
            try {
                dispatch.dispatchToEntity(head.event(), head.waitsIncludingCurrent(now));
            } catch (ActivityFailure e) {
                // Rejected by the entity or retries exhausted: record it and keep the key moving.
                log.warn("dispatch of {} on {} failed: {}", id, lockKey, e.getMessage());
                dispatch.recordTrace(List.of(new TraceRow(id, Workflow.currentTimeMillis(), TraceRow.Stage.FAILED,
                        head.event().entityKey(), lockKey, String.valueOf(e.getCause()))));
            }
            pop(id);
            if (head.keyIndex() > 0) {
                // Not on this key's critical path: the next head proceeds while releases fly.
                pendingReleases.add(Async.procedure(releaser::releaseLocks, head.keys().subList(0, head.keyIndex()), id));
            }
            return;
        }
        if (headForwardedAt == 0) {
            long now = Workflow.currentTimeMillis();
            dispatch.enqueueLock(head.keys().get(head.keyIndex() + 1), head.forwardedTo(head.keyIndex() + 1, now));
            headForwardedAt = now;
        }
        if (!Workflow.await(FORWARD_LEASE, () -> !isHead(id))) {
            log.warn("forward lease expired for {} on {}, force releasing", id, lockKey);
            dispatch.recordTrace(List.of(new TraceRow(id, Workflow.currentTimeMillis(), TraceRow.Stage.FAILED,
                    head.event().entityKey(), lockKey, "forward lease expired")));
            pop(id);
        }
    }

    private void drainReleases() {
        for (Promise<Void> p : pendingReleases) {
            try {
                p.get();
            } catch (ActivityFailure e) {
                log.warn("release from {} failed: {}", lockKey, e.getMessage());
            }
        }
        pendingReleases.clear();
    }

    private boolean contains(String requestId) {
        for (LockRequest r : queue) {
            if (r.requestId().equals(requestId)) {
                return true;
            }
        }
        return false;
    }

    private boolean isHead(String requestId) {
        LockRequest head = queue.peekFirst();
        return head != null && head.requestId().equals(requestId);
    }

    private void pop(String requestId) {
        if (isHead(requestId)) {
            queue.removeFirst();
            headForwardedAt = 0;
            ops++;
            remember(requestId);
        }
    }

    private void remember(String requestId) {
        recentIds.add(requestId);
        if (recentIds.size() > RECENT_IDS) {
            recentIds.remove(recentIds.iterator().next());
        }
    }
}
