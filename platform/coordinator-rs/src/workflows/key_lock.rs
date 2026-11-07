//! `KeyLockWorkflow` (workflow id `lock:<key>`, task queue `orchestration`): a FIFO lock for one key.
//!
//! An event with sorted keys K0..Kn-1 is acquired as a chain: it is enqueued on K0; at the head of Ki the
//! lock forwards it to Ki+1 and keeps holding Ki; the lock of the last key dispatches the event (to its
//! entity, or for event style to its processor) and then releases K0..Kn-2. Keys are always taken in the
//! same global order, so chains never deadlock. The contract with the Java code is unchanged: the workflow
//! type, the `acquire` / `release` / `unblock` updates, the duplicate message of the `acquire` validator,
//! the `snapshot` query and the JSON of `LockState` / `LockRequest`.
//!
//! The lock never looks at payloads: it orders, forwards, dispatches and releases. Only workers mutate
//! entity state.

use crate::activities::{Activities, ENTITY_REJECTED};
use crate::model::{LockRequest, LockSnapshot, LockState, ProcessRequest, Stage, TraceRow};
use crate::routing::DUPLICATE;
use futures::future::LocalBoxFuture;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, UNIX_EPOCH};
use temporalio_common::RetryPolicy;
use temporalio_common::data_converters::MultiArgs2;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::workflows::select;
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, ContinueAsNewOptions, LocalActivityOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

/// Safety net only: a forwarded holder is released by its chain, even when dispatch fails.
pub const FORWARD_LEASE: Duration = Duration::from_secs(3600);
pub const OPS_BEFORE_CONTINUE: i64 = 1000;
pub const RECENT_IDS: usize = 4000;
/// How often a lock whose head is blocked re-checks whether it should continue-as-new.
const BLOCKED_RECHECK: Duration = Duration::from_secs(300);

/// A bounded, insertion-ordered set of finished request ids.
#[derive(Default, Debug)]
struct Recent {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl Recent {
    fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    fn remember(&mut self, id: String) {
        if self.set.insert(id.clone()) {
            self.order.push_back(id);
            if self.order.len() > RECENT_IDS
                && let Some(old) = self.order.pop_front()
            {
                self.set.remove(&old);
            }
        }
    }
}

#[workflow]
#[derive(Default)]
pub struct KeyLock {
    lock_key: String,
    queue: VecDeque<LockRequest>,
    recent: Recent,
    /// Non-zero when the head was forwarded to its next lock and waits for `release`.
    head_forwarded_at: i64,
    /// The head is an event-style event in ERROR_BLOCKING: kept until `unblock`.
    head_blocked: bool,
    unblocked: HashSet<String>,
    ops: i64,
    idle: Duration,
}

impl KeyLock {
    fn contains(&self, id: &str) -> bool {
        self.queue.iter().any(|r| r.request_id == id)
    }

    fn is_head(&self, id: &str) -> bool {
        self.queue.front().is_some_and(|h| h.request_id == id)
    }

    fn pop(&mut self, id: &str) {
        if self.is_head(id) {
            self.queue.pop_front();
            self.head_forwarded_at = 0;
            self.head_blocked = false;
            self.unblocked.remove(id);
            self.ops += 1;
            self.recent.remember(id.to_owned());
        }
    }

    fn carry(&self) -> LockState {
        LockState {
            lock_key: self.lock_key.clone(),
            queue: self.queue.iter().cloned().collect(),
            head_forwarded_at_millis: self.head_forwarded_at,
            recent_request_ids: self.recent.order.iter().cloned().collect(),
            idle_seconds: self.idle.as_secs() as i64,
            head_blocked: self.head_blocked,
        }
    }
}

fn now(ctx: &WorkflowContext<KeyLock>) -> i64 {
    ctx.workflow_time().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Local activity options of the dispatches and forwards: fast retries, an entity rejection is final.
fn dispatch_options() -> LocalActivityOptions {
    LocalActivityOptions::builder()
        .start_to_close_timeout(Duration::from_secs(30))
        .schedule_to_close_timeout(Duration::from_secs(3600))
        .retry_policy(
            RetryPolicy::builder()
                .initial_interval(Duration::from_millis(50))
                .maximum_interval(Duration::from_secs(10))
                .non_retryable_error_types([ENTITY_REJECTED])
                .build(),
        )
        .build()
}

/// Releases go to *lower* keys, so they must not hold this workflow task open: a local activity would,
/// while the lock it releases may itself be blocked forwarding to this one (deadlock). A regular activity
/// lets the task complete; eager execution hands it straight back to this worker.
fn release_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(
            RetryPolicy::builder().initial_interval(Duration::from_millis(50)).maximum_interval(Duration::from_secs(10)).build(),
        )
        .build()
}

type Pending<'a> = Vec<LocalBoxFuture<'a, ()>>;

#[workflow_methods]
impl KeyLock {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, state: LockState) -> Self {
        let mut me = KeyLock {
            lock_key: state.lock_key,
            head_forwarded_at: state.head_forwarded_at_millis,
            head_blocked: state.head_blocked,
            idle: Duration::from_secs(if state.idle_seconds > 0 { state.idle_seconds as u64 } else { 600 }),
            ..Default::default()
        };
        let mut seen = HashSet::new();
        for r in state.queue {
            if seen.insert(r.request_id.clone()) {
                me.queue.push_back(r);
            }
        }
        for id in state.recent_request_ids {
            me.recent.remember(id);
        }
        me
    }

    #[run(name = "KeyLockWorkflow")]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let mut releases: Pending<'_> = Vec::new();
        loop {
            if ctx.state(|s| s.queue.is_empty()) {
                let idle = ctx.state(|s| s.idle);
                if !wait_or_timeout(ctx, idle, |s| !s.queue.is_empty()).await {
                    drain(&mut releases).await;
                    if ctx.state(|s| s.queue.is_empty()) {
                        // An update racing with this completion is rejected by the server; the caller's
                        // UpdateWithStart then retries against a fresh run, so nothing is lost.
                        return Ok(());
                    }
                    continue;
                }
            }
            let head = match ctx.state(|s| s.queue.front().cloned()) {
                Some(h) => h,
                None => continue,
            };
            serve_head(ctx, head, &mut releases).await;
            if ctx.state(|s| s.ops) >= OPS_BEFORE_CONTINUE || ctx.continue_as_new_suggested() {
                drain(&mut releases).await;
                let carried = ctx.state(KeyLock::carry);
                ctx.continue_as_new(carried, ContinueAsNewOptions::default())?;
            }
        }
    }

    #[update_validator(acquire)]
    fn validate_acquire(&self, _ctx: &WorkflowContextView, request: &LockRequest) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.recent.contains(&request.request_id) || self.contains(&request.request_id) {
            return Err(DUPLICATE.into());
        }
        Ok(())
    }

    /// Enqueues the request; done as soon as it is in the queue (callers wait for ACCEPTED).
    #[update]
    pub fn acquire(&mut self, _ctx: &mut SyncWorkflowContext<Self>, request: LockRequest) {
        if !self.recent.contains(&request.request_id) && !self.contains(&request.request_id) {
            self.queue.push_back(request);
        }
    }

    /// Sent by the last lock of a chain once the event was applied. Idempotent.
    #[update]
    pub fn release(&mut self, _ctx: &mut SyncWorkflowContext<Self>, request_id: String) {
        if self.is_head(&request_id) {
            self.pop(&request_id);
        } else {
            self.queue.retain(|r| r.request_id != request_id);
        }
    }

    /// Sent by an event processor once a blocked head was retried successfully or skipped.
    #[update]
    pub fn unblock(&mut self, _ctx: &mut SyncWorkflowContext<Self>, request_id: String) {
        if self.contains(&request_id) {
            self.unblocked.insert(request_id);
        }
    }

    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> LockSnapshot {
        let head = self.queue.front();
        let holder_state = head.map(|h| {
            if self.head_blocked {
                "blocked (ERROR_BLOCKING, waiting for retry or skip)".to_owned()
            } else if self.head_forwarded_at > 0 {
                format!("forwarded to {}", h.keys().get(h.key_index + 1).cloned().unwrap_or_default())
            } else {
                "dispatching".to_owned()
            }
        });
        LockSnapshot {
            lock_key: self.lock_key.clone(),
            holder: head.map(|h| h.request_id.clone()),
            holder_state,
            waiting: self.queue.iter().skip(1).map(|r| r.request_id.clone()).collect(),
            ops_this_run: self.ops,
        }
    }
}

/// Waits until `cond` holds or `timeout` passes; true if the condition won. The timer is cancelled when
/// the condition wins, and not created at all when it already holds.
async fn wait_or_timeout(ctx: &WorkflowContext<KeyLock>, timeout: Duration, cond: impl Fn(&KeyLock) -> bool) -> bool {
    if ctx.state(&cond) {
        return true;
    }
    let mut timer = std::pin::pin!(ctx.timer(timeout));
    let mut condition = std::pin::pin!(ctx.wait_condition(&cond));
    let won = select! {
        _ = condition.as_mut() => true,
        _ = timer.as_mut() => false,
    };
    if won {
        timer.cancel();
    }
    won
}

async fn drain(releases: &mut Pending<'_>) {
    temporalio_sdk::workflows::join_all(releases.drain(..)).await;
}

fn record_failure<'a>(ctx: &'a WorkflowContext<KeyLock>, row: TraceRow) -> impl Future<Output = ()> + 'a {
    let fut = ctx.execute_local_activity(Activities::record_trace, vec![row], dispatch_options());
    async move {
        let _ = fut.await;
    }
}

async fn serve_head<'a>(ctx: &'a WorkflowContext<KeyLock>, head: LockRequest, releases: &mut Pending<'a>) {
    let id = head.request_id.clone();
    let lock_key = ctx.state(|s| s.lock_key.clone());
    let keys = head.keys();
    if head.is_last_key() {
        if head.event.is_event_style() {
            if !serve_event_head(ctx, &head, &lock_key).await {
                return; // still blocked and continue-as-new is due: the next run carries the blocked head
            }
        } else {
            let waits = head.waits_including_current(now_of(ctx));
            let r = ctx.execute_local_activity(Activities::dispatch_to_entity, MultiArgs2(head.event.clone(), waits), dispatch_options()).await;
            if let Err(e) = r {
                // Rejected by the entity, or retries exhausted: record it and keep the key moving.
                tracing::warn!("dispatch of {id} on {lock_key} failed: {e}");
                record_failure(ctx, TraceRow::new(&id, now_of(ctx), Stage::Failed, Some(head.event.entity_key()), Some(lock_key.clone()), Some(e.to_string())))
                    .await;
            }
        }
        ctx.state_mut(|s| s.pop(&id));
        if head.key_index > 0 {
            // Not on this key's critical path: the next head proceeds while the releases fly.
            let lower: Vec<String> = keys[..head.key_index].to_vec();
            let fut = ctx.execute_activity(Activities::release_locks, MultiArgs2(lower, id.clone()), release_options());
            releases.push(Box::pin(async move {
                if let Err(e) = fut.await {
                    tracing::warn!("release failed: {e}");
                }
            }));
        }
        return;
    }
    if ctx.state(|s| s.head_forwarded_at) == 0 {
        let at = now_of(ctx);
        let next = keys[head.key_index + 1].clone();
        let forwarded = head.forwarded_to(head.key_index + 1, at);
        if let Err(e) = ctx.execute_local_activity(Activities::enqueue_lock, MultiArgs2(next, forwarded), dispatch_options()).await {
            tracing::warn!("forward of {id} from {lock_key} failed: {e}");
        }
        // The chain may finish and release this head while the enqueue is in flight; marking the next
        // head as forwarded would park it until the lease expires.
        ctx.state_mut(|s| {
            if s.is_head(&id) {
                s.head_forwarded_at = at;
            }
        });
    }
    let released = {
        let id = id.clone();
        wait_or_timeout(ctx, FORWARD_LEASE, move |s| !s.is_head(&id)).await
    };
    if !released {
        tracing::warn!("forward lease expired for {id} on {lock_key}, force releasing");
        record_failure(
            ctx,
            TraceRow::new(&id, now_of(ctx), Stage::Failed, Some(head.event.trace_workflow_id()), Some(lock_key), Some("forward lease expired".into())),
        )
        .await;
        ctx.state_mut(|s| s.pop(&id));
    }
}

/// Last key of an event-style chain: the processor applies the event. DONE and ERROR_NON_BLOCKING free the
/// chain; ERROR_BLOCKING keeps the head (and every key of its chain) until the processor sends `unblock`.
///
/// Returns false if the head is still blocked and the run should continue-as-new.
async fn serve_event_head(ctx: &WorkflowContext<KeyLock>, head: &LockRequest, lock_key: &str) -> bool {
    let id = head.request_id.clone();
    if !ctx.state(|s| s.head_blocked) {
        let request = ProcessRequest {
            request_id: id.clone(),
            event: head.event.clone(),
            lock_key: lock_key.to_owned(),
            lock_wait_ms: head.waits_including_current(now_of(ctx)),
        };
        match ctx.execute_local_activity(Activities::dispatch_to_processor, request, dispatch_options()).await {
            Ok(outcome) if !outcome.blocked() => return true,
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("dispatch of {id} on {lock_key} to its processor failed: {e}");
                record_failure(
                    ctx,
                    TraceRow::new(&id, now_of(ctx), Stage::Failed, Some(head.event.trace_workflow_id()), Some(lock_key.to_owned()), Some(e.to_string())),
                )
                .await;
                return true;
            }
        }
        ctx.state_mut(|s| s.head_blocked = true);
    }
    loop {
        let cond_id = id.clone();
        let done = wait_or_timeout(ctx, BLOCKED_RECHECK, move |s| s.unblocked.contains(&cond_id) || !s.is_head(&cond_id)).await;
        if done {
            break;
        }
        if ctx.continue_as_new_suggested() {
            return false;
        }
    }
    let still_blocked = ctx.state(|s| s.is_head(&id) && !s.unblocked.contains(&id));
    if still_blocked {
        return false;
    }
    ctx.state_mut(|s| s.unblocked.remove(&id));
    true
}

fn now_of(ctx: &WorkflowContext<KeyLock>) -> i64 {
    now(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_ids_are_bounded() {
        let mut r = Recent::default();
        for i in 0..RECENT_IDS + 10 {
            r.remember(format!("r{i}"));
        }
        assert_eq!(r.order.len(), RECENT_IDS);
        assert!(!r.contains("r0"));
        assert!(r.contains(&format!("r{}", RECENT_IDS + 9)));
    }

    #[test]
    fn pop_only_pops_the_head() {
        let mut l = KeyLock::default();
        let e: crate::model::EventEnvelope =
            serde_json::from_str(r#"{"eventId":"a","smType":"o","instanceKey":"1","eventType":"x"}"#).unwrap();
        l.queue.push_back(LockRequest::first(e.clone(), 0));
        let mut e2 = e;
        e2.event_id = "b".into();
        l.queue.push_back(LockRequest::first(e2, 0));
        l.pop("b");
        assert_eq!(l.queue.len(), 2);
        l.pop("a");
        assert_eq!(l.queue.len(), 1);
        assert!(l.recent.contains("a"));
        assert_eq!(l.ops, 1);
    }
}
