//! `IngestDispatcher`: one shard batch into Temporal.
//!
//! 1. dedupe the whole batch in one store round trip, drop duplicates;
//! 2. group the survivors by their first lock key: groups run in parallel, events of a group run one after
//!    the other in shard order (the per-key ordering guarantee);
//! 3. hand each event to Temporal and return without waiting for it to be applied: `acquire` on the lock of
//!    its first key, waiting only until the lock accepted it (direct types: UpdateWithStart on the entity;
//!    future-dated event-style events: `schedule` on their processor);
//! 4. mark the batch DISPATCHED.
//!
//! Nothing here looks at payloads or entity state: every flow goes through the same code.

use crate::model::{EventEnvelope, LockRequest, Stage, TraceRow};
use crate::routing::{self, Router};
use crate::store::Store;
use crate::trace::TraceWriter;
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;

pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct Dispatcher {
    store: Store,
    router: Router,
    traces: TraceWriter,
    direct_sm_types: HashSet<String>,
    in_flight: Arc<Semaphore>,
    pub dispatched: AtomicU64,
    pub dropped: AtomicU64,
}

impl Dispatcher {
    pub fn new(store: Store, router: Router, traces: TraceWriter, direct_sm_types: HashSet<String>, max_parallel_groups: usize) -> Self {
        Dispatcher {
            store,
            router,
            traces,
            direct_sm_types,
            in_flight: Arc::new(Semaphore::new(max_parallel_groups.max(1))),
            dispatched: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    pub async fn dispatch_batch(&self, events: Vec<EventEnvelope>) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let now = now_millis();
        let ids: Vec<String> = events.iter().map(|e| e.event_id.clone()).collect();
        let claimed = self.store.claim_for_dispatch(&ids, now).await?;

        let mut order: Vec<String> = Vec::new();
        let mut groups: HashMap<String, Vec<EventEnvelope>> = HashMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        for e in events {
            if !claimed.contains(&e.event_id) || !seen.insert(e.event_id.clone()) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.traces.add(TraceRow::new(&e.event_id, now, Stage::Dropped, Some(e.trace_workflow_id()), None, Some("duplicate".into())));
                continue;
            }
            let keys = e.effective_lock_keys();
            let at = now_millis();
            let kinesis_ms = if e.source_ts_millis > 0 { e.ingest_ts_millis - e.source_ts_millis } else { -1 };
            self.traces.add(TraceRow::new(
                &e.event_id,
                at,
                Stage::Received,
                Some(e.trace_workflow_id()),
                None,
                Some(format!("kinesisMs={kinesis_ms} dedupeMs={} keys=[{}]", at - e.ingest_ts_millis, keys.join(", "))),
            ));
            let first = keys.into_iter().next().unwrap_or_default();
            match groups.get_mut(&first) {
                Some(group) => group.push(e),
                None => {
                    order.push(first.clone());
                    groups.insert(first, vec![e]);
                }
            }
        }

        let runs = order.into_iter().filter_map(|k| groups.remove(&k)).map(|group| async move {
            let _permit = self.in_flight.acquire().await?;
            for e in group {
                self.dispatch_one(e).await?;
            }
            anyhow::Ok(())
        });
        // A failed group fails the batch: the consumer retries from its last checkpoint, the dedupe store
        // re-dispatches whatever is still RECEIVED, and Temporal update ids absorb duplicates.
        try_join_all(runs).await?;
        let seen: Vec<String> = seen.into_iter().collect();
        self.store.mark_dispatched(&seen).await?;
        Ok(())
    }

    pub async fn dispatch_one(&self, e: EventEnvelope) -> anyhow::Result<()> {
        let now = now_millis();
        if e.is_event_style() && e.scheduled_at_millis > now {
            // Future-dated: its processor keeps it on a durable timer, holding no locks until due.
            self.router.schedule(&e).await?;
        } else if !e.is_event_style() && e.is_single_key() && (e.is_direct() || self.direct_sm_types.contains(&e.sm_type)) {
            self.dispatch_direct(e).await?;
        } else {
            // Single- and multi-key alike: enqueue on the first sorted key; the lock chain does the rest.
            let first = e.first_lock_key();
            let request = LockRequest::first(e, now);
            if self.router.enqueue_lock(&first, &request).await? {
                self.traces.add(TraceRow::new(
                    &request.request_id,
                    now_millis(),
                    Stage::LockWait,
                    Some(request.event.trace_workflow_id()),
                    Some(first),
                    Some(format!("position 1/{}", request.keys().len())),
                ));
            }
        }
        self.dispatched.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Direct path (types that never share keys): returns once the entity accepted the update; the result
    /// is traced in the background.
    async fn dispatch_direct(&self, e: EventEnvelope) -> anyhow::Result<()> {
        let handle = self.router.update_entity_accepted(&e).await?;
        let traces = self.traces.clone();
        tokio::spawn(async move {
            match routing::await_result(handle).await {
                Ok(r) => trace_done(&traces, &e, &r),
                Err(err) => traces.add(TraceRow::new(&e.event_id, now_millis(), Stage::Failed, Some(e.entity_key()), None, Some(err.to_string()))),
            }
        });
        Ok(())
    }
}

/// DONE row: e2eMs = Kinesis read to transition done, srcMs = producer timestamp to transition done.
pub fn trace_done(traces: &TraceWriter, e: &EventEnvelope, r: &crate::model::TransitionResult) {
    let now = now_millis();
    let e2e = if e.ingest_ts_millis > 0 { now - e.ingest_ts_millis } else { -1 };
    let src = if e.source_ts_millis > 0 { now - e.source_ts_millis } else { -1 };
    traces.add(TraceRow::new(
        &e.event_id,
        now,
        Stage::Done,
        Some(e.entity_key()),
        None,
        Some(format!("{} e2eMs={e2e} srcMs={src}", if r.accepted { "accepted" } else { "rejected" })),
    ));
}
