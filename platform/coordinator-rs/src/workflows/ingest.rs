//! `IngestSupervisorWorkflow` (workflow id `ingest:<stream>`): exactly one consumer activity per open shard
//! (no KCL, no lease table). Child shards start only after their parents are fully consumed, which keeps
//! per-key order across resharding. Before continue-as-new it cancels and drains its consumers, so two
//! consumers never read one shard at once.

use crate::activities::Activities;
use crate::model::{IngestConfig, IngestStatus, ShardInfo, ShardResult};
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use std::collections::{BTreeMap, BTreeSet};
use std::future::poll_fn;
use std::task::Poll;
use std::time::Duration;
use temporalio_common::RetryPolicy;
use temporalio_common::data_converters::MultiArgs4;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::workflows::select;
use temporalio_sdk::{
    ActivityCancellationType, ActivityExecutionError, ActivityOptions, CancellableFuture, ContinueAsNewOptions, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

pub const ITERATIONS_BEFORE_CONTINUE: usize = 200;

#[workflow]
#[derive(Default)]
pub struct IngestSupervisor {
    config: IngestConfig,
    active: BTreeSet<String>,
    consumed: BTreeSet<String>,
    restarts: i64,
}

type Consumer<'a> = LocalBoxFuture<'a, Result<ShardResult, ActivityExecutionError>>;

/// A parent no longer listed aged out of retention: nothing left to read.
fn parent_done(parent: &Option<String>, consumed: &BTreeSet<String>, known: &BTreeSet<String>) -> bool {
    match parent {
        None => true,
        Some(p) => consumed.contains(p) || !known.contains(p),
    }
}

/// The shards to start now: open, not consumed, not running, and with their parents done.
pub fn startable(shards: &[ShardInfo], consumed: &BTreeSet<String>, active: &BTreeSet<String>) -> Vec<String> {
    let known: BTreeSet<String> = shards.iter().map(|s| s.shard_id.clone()).collect();
    shards
        .iter()
        .filter(|s| !consumed.contains(&s.shard_id) && !active.contains(&s.shard_id))
        .filter(|s| parent_done(&s.parent_shard_id, consumed, &known) && parent_done(&s.adjacent_parent_shard_id, consumed, &known))
        .map(|s| s.shard_id.clone())
        .collect()
}

/// Every consumer that completed, polled in shard order with the workflow's own waker (no foreign wakers,
/// so it stays deterministic); pending once none has.
fn completed<'a, 'b>(running: &'b mut BTreeMap<String, Consumer<'a>>) -> impl Future<Output = Vec<(String, Result<ShardResult, ActivityExecutionError>)>> + 'b {
    poll_fn(move |cx| {
        let mut done = Vec::new();
        for (shard, fut) in running.iter_mut() {
            if let Poll::Ready(r) = fut.as_mut().poll(cx) {
                done.push((shard.clone(), r));
            }
        }
        for (shard, _) in &done {
            running.remove(shard);
        }
        if done.is_empty() { Poll::Pending } else { Poll::Ready(done) }
    })
}

#[workflow_methods]
impl IngestSupervisor {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, config: IngestConfig) -> Self {
        let consumed = config.consumed_shards.iter().cloned().collect();
        IngestSupervisor { config, consumed, ..Default::default() }
    }

    #[run(name = "IngestSupervisorWorkflow")]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let config = ctx.state(|s| s.config.clone());
        let stream = config.stream.clone();
        let consumers = ctx.cancellation_token().child_token();
        let lister = ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
            .retry_policy(RetryPolicy::builder().maximum_interval(Duration::from_secs(10)).build())
            .build();
        let consumer_options = ActivityOptions::with_start_to_close_timeout(Duration::from_secs((config.consume_run_minutes.max(1) as u64 + 5) * 60))
            .heartbeat_timeout(Duration::from_secs(if config.heartbeat_timeout_sec > 0 { config.heartbeat_timeout_sec as u64 } else { 3 }))
            .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
            .cancellation_token(consumers.clone())
            .retry_policy(RetryPolicy::builder().initial_interval(Duration::from_secs(1)).maximum_interval(Duration::from_secs(30)).build())
            .build();

        let mut running: BTreeMap<String, Consumer<'_>> = BTreeMap::new();
        for _ in 0..ITERATIONS_BEFORE_CONTINUE {
            let shards = ctx.execute_activity(Activities::list_shards, stream.clone(), lister.clone()).await?;
            let start = ctx.state(|s| startable(&shards, &s.consumed, &s.active));
            for shard in start {
                let fut = ctx.execute_activity(
                    Activities::consume_shard,
                    MultiArgs4(stream.clone(), shard.clone(), config.initial_position.clone(), config.consume_run_minutes),
                    consumer_options.clone(),
                );
                running.insert(shard.clone(), fut.boxed_local());
                ctx.state_mut(|s| s.active.insert(shard));
            }

            let interval = Duration::from_secs(config.shard_list_interval_sec.max(1) as u64);
            let mut timer = std::pin::pin!(ctx.timer(interval));
            let done = if running.is_empty() {
                timer.as_mut().await;
                Vec::new()
            } else {
                let finished = select! {
                    d = completed(&mut running).fuse() => d,
                    _ = timer.as_mut() => Vec::new(),
                };
                if !finished.is_empty() {
                    timer.cancel();
                }
                finished
            };
            reap(ctx, done);
            if ctx.continue_as_new_suggested() {
                break;
            }
        }

        // Drain the consumers before handing over to the next run.
        consumers.cancel_with_reason("continue-as-new");
        while !running.is_empty() {
            let done = completed(&mut running).await;
            reap(ctx, done);
        }
        let carried = ctx.state(|s| IngestConfig { consumed_shards: s.consumed.iter().cloned().collect(), ..s.config.clone() });
        ctx.continue_as_new(carried, ContinueAsNewOptions::default())?;
        Ok(())
    }

    #[query]
    pub fn status(&self, _ctx: &WorkflowContextView) -> IngestStatus {
        IngestStatus {
            stream: self.config.stream.clone(),
            active_shards: self.active.iter().cloned().collect(),
            consumed_shards: self.consumed.iter().cloned().collect(),
            consumer_restarts: self.restarts,
        }
    }
}

fn reap(ctx: &WorkflowContext<IngestSupervisor>, done: Vec<(String, Result<ShardResult, ActivityExecutionError>)>) {
    for (shard, result) in done {
        ctx.state_mut(|s| {
            s.active.remove(&shard);
            match &result {
                Ok(r) if r.closed => {
                    s.consumed.insert(shard.clone());
                }
                Ok(_) => s.restarts += 1, // run budget used up: restarted on the next iteration
                Err(e) => {
                    s.restarts += 1;
                    tracing::warn!("consumer for {shard} ended: {e}");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(id: &str, parent: Option<&str>) -> ShardInfo {
        ShardInfo { shard_id: id.into(), parent_shard_id: parent.map(Into::into), adjacent_parent_shard_id: None }
    }

    #[test]
    fn children_wait_for_their_parents() {
        let shards = vec![shard("a", None), shard("b", Some("a"))];
        let none = BTreeSet::new();
        assert_eq!(startable(&shards, &none, &none), vec!["a"]);
        let consumed = BTreeSet::from(["a".to_string()]);
        assert_eq!(startable(&shards, &consumed, &none), vec!["b"]);
        // a parent that aged out of retention counts as done
        assert_eq!(startable(&[shard("c", Some("gone"))], &none, &none), vec!["c"]);
        let active = BTreeSet::from(["a".to_string()]);
        assert!(startable(&shards, &none, &active).is_empty());
    }
}
