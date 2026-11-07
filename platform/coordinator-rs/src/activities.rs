//! The coordinator's activities (`DispatchActivitiesImpl` and `ShardConsumerActivitiesImpl` in Java).
//! Their type names and argument lists are the Java ones (`DispatchToEntity(event, lockWaitMs)`, ...), so an
//! activity task scheduled by either implementation's workflows runs on either worker.

use crate::dispatcher::{Dispatcher, now_millis, trace_done};
use crate::model::{EventEnvelope, EventOutcome, LockRequest, ProcessRequest, ShardInfo, ShardResult, Stage, TraceRow, TransitionResult};
use crate::routing::{RouteError, Router};
use crate::store::Store;
use crate::trace::TraceWriter;
use aws_sdk_kinesis::Client as Kinesis;
use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorError;
use aws_sdk_kinesis::types::ShardIteratorType;
use futures::future::join_all;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use temporalio_common::data_converters::{MultiArgs2, MultiArgs4};
use temporalio_common::error::ApplicationFailure;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::{info, warn};

/// Failure type of an entity / processor validator rejection; never retried (`EntityRejectedException.TYPE`).
pub const ENTITY_REJECTED: &str = "EntityRejected";

pub struct Activities {
    pub router: Router,
    pub traces: TraceWriter,
    pub store: Store,
    pub dispatcher: Arc<Dispatcher>,
    pub kinesis: Kinesis,
    pub idle_poll: Duration,
    pub checkpoint_every: Duration,
}

/// Runs a call within this attempt's start-to-close budget. Temporal retries a timed-out attempt but does not
/// stop it; without this, every attempt against a slow or absent worker would stay blocked and finish
/// together once the worker answers. Update ids make the abandoned calls harmless.
async fn within_attempt<T>(ctx: &ActivityContext, call: impl Future<Output = T>) -> Result<T, ActivityError> {
    let budget = ctx
        .info()
        .deadline
        .and_then(|d| d.duration_since(std::time::SystemTime::now()).ok())
        .map(|left| left.saturating_sub(Duration::from_millis(500)))
        .unwrap_or(Duration::from_secs(29));
    tokio::time::timeout(budget, call).await.map_err(|_| anyhow::anyhow!("no answer within the attempt's timeout").into())
}

fn rejected(message: String) -> ActivityError {
    ActivityError::application(ApplicationFailure::builder(message).type_name(ENTITY_REJECTED.to_owned()).non_retryable(true).build())
}

impl Activities {
    fn trace_granted(&self, event: &EventEnvelope, workflow_id: String, waits: &BTreeMap<String, i64>) {
        let now = now_millis();
        for (key, waited) in waits {
            self.traces.add(TraceRow::new(
                &event.event_id,
                now,
                Stage::LockGranted,
                Some(workflow_id.clone()),
                Some(key.clone()),
                Some(format!("waitedMs={waited}")),
            ));
        }
    }
}

#[activities]
impl Activities {
    /// UpdateWithStart `handle` on the entity, waiting for the transition (local activity of the last lock).
    #[activity(name = "DispatchToEntity")]
    pub async fn dispatch_to_entity(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: MultiArgs2<EventEnvelope, BTreeMap<String, i64>>,
    ) -> Result<TransitionResult, ActivityError> {
        let MultiArgs2(event, waits) = input;
        if ctx.info().attempt <= 1 {
            self.trace_granted(&event, event.entity_key(), &waits);
        }
        match within_attempt(&ctx, self.router.update_entity(&event)).await? {
            Ok(r) => {
                trace_done(&self.traces, &event, &r);
                Ok(r)
            }
            Err(RouteError::Rejected(m)) => Err(rejected(m)),
            Err(RouteError::Other(e)) => Err(e.into()),
        }
    }

    /// Event style: UpdateWithStart `process` on the event's processor, waiting for the outcome.
    #[activity(name = "DispatchToProcessor")]
    pub async fn dispatch_to_processor(self: Arc<Self>, ctx: ActivityContext, request: ProcessRequest) -> Result<EventOutcome, ActivityError> {
        if ctx.info().attempt <= 1 {
            self.trace_granted(&request.event, request.event.trace_workflow_id(), &request.lock_wait_ms);
        }
        match within_attempt(&ctx, self.router.process(&request)).await? {
            Ok(o) => Ok(o),
            Err(RouteError::Rejected(m)) => Err(rejected(m)),
            Err(RouteError::Other(e)) => Err(e.into()),
        }
    }

    /// Event style, future-dated: `schedule` on the processor (no locks taken).
    #[activity(name = "ScheduleEvent")]
    pub async fn schedule_event(self: Arc<Self>, _ctx: ActivityContext, event: EventEnvelope) -> Result<(), ActivityError> {
        self.router.schedule(&event).await?;
        Ok(())
    }

    /// `acquire` on the next lock of a chain; returns once the request is in its queue.
    #[activity(name = "EnqueueLock")]
    pub async fn enqueue_lock(self: Arc<Self>, _ctx: ActivityContext, input: MultiArgs2<String, LockRequest>) -> Result<(), ActivityError> {
        let MultiArgs2(lock_key, request) = input;
        if self.router.enqueue_lock(&lock_key, &request).await? {
            self.traces.add(TraceRow::new(
                &request.request_id,
                now_millis(),
                Stage::LockWait,
                Some(request.event.trace_workflow_id()),
                Some(lock_key),
                Some(format!("position {}/{}", request.key_index + 1, request.keys().len())),
            ));
        }
        Ok(())
    }

    /// `release` to each lock, in parallel. A failed release is logged: the lock's forward lease frees it.
    #[activity(name = "ReleaseLocks")]
    pub async fn release_locks(self: Arc<Self>, _ctx: ActivityContext, input: MultiArgs2<Vec<String>, String>) -> Result<(), ActivityError> {
        let MultiArgs2(keys, request_id) = input;
        join_all(keys.iter().map(|key| {
            let me = &self;
            let request_id = &request_id;
            async move {
                match me.router.release(key, request_id).await {
                    Ok(()) => me.traces.add(TraceRow::new(request_id, now_millis(), Stage::Released, None, Some(key.clone()), None)),
                    Err(e) => warn!("release of {request_id} on {key} failed: {e:#}"),
                }
            }
        }))
        .await;
        Ok(())
    }

    #[activity(name = "RecordTrace")]
    pub async fn record_trace(self: Arc<Self>, _ctx: ActivityContext, rows: Vec<TraceRow>) -> Result<(), ActivityError> {
        self.traces.add_all(rows);
        Ok(())
    }

    #[activity(name = "ListShards")]
    pub async fn list_shards(self: Arc<Self>, _ctx: ActivityContext, stream: String) -> Result<Vec<ShardInfo>, ActivityError> {
        let mut out = Vec::new();
        let mut next: Option<String> = None;
        loop {
            let req = match &next {
                None => self.kinesis.list_shards().stream_name(&stream),
                Some(token) => self.kinesis.list_shards().next_token(token),
            };
            let resp = req.send().await.map_err(anyhow::Error::from)?;
            for s in resp.shards() {
                out.push(ShardInfo {
                    shard_id: s.shard_id().to_owned(),
                    parent_shard_id: s.parent_shard_id().map(str::to_owned),
                    adjacent_parent_shard_id: s.adjacent_parent_shard_id().map(str::to_owned),
                });
            }
            next = resp.next_token().map(str::to_owned);
            if next.is_none() {
                return Ok(out);
            }
        }
    }

    /// Reads one shard (long-running, heartbeating). The heartbeat carries the last dispatched sequence
    /// number, so a retry on another coordinator resumes exactly there; the store holds the same
    /// checkpoint for supervisor restarts.
    #[activity(name = "ConsumeShard")]
    pub async fn consume_shard(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: MultiArgs4<String, String, String, i64>,
    ) -> Result<ShardResult, ActivityError> {
        let MultiArgs4(stream, shard_id, initial_position, run_minutes) = input;
        let from_heartbeat: Option<String> = ctx.heartbeat_details().deserialize::<Option<String>>().ok().flatten().flatten();
        let mut seq = match from_heartbeat {
            Some(s) => Some(s),
            None => self.store.load_checkpoint(&stream, &shard_id).await?,
        };
        let mut saved = seq.clone();
        info!("consuming {stream}/{shard_id} from {}", seq.as_deref().map(|s| format!("after {s}")).unwrap_or(initial_position.clone()));

        let mut iterator = self.iterator(&stream, &shard_id, seq.as_deref(), &initial_position).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(run_minutes.max(1) as u64 * 60);
        let mut last_save = tokio::time::Instant::now();
        let mut processed: i64 = 0;
        while tokio::time::Instant::now() < deadline {
            if ctx.is_cancelled() {
                self.save_quietly(&stream, &shard_id, &seq, &saved).await;
                return Err(ActivityError::cancelled());
            }
            let resp = match self.kinesis.get_records().shard_iterator(&iterator).limit(500).send().await {
                Ok(r) => r,
                Err(e) if matches!(e.as_service_error(), Some(GetRecordsError::ExpiredIteratorException(_))) => {
                    iterator = self.iterator(&stream, &shard_id, seq.as_deref(), &initial_position).await?;
                    continue;
                }
                Err(e) => {
                    self.save_quietly(&stream, &shard_id, &seq, &saved).await;
                    return Err(anyhow::Error::from(e).into());
                }
            };
            let records = resp.records();
            if !records.is_empty() {
                let now = now_millis();
                let batch: Vec<EventEnvelope> = records
                    .iter()
                    .filter_map(|r| match serde_json::from_slice::<EventEnvelope>(r.data().as_ref()) {
                        Ok(e) => Some(e.with_ingest_ts(now)),
                        Err(bad) => {
                            warn!("skipping unparseable record {} on {shard_id}: {bad}", r.sequence_number());
                            None
                        }
                    })
                    .collect();
                if let Err(e) = self.dispatch_with_heartbeat(&ctx, batch, &seq).await {
                    self.save_quietly(&stream, &shard_id, &seq, &saved).await;
                    return Err(e.into());
                }
                seq = records.last().map(|r| r.sequence_number().to_owned());
                processed += records.len() as i64;
            }
            let _ = ctx.record_heartbeat(seq.clone()).await;
            if seq.is_some() && seq != saved && last_save.elapsed() >= self.checkpoint_every {
                if let Some(s) = &seq {
                    self.store.save_checkpoint(&stream, &shard_id, s).await?;
                }
                saved = seq.clone();
                last_save = tokio::time::Instant::now();
            }
            match resp.next_shard_iterator() {
                None => {
                    if let Some(s) = &seq {
                        self.store.save_checkpoint(&stream, &shard_id, s).await?;
                    }
                    info!("shard {shard_id} closed after {processed} records");
                    return Ok(ShardResult { shard_id, closed: true, last_sequence_number: seq, records_processed: processed });
                }
                Some(next) => iterator = next.to_owned(),
            }
            if records.is_empty() {
                tokio::time::sleep(self.idle_poll).await;
            }
        }
        self.save_quietly(&stream, &shard_id, &seq, &saved).await;
        Ok(ShardResult { shard_id, closed: false, last_sequence_number: seq, records_processed: processed })
    }
}

impl Activities {
    /// While downstream is saturated a batch takes long (the backpressure: records wait in Kinesis); keep
    /// heartbeating with the last fully dispatched sequence number so it is not taken for a dead consumer.
    async fn dispatch_with_heartbeat(&self, ctx: &ActivityContext, batch: Vec<EventEnvelope>, committed: &Option<String>) -> anyhow::Result<()> {
        let dispatch = self.dispatcher.dispatch_batch(batch);
        tokio::pin!(dispatch);
        let mut beat = tokio::time::interval(Duration::from_secs(1));
        beat.tick().await;
        loop {
            tokio::select! {
                r = &mut dispatch => return r,
                _ = beat.tick() => { let _ = ctx.record_heartbeat(committed.clone()).await; }
            }
        }
    }

    async fn iterator(&self, stream: &str, shard: &str, after: Option<&str>, initial: &str) -> anyhow::Result<String> {
        if let Some(seq) = after {
            match self.raw_iterator(stream, shard, Some(seq), initial).await {
                Ok(it) => return Ok(it),
                Err(e)
                    if matches!(
                        e.as_service_error(),
                        Some(GetShardIteratorError::InvalidArgumentException(_) | GetShardIteratorError::ResourceNotFoundException(_))
                    ) =>
                {
                    // The checkpoint belongs to an earlier incarnation of the stream: start from the
                    // configured position instead of failing forever; dedupe absorbs replays.
                    warn!("checkpoint {seq} is not valid for {stream}/{shard} ({e}); starting from {initial}");
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(self.raw_iterator(stream, shard, None, initial).await?)
    }

    async fn raw_iterator(
        &self,
        stream: &str,
        shard: &str,
        after: Option<&str>,
        initial: &str,
    ) -> Result<String, aws_sdk_kinesis::error::SdkError<GetShardIteratorError>> {
        let mut req = self.kinesis.get_shard_iterator().stream_name(stream).shard_id(shard);
        req = match after {
            Some(seq) => req.shard_iterator_type(ShardIteratorType::AfterSequenceNumber).starting_sequence_number(seq),
            None => req.shard_iterator_type(ShardIteratorType::from(initial)),
        };
        Ok(req.send().await?.shard_iterator().unwrap_or_default().to_owned())
    }

    async fn save_quietly(&self, stream: &str, shard: &str, seq: &Option<String>, saved: &Option<String>) {
        if let Some(s) = seq.as_ref().filter(|s| Some(*s) != saved.as_ref())
            && let Err(e) = self.store.save_checkpoint(stream, shard, s).await
        {
            warn!("final checkpoint for {shard} failed: {e:#}");
        }
    }
}
