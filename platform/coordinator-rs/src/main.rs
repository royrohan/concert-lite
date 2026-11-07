//! concert-coordinator: the Rust coordinator. A Temporal worker on task queue `orchestration` hosting the
//! ingest supervisor, the shard consumers, the key locks and the dispatch activities. Run as many as you
//! like; Temporal spreads the work and fails it over.
//!
//! Environment (same names and defaults as the Java coordinator):
//!
//! ```text
//! TEMPORAL_ADDRESS=localhost:7233   TEMPORAL_NAMESPACE=default
//! CONCERT_FLOW=                     the flow served: stream concert-<flow>, queue orchestration-<flow>,
//!                                   lock ids lock:<flow>:<key> (empty: concert-events, orchestration, lock:<key>)
//! KINESIS_ENDPOINT=http://localhost:4566   INGEST_STREAM=concert-<flow>   INGEST_POSITION=TRIM_HORIZON
//! INGEST_CREATE_STREAM=             create the stream if missing (default: true under CONCERT_FLOW), KINESIS_SHARDS=2
//! STORE_KIND=postgres|dsql|dynamo   STORE_JDBC_URL=jdbc:postgresql://localhost:5433/concert
//! DIRECT_SM_TYPES=                  types that may bypass locks (no shared keys!)
//! DEDUPE_TTL_HOURS=24  TRACE_SAMPLE=1.0  INGEST_PARALLEL_GROUPS=256  INGEST_IDLE_POLL_MS=20 ...
//! ```

use concert_coordinator::activities::Activities;
use concert_coordinator::config::Env;
use concert_coordinator::dispatcher::{Dispatcher, now_millis};
use concert_coordinator::model::{Flow, IngestConfig, Sampling, ids};
use concert_coordinator::routing::Router;
use concert_coordinator::store;
use concert_coordinator::trace::TraceWriter;
use concert_coordinator::workflows::{IngestSupervisor, KeyLock};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use temporalio_client::errors::WorkflowStartError;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions, WorkflowIdConflictPolicy, WorkflowStartOptions};
use temporalio_sdk::runtime::worker_tuner::{FixedSizeSlotSupplier, TunerHolder};
use temporalio_sdk::runtime::{AutoscalingOptions, PollerBehavior, RuntimeOptions};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,temporalio_sdk_core=warn,tokio_postgres=warn".into()))
        .init();
    let env = Env::process();

    let flow = Flow::new(&env.str("CONCERT_FLOW", ""));
    let queue = flow.orchestration_queue();
    let stream = env.get("INGEST_STREAM").unwrap_or_else(|| flow.stream());
    let store = store::from_env(&env).await?;
    let client = connect(&env).await?;
    let router = Router::new(client.clone(), &env, flow.clone());
    let (traces, flusher) = TraceWriter::new(store.clone(), Sampling::new(env.f64("TRACE_SAMPLE", 1.0)));
    let direct: HashSet<String> =
        env.str("DIRECT_SM_TYPES", "").split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned).collect();
    let dispatcher = Arc::new(Dispatcher::new(store.clone(), router.clone(), traces.clone(), direct.clone(), env.usize("INGEST_PARALLEL_GROUPS", 256)));
    let kinesis = kinesis_client(&env).await;
    if env.bool("INGEST_CREATE_STREAM", !flow.is_legacy()) {
        // a flow's stream is the flow's own: its coordinator creates it if Start flow (trace UI) did not
        if ensure_stream(&kinesis, &stream, env.i64("KINESIS_SHARDS", 2) as i32, Duration::from_secs(120)).await? {
            info!("created stream {stream}");
        }
    }
    let activities = Activities {
        router: router.clone(),
        traces: traces.clone(),
        store: store.clone(),
        dispatcher,
        kinesis,
        idle_poll: env.millis("INGEST_IDLE_POLL_MS", 20),
        checkpoint_every: env.millis("INGEST_CHECKPOINT_MS", 500),
    };

    let runtime = Runtime::from_current_tokio(RuntimeOptions::default())?;
    let tuner = TunerHolder::builder()
        .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(env.usize("WORKER_WF_SLOTS", 1000)))
        .activity_task_slot_supplier(FixedSizeSlotSupplier::new(env.usize("WORKER_ACTIVITY_SLOTS", 1000)))
        .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(env.usize("WORKER_LA_SLOTS", 2000)))
        .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .build();
    let options = WorkerOptions::new(queue.clone())
        .register_workflow::<KeyLock>()?
        .register_workflow::<IngestSupervisor>()?
        .register_activities(activities)
        .max_cached_workflows(env.usize("WORKER_WORKFLOW_CACHE", 50_000))
        .tuner(tuner)
        .workflow_task_poller_behavior(autoscaling(2, env.usize("ORCH_WF_POLLERS_MAX", 64), 8))
        .activity_task_poller_behavior(autoscaling(1, env.usize("ORCH_ACTIVITY_POLLERS_MAX", 8), 2))
        .sticky_queue_schedule_to_start_timeout(env.sticky_schedule_to_start())
        .graceful_shutdown_period(Duration::from_secs(10))
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), options)?;
    let shutdown = worker.shutdown_handle();

    if env.bool("INGEST_AUTOSTART", true) {
        let config = IngestConfig {
            stream: stream.clone(),
            initial_position: env.str("INGEST_POSITION", "TRIM_HORIZON"),
            shard_list_interval_sec: env.i64("INGEST_SHARD_LIST_SEC", 30),
            consume_run_minutes: env.i64("INGEST_RUN_MINUTES", 30),
            consumed_shards: vec![],
            heartbeat_timeout_sec: env.shard_heartbeat_timeout_sec(),
        };
        ensure_ingest(&client, &env, &queue, config).await?;
    }

    let ttl = Duration::from_secs(env.i64("DEDUPE_TTL_HOURS", 24).max(0) as u64 * 3600);
    let sweep_store = store.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(600)).await;
            match sweep_store.sweep_processed(now_millis() - ttl.as_millis() as i64, 1000).await {
                Ok(n) if n > 0 => info!("swept {n} dedupe rows"),
                Ok(_) => {}
                Err(e) => warn!("dedupe sweep failed: {e:#}"),
            }
        }
    });

    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        // Running shard consumers see the cancellation, checkpoint and fail: Temporal retries them on another
        // coordinator right away instead of after a heartbeat timeout.
        info!("shutting down");
        shutdown();
    });

    info!("rust coordinator up (flow={}, stream={stream}, queue={queue}, store={}, direct types={direct:?})", flow.name(), store.kind());
    worker.run().await?;
    drop(traces);
    flusher.close().await;
    Ok(())
}

async fn connect(env: &Env) -> anyhow::Result<Client> {
    let address = env.str("TEMPORAL_ADDRESS", "localhost:7233");
    let url = if address.contains("://") { address } else { format!("http://{address}") };
    let identity = format!("concert-coordinator-rs@{}", std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into()));
    let mut attempt = 0;
    loop {
        let conn_opts = ConnectionOptions::new(url.parse::<temporalio_client::Url>()?).identity(identity.clone()).build();
        match Connection::connect(conn_opts).await {
            Ok(connection) => {
                return Ok(Client::new(connection, ClientOptions::new(env.str("TEMPORAL_NAMESPACE", "default")).build())?);
            }
            Err(e) if attempt < 30 => {
                attempt += 1;
                warn!("temporal not reachable at {url} ({e}); retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn kinesis_client(env: &Env) -> aws_sdk_kinesis::Client {
    let region = aws_config::Region::new(env.str("AWS_REGION", "us-east-1"));
    let mut loader = aws_config::from_env().region(region);
    if let Some(endpoint) = env.get("KINESIS_ENDPOINT") {
        loader = loader
            .endpoint_url(endpoint)
            .credentials_provider(aws_sdk_kinesis::config::Credentials::new("test", "test", None, None, "static"));
    }
    aws_sdk_kinesis::Client::new(&loader.load().await)
}

/// Idempotently starts the supervisor of a stream (one per stream cluster-wide).
async fn ensure_ingest(client: &Client, env: &Env, queue: &str, config: IngestConfig) -> anyhow::Result<()> {
    let options = WorkflowStartOptions::new(queue, ids::ingest(&config.stream))
        .task_timeout(env.workflow_task_timeout())
        .id_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
        .build();
    match client.start_workflow(IngestSupervisor::run, config, options).await {
        Ok(_) | Err(WorkflowStartError::AlreadyStarted { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// `KinesisStreams.ensure`: creates the stream if it is missing and waits until it is ACTIVE; true if created.
async fn ensure_stream(kinesis: &aws_sdk_kinesis::Client, stream: &str, shards: i32, timeout: Duration) -> anyhow::Result<bool> {
    use aws_sdk_kinesis::types::StreamStatus;
    let status = |k: aws_sdk_kinesis::Client, s: String| async move {
        match k.describe_stream_summary().stream_name(s).send().await {
            Ok(out) => Ok(out.stream_description_summary.map(|d| d.stream_status)),
            Err(e) if e.as_service_error().is_some_and(|se| se.is_resource_not_found_exception()) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e)),
        }
    };
    let mut created = false;
    if status(kinesis.clone(), stream.to_owned()).await?.is_none() {
        match kinesis.create_stream().stream_name(stream).shard_count(shards).send().await {
            Ok(_) => created = true,
            Err(e) if e.as_service_error().is_some_and(|se| se.is_resource_in_use_exception()) => {} // created concurrently
            Err(e) => return Err(e.into()),
        }
    }
    let deadline = tokio::time::Instant::now() + timeout;
    while status(kinesis.clone(), stream.to_owned()).await? != Some(StreamStatus::Active) {
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "stream {stream} not ACTIVE after {timeout:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(created)
}

fn autoscaling(minimum: usize, maximum: usize, initial: usize) -> PollerBehavior {
    let maximum = maximum.max(minimum);
    PollerBehavior::Autoscaling(AutoscalingOptions::builder().minimum(minimum).maximum(maximum).initial(initial.clamp(minimum, maximum)).build())
}
