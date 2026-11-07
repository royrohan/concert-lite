//! The part of `io.concert.store.StateStore` the coordinator uses: dedupe, shard checkpoints, trace rows
//! and the dedupe sweep. Entity projections (`sm_state`) belong to the workers and are never touched here.
//! Each backend writes the same tables, keys and values as its Java counterpart (`JdbcStateStore`,
//! `DynamoStateStore`), so the Java workers, trace UI and tools read what this coordinator writes.

mod dynamo;
mod memory;
mod postgres;

pub use dynamo::DynamoStore;
pub use memory::MemoryStore;
pub use postgres::PostgresStore;

use crate::config::Env;
use crate::model::TraceRow;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

#[async_trait]
pub trait CoordinatorStore: Send + Sync + 'static {
    fn kind(&self) -> &str;

    /// Records the ids as RECEIVED and returns those to dispatch: never seen before, or seen but never
    /// marked dispatched (a crash between claim and mark). Everything else is a duplicate.
    async fn claim_for_dispatch(&self, event_ids: &[String], now_millis: i64) -> anyhow::Result<HashSet<String>>;

    async fn mark_dispatched(&self, event_ids: &[String]) -> anyhow::Result<()>;

    async fn load_checkpoint(&self, stream: &str, shard_id: &str) -> anyhow::Result<Option<String>>;

    async fn save_checkpoint(&self, stream: &str, shard_id: &str, seq: &str) -> anyhow::Result<()>;

    async fn append_trace(&self, rows: &[TraceRow]) -> anyhow::Result<()>;

    /// Deletes dedupe rows older than the cutoff; stores with native expiry return 0.
    async fn sweep_processed(&self, older_than_millis: i64, batch_size: usize) -> anyhow::Result<u64>;

    /// For tests: the dedupe status of an event.
    async fn processed_status(&self, event_id: &str) -> anyhow::Result<Option<String>>;
}

pub type Store = Arc<dyn CoordinatorStore>;

/// `STORE_KIND`: postgres (default), dsql, dynamo; `memory` for local experiments. Spanner stays on the
/// Java coordinator.
pub async fn from_env(env: &Env) -> anyhow::Result<Store> {
    let kind = env.str("STORE_KIND", "postgres").to_lowercase();
    let init = env.bool("STORE_INIT_SCHEMA", true);
    let store: Store = match kind.as_str() {
        "postgres" | "dsql" => Arc::new(PostgresStore::from_env(env, kind == "dsql", init).await?),
        "dynamo" | "dynamodb" => Arc::new(DynamoStore::from_env(env, init).await?),
        "memory" => Arc::new(MemoryStore::default()),
        other => anyhow::bail!("STORE_KIND={other} is not supported by the Rust coordinator (postgres, dsql, dynamo, memory)"),
    };
    Ok(store)
}

/// Distinct ids in first-seen order.
pub(crate) fn distinct(ids: &[String]) -> Vec<String> {
    let mut seen = HashSet::with_capacity(ids.len());
    ids.iter().filter(|id| seen.insert(id.as_str())).cloned().collect()
}
