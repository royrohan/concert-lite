//! `STORE_KIND=dynamo`: the tables and items of `DynamoStateStore` (prefix `DYNAMO_TABLE_PREFIX`, default
//! `concert_`). Dedupe claim = conditional put per id, in parallel; on conflict the old item comes back on
//! the same call. Dedupe rows expire through native TTL on `expires_at` (`DEDUPE_TTL_HOURS`), so the sweep
//! is a no-op. `DYNAMO_ENDPOINT` points at LocalStack (static test credentials); unset for AWS.

use super::{CoordinatorStore, distinct};
use crate::config::Env;
use crate::model::TraceRow;
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, PutRequest, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TimeToLiveSpecification,
    WriteRequest,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, info};

const HOUR: i64 = 3_600_000;
const PARALLEL: usize = 64;

pub struct DynamoStore {
    ddb: Client,
    processed: String,
    checkpoints: String,
    states: String,
    traces: String,
    ttl_hours: i64,
}

fn s(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}

fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn attr_s(item: &HashMap<String, AttributeValue>, k: &str) -> Option<String> {
    item.get(k).and_then(|v| v.as_s().ok()).cloned()
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The Java sort key of a trace item: `%013d#STAGE#<8 chars of a uuid>`.
pub(crate) fn trace_sort_key(ts: i64, stage: &str, uuid: &str) -> String {
    format!("{ts:013}#{stage}#{}", &uuid[..8.min(uuid.len())])
}

pub(crate) fn recent_bucket(ts: i64) -> String {
    format!("r#{}", ts.div_euclid(HOUR))
}

impl DynamoStore {
    pub async fn from_env(env: &Env, init: bool) -> anyhow::Result<Self> {
        let region = aws_config::Region::new(env.str("AWS_REGION", "us-east-1"));
        let mut loader = aws_config::from_env().region(region);
        if let Some(endpoint) = env.get("DYNAMO_ENDPOINT") {
            loader = loader
                .endpoint_url(endpoint)
                .credentials_provider(aws_sdk_dynamodb::config::Credentials::new("test", "test", None, None, "static"));
        }
        let sdk = loader.load().await;
        let prefix = env.str("DYNAMO_TABLE_PREFIX", "concert_");
        let store = DynamoStore {
            ddb: Client::new(&sdk),
            processed: format!("{prefix}processed_event"),
            checkpoints: format!("{prefix}shard_checkpoint"),
            states: format!("{prefix}sm_state"),
            traces: format!("{prefix}event_trace"),
            ttl_hours: env.i64("DEDUPE_TTL_HOURS", 24),
        };
        if init {
            store.create_tables().await?;
        }
        Ok(store)
    }

    fn dedupe_item(&self, id: &str, status: &str, now: i64) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("event_id".to_owned(), s(id)),
            ("status".to_owned(), s(status)),
            ("received_at".to_owned(), n(now)),
            ("expires_at".to_owned(), n(now / 1000 + self.ttl_hours * 3600)),
        ])
    }

    /// BatchWriteItem in chunks of 25, chunks in parallel, unprocessed items retried with backoff.
    async fn batch_put(&self, table: &str, items: Vec<HashMap<String, AttributeValue>>) -> anyhow::Result<()> {
        let chunks: Vec<Vec<HashMap<String, AttributeValue>>> = items.chunks(25).map(<[_]>::to_vec).collect();
        stream::iter(chunks)
            .map(|chunk| async move {
                let mut pending: Vec<WriteRequest> = chunk
                    .into_iter()
                    .map(|it| WriteRequest::builder().put_request(PutRequest::builder().set_item(Some(it)).build()?).build_ok())
                    .collect::<anyhow::Result<_>>()?;
                let mut attempt = 0u32;
                while !pending.is_empty() {
                    let out = self.ddb.batch_write_item().request_items(table, pending).send().await?;
                    pending = out.unprocessed_items.and_then(|mut m| m.remove(table)).unwrap_or_default();
                    if !pending.is_empty() {
                        tokio::time::sleep(Duration::from_millis((20u64 << attempt.min(6)).min(1000))).await;
                        attempt += 1;
                    }
                }
                anyhow::Ok(())
            })
            .buffer_unordered(PARALLEL)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// `createTables`: on-demand tables (same keys and GSIs as Java) and TTL on dedupe rows. Idempotent.
    async fn create_tables(&self) -> anyhow::Result<()> {
        let key = |name: &str, t: KeyType| KeySchemaElement::builder().attribute_name(name).key_type(t).build();
        let attr = |name: &str| AttributeDefinition::builder().attribute_name(name).attribute_type(ScalarAttributeType::S).build();
        let gsi = |name: &str, hash: &str| -> anyhow::Result<GlobalSecondaryIndex> {
            Ok(GlobalSecondaryIndex::builder()
                .index_name(name)
                .key_schema(key(hash, KeyType::Hash)?)
                .key_schema(key("sk", KeyType::Range)?)
                .projection(Projection::builder().projection_type(ProjectionType::All).build())
                .build()?)
        };
        self.create(&self.processed, vec![key("event_id", KeyType::Hash)?], vec![attr("event_id")?], vec![]).await?;
        self.create(&self.checkpoints, vec![key("id", KeyType::Hash)?], vec![attr("id")?], vec![]).await?;
        self.create(&self.states, vec![key("workflow_id", KeyType::Hash)?], vec![attr("workflow_id")?], vec![]).await?;
        self.create(
            &self.traces,
            vec![key("event_id", KeyType::Hash)?, key("sk", KeyType::Range)?],
            vec![attr("event_id")?, attr("sk")?, attr("workflow_id")?, attr("recent_bucket")?],
            vec![gsi("by_workflow", "workflow_id")?, gsi("by_recent", "recent_bucket")?],
        )
        .await?;
        let ttl = TimeToLiveSpecification::builder().attribute_name("expires_at").enabled(true).build()?;
        if let Err(e) = self.ddb.update_time_to_live().table_name(&self.processed).time_to_live_specification(ttl).send().await {
            debug!("TTL on {}: {e}", self.processed); // already enabled, or not supported by the emulator
        }
        Ok(())
    }

    async fn create(
        &self,
        table: &str,
        keys: Vec<KeySchemaElement>,
        attrs: Vec<AttributeDefinition>,
        gsis: Vec<GlobalSecondaryIndex>,
    ) -> anyhow::Result<()> {
        if self.ddb.describe_table().table_name(table).send().await.is_ok() {
            return Ok(());
        }
        let mut req = self
            .ddb
            .create_table()
            .table_name(table)
            .set_key_schema(Some(keys))
            .set_attribute_definitions(Some(attrs))
            .billing_mode(BillingMode::PayPerRequest);
        if !gsis.is_empty() {
            req = req.set_global_secondary_indexes(Some(gsis));
        }
        if let Err(e) = req.send().await {
            let in_use = e.as_service_error().is_some_and(|se| se.is_resource_in_use_exception());
            if !in_use {
                return Err(e.into());
            }
        }
        for _ in 0..120 {
            let active = self
                .ddb
                .describe_table()
                .table_name(table)
                .send()
                .await
                .ok()
                .and_then(|d| d.table)
                .and_then(|t| t.table_status)
                .is_some_and(|st| st.as_str() == "ACTIVE");
            if active {
                info!("created DynamoDB table {table}");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("table {table} did not become ACTIVE")
    }
}

trait BuildOk {
    fn build_ok(self) -> anyhow::Result<WriteRequest>;
}

impl BuildOk for aws_sdk_dynamodb::types::builders::WriteRequestBuilder {
    fn build_ok(self) -> anyhow::Result<WriteRequest> {
        Ok(self.build())
    }
}

#[async_trait]
impl CoordinatorStore for DynamoStore {
    fn kind(&self) -> &str {
        "dynamo"
    }

    async fn claim_for_dispatch(&self, event_ids: &[String], now: i64) -> anyhow::Result<HashSet<String>> {
        let claimed: Vec<Option<String>> = stream::iter(distinct(event_ids))
            .map(|id| async move {
                let put = self
                    .ddb
                    .put_item()
                    .table_name(&self.processed)
                    .set_item(Some(self.dedupe_item(&id, "RECEIVED", now)))
                    .condition_expression("attribute_not_exists(event_id)")
                    .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
                    .send()
                    .await;
                match put {
                    Ok(_) => Ok(Some(id)),
                    Err(SdkError::ServiceError(se)) if matches!(se.err(), PutItemError::ConditionalCheckFailedException(_)) => {
                        // Seen before: re-dispatch only if a previous attempt never got past RECEIVED.
                        let old = match se.into_err() {
                            PutItemError::ConditionalCheckFailedException(c) => c.item,
                            _ => None,
                        };
                        let status = match old {
                            Some(item) => attr_s(&item, "status"),
                            None => self.processed_status(&id).await?,
                        };
                        Ok((status.as_deref() == Some("RECEIVED")).then_some(id))
                    }
                    Err(e) => Err(anyhow::Error::from(e)),
                }
            })
            .buffer_unordered(PARALLEL)
            .try_collect()
            .await?;
        Ok(claimed.into_iter().flatten().collect())
    }

    async fn mark_dispatched(&self, event_ids: &[String]) -> anyhow::Result<()> {
        let now = now_millis();
        let items = distinct(event_ids).iter().map(|id| self.dedupe_item(id, "DISPATCHED", now)).collect();
        self.batch_put(&self.processed, items).await
    }

    async fn load_checkpoint(&self, stream: &str, shard: &str) -> anyhow::Result<Option<String>> {
        let out = self
            .ddb
            .get_item()
            .table_name(&self.checkpoints)
            .key("id", s(format!("{stream}#{shard}")))
            .consistent_read(true)
            .send()
            .await?;
        Ok(out.item.and_then(|i| attr_s(&i, "seq_no")))
    }

    async fn save_checkpoint(&self, stream: &str, shard: &str, seq: &str) -> anyhow::Result<()> {
        self.ddb
            .put_item()
            .table_name(&self.checkpoints)
            .item("id", s(format!("{stream}#{shard}")))
            .item("seq_no", s(seq))
            .item("updated_at", n(now_millis()))
            .send()
            .await?;
        Ok(())
    }

    async fn append_trace(&self, rows: &[TraceRow]) -> anyhow::Result<()> {
        let items = rows
            .iter()
            .map(|t| {
                let mut item = HashMap::from([
                    ("event_id".to_owned(), s(&t.event_id)),
                    ("sk".to_owned(), s(trace_sort_key(t.ts_millis, t.stage.name(), &uuid::Uuid::new_v4().to_string()))),
                    ("ts".to_owned(), n(t.ts_millis)),
                    ("stage".to_owned(), s(t.stage.name())),
                ]);
                for (k, v) in [("workflow_id", &t.workflow_id), ("lock_key", &t.lock_key), ("detail", &t.detail)] {
                    if let Some(v) = v {
                        item.insert(k.to_owned(), s(v));
                    }
                }
                if t.stage.is_terminal() {
                    item.insert("recent_bucket".to_owned(), s(recent_bucket(t.ts_millis)));
                }
                item
            })
            .collect();
        self.batch_put(&self.traces, items).await
    }

    async fn sweep_processed(&self, _older_than: i64, _batch: usize) -> anyhow::Result<u64> {
        Ok(0) // native TTL on expires_at
    }

    async fn processed_status(&self, event_id: &str) -> anyhow::Result<Option<String>> {
        let out = self
            .ddb
            .get_item()
            .table_name(&self.processed)
            .key("event_id", s(event_id))
            .consistent_read(true)
            .send()
            .await?;
        Ok(out.item.and_then(|i| attr_s(&i, "status")))
    }
}

impl DynamoStore {
    /// The entity table name (created with the others so a Rust-only deployment leaves nothing missing).
    pub fn states_table(&self) -> &str {
        &self.states
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_match_java_format() {
        assert_eq!(trace_sort_key(42, "DONE", "0123456789abcdef"), "0000000000042#DONE#01234567");
        assert_eq!(recent_bucket(7_200_001), "r#2");
    }
}
