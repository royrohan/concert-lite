//! `STORE_KIND=postgres|dsql`: the SQL of `JdbcStateStore` (the DSQL subset: multi-row VALUES and IN lists,
//! no arrays, short statements retried on OCC conflicts) over tokio-postgres.
//!
//! Same variables as the Java store: `STORE_JDBC_URL` (`jdbc:postgresql://host:port/db`, or for DSQL
//! `jdbc:aws-dsql:postgresql://<cluster-endpoint>/postgres`), `STORE_USER`, `STORE_PASSWORD`,
//! `STORE_POOL_SIZE`. DSQL connects over TLS with an IAM auth token minted per connection (tokens are
//! short-lived; connections are recycled after 50 minutes, before DSQL's 60-minute limit).

use super::{CoordinatorStore, distinct};
use crate::config::Env;
use crate::model::TraceRow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool::managed::{self, Metrics, RecycleError, RecycleResult};
use std::collections::HashSet;
use std::time::Duration;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};
use tracing::debug;

const SCHEMA: &str = include_str!("../../../store/src/main/resources/db/schema.sql");
const MAX_ATTEMPTS: u32 = 6;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Target {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub ssl: bool,
}

/// Parses the JDBC URL the Java store uses (`jdbc:[aws-dsql:]postgresql://host[:port]/db[?params]`).
pub(crate) fn parse_jdbc_url(url: &str) -> anyhow::Result<Target> {
    let rest = url
        .strip_prefix("jdbc:")
        .unwrap_or(url)
        .trim_start_matches("aws-dsql:")
        .strip_prefix("postgresql://")
        .ok_or_else(|| anyhow::anyhow!("STORE_JDBC_URL must look like jdbc:postgresql://host:port/db, got {url}"))?;
    let (hostport, rest) = rest.split_once('/').unwrap_or((rest, "postgres"));
    let (database, params) = rest.split_once('?').unwrap_or((rest, ""));
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_owned(), p.parse()?),
        None => (hostport.to_owned(), 5432),
    };
    let ssl = params.split('&').any(|p| p == "sslmode=require" || p == "sslmode=verify-full" || p == "ssl=true");
    Ok(Target { host, port, database: if database.is_empty() { "postgres".into() } else { database.into() }, ssl })
}

struct PgManager {
    target: Target,
    user: String,
    password: Option<String>,
    dsql: Option<(aws_sdk_dsql::auth_token::AuthTokenGenerator, aws_config::SdkConfig)>,
    tls: Option<tokio_postgres_rustls::MakeRustlsConnect>,
    max_lifetime: Duration,
}

impl managed::Manager for PgManager {
    type Type = Client;
    type Error = anyhow::Error;

    async fn create(&self) -> anyhow::Result<Client> {
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&self.target.host)
            .port(self.target.port)
            .dbname(&self.target.database)
            .user(&self.user)
            .application_name("concert-coordinator")
            .connect_timeout(Duration::from_secs(5));
        if let Some((signer, sdk)) = &self.dsql {
            let token = if self.user == "admin" {
                signer.db_connect_admin_auth_token(sdk).await
            } else {
                signer.db_connect_auth_token(sdk).await
            }
            .map_err(|e| anyhow::anyhow!("DSQL auth token: {e}"))?;
            cfg.password(token.as_str());
        } else if let Some(p) = &self.password {
            cfg.password(p);
        }
        let client = match &self.tls {
            Some(tls) => {
                cfg.ssl_mode(tokio_postgres::config::SslMode::Require);
                let (client, conn) = cfg.connect(tls.clone()).await?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        debug!("postgres connection closed: {e}");
                    }
                });
                client
            }
            None => {
                let (client, conn) = cfg.connect(NoTls).await?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        debug!("postgres connection closed: {e}");
                    }
                });
                client
            }
        };
        Ok(client)
    }

    async fn recycle(&self, client: &mut Client, metrics: &Metrics) -> RecycleResult<anyhow::Error> {
        if client.is_closed() {
            return Err(RecycleError::message("connection closed"));
        }
        if metrics.age() > self.max_lifetime {
            return Err(RecycleError::message("connection reached its max lifetime"));
        }
        Ok(())
    }
}

type Pool = managed::Pool<PgManager>;

pub struct PostgresStore {
    pool: Pool,
    kind: &'static str,
}

impl PostgresStore {
    pub async fn from_env(env: &Env, dsql: bool, init_schema: bool) -> anyhow::Result<Self> {
        let target = parse_jdbc_url(&env.str("STORE_JDBC_URL", "jdbc:postgresql://localhost:5433/concert"))?;
        let user = env.str("STORE_USER", if dsql { "admin" } else { "concert" });
        let tls = if dsql || target.ssl {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let cfg = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
            Some(tokio_postgres_rustls::MakeRustlsConnect::new(cfg))
        } else {
            None
        };
        let signer = if dsql {
            let sdk = aws_config::load_from_env().await;
            let region = sdk.region().cloned().unwrap_or_else(|| aws_config::Region::new(env.str("AWS_REGION", "us-east-1")));
            let cfg = aws_sdk_dsql::auth_token::Config::builder()
                .hostname(&target.host)
                .region(region)
                .build()
                .map_err(|e| anyhow::anyhow!("DSQL signer: {e}"))?;
            Some((aws_sdk_dsql::auth_token::AuthTokenGenerator::new(cfg), sdk))
        } else {
            None
        };
        let manager = PgManager {
            target,
            user,
            password: if dsql { None } else { Some(env.str("STORE_PASSWORD", "concert")) },
            dsql: signer,
            tls,
            max_lifetime: Duration::from_secs(if dsql { 50 * 60 } else { 30 * 60 }),
        };
        let pool = Pool::builder(manager)
            .max_size(env.usize("STORE_POOL_SIZE", 32))
            .runtime(deadpool::Runtime::Tokio1)
            .create_timeout(Some(Duration::from_secs(10)))
            .wait_timeout(Some(Duration::from_secs(30)))
            .build()?;
        let store = PostgresStore { pool, kind: if dsql { "dsql" } else { "postgres" } };
        if init_schema {
            store.apply_schema(dsql).await?;
        }
        Ok(store)
    }

    async fn client(&self) -> anyhow::Result<managed::Object<PgManager>> {
        self.pool.get().await.map_err(|e| anyhow::anyhow!("store connection: {e}"))
    }

    /// `SchemaInitializer`: db/schema.sql one statement at a time (DSQL allows one DDL per transaction);
    /// "already exists" from a concurrent initializer is fine.
    async fn apply_schema(&self, dsql: bool) -> anyhow::Result<()> {
        let sql = SCHEMA.replace("/*ASYNC*/", if dsql { "ASYNC" } else { "" }).replace("/*JSON*/", if dsql { "text" } else { "jsonb" });
        let stripped: String = sql.lines().map(|l| l.split("--").next().unwrap_or("")).collect::<Vec<_>>().join("\n");
        let client = self.client().await?;
        for stmt in stripped.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            if let Err(e) = client.batch_execute(stmt).await {
                let exists = e.code().is_some_and(|c| {
                    *c == SqlState::DUPLICATE_TABLE || *c == SqlState::DUPLICATE_OBJECT || *c == SqlState::UNIQUE_VIOLATION
                });
                if !exists {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// `OccRetry`: DSQL's optimistic concurrency (and Postgres serialization failures / deadlocks) fail
    /// with 40001 / 40P01 and are retried with jittered backoff.
    async fn retry<T, F, Fut>(&self, mut call: F) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let mut attempt = 1;
        loop {
            match call().await {
                Ok(v) => return Ok(v),
                Err(e) if attempt < MAX_ATTEMPTS && is_retryable(&e) => {
                    let base = (2u64 << attempt).min(200);
                    let wait = rand::random_range(base / 2..=base);
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn is_retryable(e: &anyhow::Error) -> bool {
    e.downcast_ref::<tokio_postgres::Error>()
        .and_then(|pg| pg.code())
        .is_some_and(|c| *c == SqlState::T_R_SERIALIZATION_FAILURE || *c == SqlState::T_R_DEADLOCK_DETECTED)
}

/// `$1, $2, ...` groups: `tuples(2, 3, 0)` = `($1, $2), ($3, $4), ($5, $6)`.
fn tuples(arity: usize, count: usize, offset: usize) -> String {
    (0..count)
        .map(|i| {
            let cols: Vec<String> = (0..arity).map(|c| format!("${}", offset + i * arity + c + 1)).collect();
            format!("({})", cols.join(", "))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(count: usize, offset: usize) -> String {
    (1..=count).map(|i| format!("${}", offset + i)).collect::<Vec<_>>().join(", ")
}

fn ts(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

#[async_trait]
impl CoordinatorStore for PostgresStore {
    fn kind(&self) -> &str {
        self.kind
    }

    async fn claim_for_dispatch(&self, event_ids: &[String], now: i64) -> anyhow::Result<HashSet<String>> {
        let ids = distinct(event_ids);
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let received = ts(now);
        self.retry(|| async {
            let client = self.client().await?;
            let insert = format!(
                "INSERT INTO processed_event (event_id, received_at, status) VALUES {} ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
                (0..ids.len()).map(|i| format!("(${}, ${}, 'RECEIVED')", 2 * i + 1, 2 * i + 2)).collect::<Vec<_>>().join(", ")
            );
            let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(ids.len() * 2);
            for id in &ids {
                params.push(id);
                params.push(&received);
            }
            let mut claimed: HashSet<String> = client.query(&insert, &params).await?.iter().map(|r| r.get(0)).collect();
            if claimed.len() < ids.len() {
                // Seen before: re-dispatch only what a previous attempt never got past RECEIVED.
                let seen: Vec<&String> = ids.iter().filter(|id| !claimed.contains(*id)).collect();
                let select = format!(
                    "SELECT event_id FROM processed_event WHERE status = 'RECEIVED' AND event_id IN ({})",
                    placeholders(seen.len(), 0)
                );
                let params: Vec<&(dyn ToSql + Sync)> = seen.iter().map(|s| *s as &(dyn ToSql + Sync)).collect();
                claimed.extend(client.query(&select, &params).await?.iter().map(|r| r.get::<_, String>(0)));
            }
            Ok(claimed)
        })
        .await
    }

    async fn mark_dispatched(&self, event_ids: &[String]) -> anyhow::Result<()> {
        let ids = distinct(event_ids);
        if ids.is_empty() {
            return Ok(());
        }
        self.retry(|| async {
            let client = self.client().await?;
            let sql = format!("UPDATE processed_event SET status = 'DISPATCHED' WHERE event_id IN ({})", placeholders(ids.len(), 0));
            let params: Vec<&(dyn ToSql + Sync)> = ids.iter().map(|s| s as &(dyn ToSql + Sync)).collect();
            client.execute(&sql, &params).await?;
            Ok(())
        })
        .await
    }

    async fn load_checkpoint(&self, stream: &str, shard: &str) -> anyhow::Result<Option<String>> {
        self.retry(|| async {
            let client = self.client().await?;
            let row = client
                .query_opt("SELECT seq_no FROM shard_checkpoint WHERE stream = $1 AND shard_id = $2", &[&stream, &shard])
                .await?;
            Ok(row.map(|r| r.get(0)))
        })
        .await
    }

    async fn save_checkpoint(&self, stream: &str, shard: &str, seq: &str) -> anyhow::Result<()> {
        self.retry(|| async {
            let client = self.client().await?;
            client
                .execute(
                    "INSERT INTO shard_checkpoint (stream, shard_id, seq_no, updated_at) VALUES ($1, $2, $3, now()) \
                     ON CONFLICT (stream, shard_id) DO UPDATE SET seq_no = EXCLUDED.seq_no, updated_at = now()",
                    &[&stream, &shard, &seq],
                )
                .await?;
            Ok(())
        })
        .await
    }

    async fn append_trace(&self, rows: &[TraceRow]) -> anyhow::Result<()> {
        // DSQL caps rows per statement; 500 matches the Java chunking.
        for chunk in rows.chunks(500) {
            self.retry(|| async {
                let client = self.client().await?;
                let sql = format!(
                    "INSERT INTO event_trace (trace_id, event_id, ts, stage, workflow_id, lock_key, detail) VALUES {}",
                    tuples(7, chunk.len(), 0)
                );
                let ids: Vec<String> = chunk.iter().map(|_| uuid::Uuid::new_v4().to_string()).collect();
                let times: Vec<DateTime<Utc>> = chunk.iter().map(|r| ts(r.ts_millis)).collect();
                let stages: Vec<&'static str> = chunk.iter().map(|r| r.stage.name()).collect();
                let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(chunk.len() * 7);
                for (i, r) in chunk.iter().enumerate() {
                    params.push(&ids[i]);
                    params.push(&r.event_id);
                    params.push(&times[i]);
                    params.push(&stages[i]);
                    params.push(&r.workflow_id);
                    params.push(&r.lock_key);
                    params.push(&r.detail);
                }
                client.execute(&sql, &params).await?;
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    async fn sweep_processed(&self, older_than: i64, batch_size: usize) -> anyhow::Result<u64> {
        let cutoff = ts(older_than);
        let limit = batch_size.min(2500) as i64;
        let mut total = 0;
        loop {
            let n = self
                .retry(|| async {
                    let client = self.client().await?;
                    Ok(client
                        .execute(
                            "DELETE FROM processed_event WHERE event_id IN \
                             (SELECT event_id FROM processed_event WHERE received_at < $1 LIMIT $2)",
                            &[&cutoff, &limit],
                        )
                        .await?)
                })
                .await?;
            total += n;
            if n == 0 {
                return Ok(total);
            }
        }
    }

    async fn processed_status(&self, event_id: &str) -> anyhow::Result<Option<String>> {
        let client = self.client().await?;
        let row = client.query_opt("SELECT status FROM processed_event WHERE event_id = $1", &[&event_id]).await?;
        Ok(row.map(|r| r.get(0)))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jdbc_urls() {
        assert_eq!(
            parse_jdbc_url("jdbc:postgresql://localhost:5433/concert").unwrap(),
            Target { host: "localhost".into(), port: 5433, database: "concert".into(), ssl: false }
        );
        assert_eq!(
            parse_jdbc_url("jdbc:aws-dsql:postgresql://abc.dsql.us-east-1.on.aws/postgres").unwrap(),
            Target { host: "abc.dsql.us-east-1.on.aws".into(), port: 5432, database: "postgres".into(), ssl: false }
        );
        assert!(parse_jdbc_url("jdbc:postgresql://h:1/db?sslmode=require").unwrap().ssl);
        assert!(parse_jdbc_url("mysql://x").is_err());
    }

    #[test]
    fn placeholder_lists() {
        assert_eq!(tuples(2, 2, 0), "($1, $2), ($3, $4)");
        assert_eq!(placeholders(3, 0), "$1, $2, $3");
    }
}
