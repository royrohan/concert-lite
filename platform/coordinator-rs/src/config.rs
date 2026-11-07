//! Environment variables, read the way `io.concert.common.Env` reads them: unset or blank means the default.

use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct Env {
    overrides: HashMap<String, String>,
}

impl Env {
    pub fn process() -> Self {
        Env::default()
    }

    /// For tests: values that win over the process environment.
    pub fn with(mut self, key: &str, value: &str) -> Self {
        self.overrides.insert(key.to_owned(), value.to_owned());
        self
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.overrides
            .get(key)
            .cloned()
            .or_else(|| std::env::var(key).ok())
            .filter(|v| !v.trim().is_empty())
    }

    pub fn str(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or_else(|| default.to_owned())
    }

    pub fn i64(&self, key: &str, default: i64) -> i64 {
        self.get(key).and_then(|v| v.trim().parse().ok()).unwrap_or(default)
    }

    pub fn usize(&self, key: &str, default: usize) -> usize {
        self.get(key).and_then(|v| v.trim().parse().ok()).unwrap_or(default)
    }

    pub fn f64(&self, key: &str, default: f64) -> f64 {
        self.get(key).and_then(|v| v.trim().parse().ok()).unwrap_or(default)
    }

    pub fn bool(&self, key: &str, default: bool) -> bool {
        self.get(key).map(|v| v.trim().eq_ignore_ascii_case("true")).unwrap_or(default)
    }

    pub fn millis(&self, key: &str, default: i64) -> Duration {
        Duration::from_millis(self.i64(key, default).max(0) as u64)
    }

    /// `Failover.workflowTaskTimeout()`.
    pub fn workflow_task_timeout(&self) -> Duration {
        self.millis("WORKFLOW_TASK_TIMEOUT_MS", 2000)
    }

    /// `Failover.stickyScheduleToStart()`.
    pub fn sticky_schedule_to_start(&self) -> Duration {
        self.millis("STICKY_SCHEDULE_TO_START_MS", 1000)
    }

    /// `Failover.shardHeartbeatTimeoutSeconds()`.
    pub fn shard_heartbeat_timeout_sec(&self) -> i64 {
        self.i64("SHARD_HEARTBEAT_TIMEOUT_SEC", 3)
    }
}
