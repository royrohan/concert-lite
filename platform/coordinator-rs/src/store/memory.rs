//! In-process store (`STORE_KIND=memory`, and unit tests): same semantics as the real backends.

use super::{CoordinatorStore, distinct};
use crate::model::TraceRow;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Default)]
pub struct MemoryStore {
    processed: Mutex<HashMap<String, (String, i64)>>,
    checkpoints: Mutex<HashMap<(String, String), String>>,
    pub traces: Mutex<Vec<TraceRow>>,
}

#[async_trait]
impl CoordinatorStore for MemoryStore {
    fn kind(&self) -> &str {
        "memory"
    }

    async fn claim_for_dispatch(&self, event_ids: &[String], now: i64) -> anyhow::Result<HashSet<String>> {
        let mut p = self.processed.lock().unwrap();
        let mut claimed = HashSet::new();
        for id in distinct(event_ids) {
            match p.get(&id) {
                None => {
                    p.insert(id.clone(), ("RECEIVED".into(), now));
                    claimed.insert(id);
                }
                Some((status, _)) if status == "RECEIVED" => {
                    claimed.insert(id);
                }
                Some(_) => {}
            }
        }
        Ok(claimed)
    }

    async fn mark_dispatched(&self, event_ids: &[String]) -> anyhow::Result<()> {
        let mut p = self.processed.lock().unwrap();
        for id in event_ids {
            if let Some(e) = p.get_mut(id) {
                e.0 = "DISPATCHED".into();
            }
        }
        Ok(())
    }

    async fn load_checkpoint(&self, stream: &str, shard: &str) -> anyhow::Result<Option<String>> {
        Ok(self.checkpoints.lock().unwrap().get(&(stream.to_owned(), shard.to_owned())).cloned())
    }

    async fn save_checkpoint(&self, stream: &str, shard: &str, seq: &str) -> anyhow::Result<()> {
        self.checkpoints.lock().unwrap().insert((stream.to_owned(), shard.to_owned()), seq.to_owned());
        Ok(())
    }

    async fn append_trace(&self, rows: &[TraceRow]) -> anyhow::Result<()> {
        self.traces.lock().unwrap().extend_from_slice(rows);
        Ok(())
    }

    async fn sweep_processed(&self, older_than: i64, _batch: usize) -> anyhow::Result<u64> {
        let mut p = self.processed.lock().unwrap();
        let before = p.len();
        p.retain(|_, (_, at)| *at >= older_than);
        Ok((before - p.len()) as u64)
    }

    async fn processed_status(&self, event_id: &str) -> anyhow::Result<Option<String>> {
        Ok(self.processed.lock().unwrap().get(event_id).map(|(s, _)| s.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn claim_dedupes_and_redispatches_unfinished() {
        let s = MemoryStore::default();
        let ids: Vec<String> = ["a", "b", "a"].iter().map(|s| s.to_string()).collect();
        let c = s.claim_for_dispatch(&ids, 1).await.unwrap();
        assert_eq!(c.len(), 2);
        s.mark_dispatched(&["a".to_string()]).await.unwrap();
        let c = s.claim_for_dispatch(&ids, 2).await.unwrap();
        assert_eq!(c, HashSet::from(["b".to_string()]));
        assert_eq!(s.sweep_processed(2, 100).await.unwrap(), 2);
    }
}
