//! `TraceWriter`: trace rows leave the hot path at once. Callers enqueue and return; one task writes a
//! multi-row insert per `flush` window (or per `max_batch` rows). Best effort: a full buffer drops rows
//! (counted) instead of slowing ingest. Sampling (`TRACE_SAMPLE`) matches the Java processes.

use crate::model::{Sampling, TraceRow};
use crate::store::Store;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Clone)]
pub struct TraceWriter {
    tx: mpsc::Sender<TraceRow>,
    sampling: Sampling,
    dropped: Arc<AtomicU64>,
}

pub struct TraceFlusher {
    handle: JoinHandle<()>,
}

impl TraceWriter {
    pub fn new(store: Store, sampling: Sampling) -> (Self, TraceFlusher) {
        Self::with_limits(store, sampling, 100_000, 500, Duration::from_millis(50))
    }

    pub fn with_limits(
        store: Store,
        sampling: Sampling,
        capacity: usize,
        max_batch: usize,
        flush: Duration,
    ) -> (Self, TraceFlusher) {
        let (tx, mut rx) = mpsc::channel::<TraceRow>(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_in_task = dropped.clone();
        let handle = tokio::spawn(async move {
            let mut batch = Vec::with_capacity(max_batch);
            while let Some(first) = rx.recv().await {
                batch.push(first);
                // Linger up to `flush` so a trickle of rows becomes one insert.
                let deadline = tokio::time::Instant::now() + flush;
                while batch.len() < max_batch {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(row)) => batch.push(row),
                        _ => break,
                    }
                }
                if let Err(e) = store.append_trace(&batch).await {
                    dropped_in_task.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    warn!("trace flush failed, dropped {} rows: {e:#}", batch.len());
                }
                batch.clear();
            }
        });
        (TraceWriter { tx, sampling, dropped }, TraceFlusher { handle })
    }

    pub fn add(&self, row: TraceRow) {
        if !self.sampling.sampled(&row.event_id) {
            return;
        }
        if self.tx.try_send(row).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_all(&self, rows: impl IntoIterator<Item = TraceRow>) {
        rows.into_iter().for_each(|r| self.add(r));
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl TraceFlusher {
    /// Waits (bounded) for the buffered rows once every writer handle is dropped.
    pub async fn close(self) {
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Stage;
    use crate::store::MemoryStore;

    #[tokio::test]
    async fn batches_and_flushes_on_close() {
        let mem = Arc::new(MemoryStore::default());
        let (w, f) = TraceWriter::with_limits(mem.clone(), Sampling::new(1.0), 10, 3, Duration::from_millis(5));
        for i in 0..7 {
            w.add(TraceRow::new(format!("e{i}"), i, Stage::Received, None, None, None));
        }
        drop(w);
        f.close().await;
        assert_eq!(mem.traces.lock().unwrap().len(), 7);
    }

    #[tokio::test]
    async fn full_buffer_drops_instead_of_blocking() {
        let mem = Arc::new(MemoryStore::default());
        let (w, _f) = TraceWriter::with_limits(mem, Sampling::new(1.0), 1, 1, Duration::from_secs(10));
        for i in 0..1000 {
            w.add(TraceRow::new(format!("e{i}"), i, Stage::Received, None, None, None));
        }
        assert!(w.dropped() > 0);
    }
}
