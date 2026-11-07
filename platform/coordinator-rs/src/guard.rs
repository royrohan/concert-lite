//! Startup guard against mixing coordinator implementations on one task queue.
//!
//! The Java and Rust coordinators share every external interface, but neither can replay the other's workflow
//! histories: a lock run written by one and picked up by the other fails with a nondeterminism error and its key
//! stops moving. Before polling, the Rust coordinator looks at the running workflows on its queue and refuses to
//! start if any was last worked on by another implementation (worker identity not ours). Terminating them is not
//! an option: their queued requests are already marked DISPATCHED in the store. Drain the other coordinator first
//! (README, "Switching an existing deployment"), or set `COORDINATOR_ALLOW_FOREIGN_RUNS=true` to start anyway.

use futures::StreamExt;
use temporalio_client::{Client, UntypedWorkflow, WorkflowFetchHistoryOptions, WorkflowListOptions};
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;

/// Prefix of this implementation's worker identity (`main.rs` sets it).
pub const IDENTITY_PREFIX: &str = "concert-coordinator-rs@";

/// Whether a worker identity belongs to this implementation.
pub fn is_ours(identity: &str) -> bool {
    identity.starts_with(IDENTITY_PREFIX)
}

/// Running workflows on `queue` whose last completed workflow task came from another implementation, as
/// `(workflow id, identity)`; looks at most at `limit` runs.
pub async fn foreign_runs(client: &Client, queue: &str, limit: usize) -> anyhow::Result<Vec<(String, String)>> {
    let query = format!("TaskQueue=\"{queue}\" AND ExecutionStatus=\"Running\"");
    let mut runs = client.list_workflows(query, WorkflowListOptions::builder().limit(limit).build());
    let mut foreign = Vec::new();
    while let Some(run) = runs.next().await {
        let run = run?;
        let handle = client.get_workflow_handle::<UntypedWorkflow>(run.id().to_owned());
        let mut history = handle.fetch_history(WorkflowFetchHistoryOptions::default());
        let mut last_identity: Option<String> = None;
        while let Some(event) = history.next().await {
            if let Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) = event?.attributes {
                last_identity = Some(a.identity);
            }
        }
        if let Some(identity) = last_identity.filter(|i| !is_ours(i)) {
            foreign.push((run.id().to_owned(), identity));
        }
    }
    Ok(foreign)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities() {
        assert!(is_ours("concert-coordinator-rs@host-1"));
        assert!(!is_ours("12345@host-1"));
    }
}
