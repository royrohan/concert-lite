//! Client calls into Temporal on the coordinator's behalf (`EventRouting` and `DispatchActivitiesImpl` in
//! Java). The workflow and update names and their payloads are the Java contract: the coordinator calls
//! the Java `EntityWorkflow` and `EventProcessorWorkflow` by name, and Java workers call this coordinator's
//! `KeyLockWorkflow` the same way. Every call is idempotent: update ids come from request / event ids.

use crate::config::Env;
use crate::model::{
    EntityInit, EventEnvelope, EventOutcome, Flow, LockRequest, LockState, ProcessRequest, ProcessorState, TransitionResult,
    ids, queues,
};
use std::time::Duration;
use temporalio_client::errors::{WorkflowUpdateError, WorkflowUpdateWithStartError};
use temporalio_client::{
    Client, RpcOptions, WorkflowExecuteUpdateOptions, WorkflowIdConflictPolicy, WorkflowUpdateHandle,
    WorkflowUpdateWithStartOptions,
};
use temporalio_common::search_attributes::{SearchAttributeKey, SearchAttributes};
use temporalio_common::{HasWorkflowDefinition, UpdateDefinition, WorkflowDefinition};

/// Validator message of a duplicate `acquire` (`KeyLockWorkflow.DUPLICATE`); Java callers match on it.
pub const DUPLICATE: &str = "duplicate request";

macro_rules! java_workflow {
    ($ty:ident, $name:literal, $input:ty) => {
        #[doc = concat!("The `", $name, "` workflow type (Java contract).")]
        pub struct $ty;
        impl WorkflowDefinition for $ty {
            type Input = $input;
            type Output = ();
            fn name(&self) -> &str {
                $name
            }
        }
        impl HasWorkflowDefinition for $ty {
            type Run = $ty;
        }
    };
}

macro_rules! java_update {
    ($ty:ident, $wf:ty, $name:literal, $input:ty, $output:ty) => {
        #[doc = concat!("The `", $name, "` update (Java contract).")]
        pub struct $ty;
        impl UpdateDefinition for $ty {
            type Workflow = $wf;
            type Input = $input;
            type Output = $output;
            fn name(&self) -> &str {
                $name
            }
        }
    };
}

java_workflow!(EntityWorkflow, "EntityWorkflow", EntityInit);
java_update!(EntityHandle, EntityWorkflow, "handle", EventEnvelope, TransitionResult);
java_workflow!(EventProcessorWorkflow, "EventProcessorWorkflow", ProcessorState);
java_update!(ProcessorProcess, EventProcessorWorkflow, "process", ProcessRequest, EventOutcome);
java_update!(ProcessorSchedule, EventProcessorWorkflow, "schedule", EventEnvelope, EventOutcome);
java_workflow!(KeyLockWorkflowType, "KeyLockWorkflow", LockState);
java_update!(LockAcquire, KeyLockWorkflowType, "acquire", LockRequest, ());
java_update!(LockRelease, KeyLockWorkflowType, "release", String, ());

const SM_TYPE: SearchAttributeKey<String> = SearchAttributeKey::keyword("SmType");
const INSTANCE_KEY: SearchAttributeKey<String> = SearchAttributeKey::keyword("InstanceKey");
const LOCK_KEYS: SearchAttributeKey<Vec<String>> = SearchAttributeKey::keyword_list("LockKeys");

/// Why a routed call failed.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    /// The target's update validator (or handler) refused it: deterministic, retrying cannot help.
    #[error("rejected: {0}")]
    Rejected(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct Router {
    client: Client,
    flow: Flow,
    task_timeout: Duration,
    search_attributes: bool,
    lock_idle_seconds: i64,
    processor_idle_seconds: i64,
}

fn failure_message(e: &WorkflowUpdateError) -> Option<String> {
    match e {
        WorkflowUpdateError::Failed(f) => Some(f.message.clone()),
        _ => None,
    }
}

impl Router {
    pub fn new(client: Client, env: &Env, flow: Flow) -> Self {
        Router {
            client,
            flow,
            task_timeout: env.workflow_task_timeout(),
            search_attributes: env.bool("TEMPORAL_SEARCH_ATTRIBUTES", true),
            lock_idle_seconds: env.i64("LOCK_IDLE_SECONDS", 600),
            processor_idle_seconds: env.i64("EVENT_PROCESSOR_IDLE_SECONDS", 600),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    fn options(&self, task_queue: String, workflow_id: String, update_id: String, attrs: SearchAttributes) -> WorkflowUpdateWithStartOptions {
        WorkflowUpdateWithStartOptions::new(task_queue, workflow_id, WorkflowIdConflictPolicy::UseExisting)
            .task_timeout(self.task_timeout)
            .maybe_search_attributes((self.search_attributes).then_some(attrs))
            .update_id(update_id)
            .build()
    }

    fn entity_options(&self, e: &EventEnvelope) -> WorkflowUpdateWithStartOptions {
        self.options(
            queues::state_machine(&e.sm_type),
            ids::entity_key(&e.sm_type, &e.instance_key),
            e.event_id.clone(),
            SearchAttributes::new([SM_TYPE.value_set(e.sm_type.clone()), INSTANCE_KEY.value_set(e.instance_key.clone())]),
        )
    }

    /// UpdateWithStart `handle` on the entity, waiting for the transition to complete.
    pub async fn update_entity(&self, e: &EventEnvelope) -> Result<TransitionResult, RouteError> {
        let opts = self.entity_options(e);
        self.client
            .execute_update_with_start_workflow(EntityWorkflow, EntityInit::fresh(&e.sm_type, &e.instance_key), EntityHandle, e.clone(), opts)
            .await
            .map_err(|err| match err {
                WorkflowUpdateWithStartError::Update(u) => match failure_message(&u) {
                    Some(m) => RouteError::Rejected(m),
                    None => RouteError::Other(u.into()),
                },
                other => RouteError::Other(other.into()),
            })
    }

    /// Direct path: returns once the entity accepted the update (that fixes its order).
    pub async fn update_entity_accepted(
        &self,
        e: &EventEnvelope,
    ) -> anyhow::Result<WorkflowUpdateHandle<Client, TransitionResult>> {
        let opts = self.entity_options(e);
        Ok(self
            .client
            .start_update_with_start_workflow(EntityWorkflow, EntityInit::fresh(&e.sm_type, &e.instance_key), EntityHandle, e.clone(), opts)
            .await?)
    }

    /// UpdateWithStart `acquire` on `lock:<key>`, returning once the request is in the queue. Retries a few
    /// times when the lock run completes (idle) while the update is in flight.
    ///
    /// Returns false if the request was a duplicate (already queued or recently done).
    pub async fn enqueue_lock(&self, lock_key: &str, request: &LockRequest) -> anyhow::Result<bool> {
        let mut attempt = 1;
        loop {
            let opts = self.options(
                self.flow.orchestration_queue(),
                self.flow.lock_workflow_id(lock_key),
                format!("{}@{lock_key}", request.request_id),
                SearchAttributes::new([LOCK_KEYS.value_set(vec![lock_key.to_owned()])]),
            );
            let result = self
                .client
                .start_update_with_start_workflow(
                    KeyLockWorkflowType,
                    LockState::fresh(lock_key, self.lock_idle_seconds),
                    LockAcquire,
                    request.clone(),
                    opts,
                )
                .await;
            match result {
                Ok(_) => return Ok(true),
                Err(WorkflowUpdateWithStartError::Update(u)) if failure_message(&u).is_some_and(|m| m.contains(DUPLICATE)) => {
                    return Ok(false);
                }
                Err(e) if attempt < 5 => {
                    tracing::debug!("acquire {}@{lock_key} attempt {attempt}: {e}", request.request_id);
                    tokio::time::sleep(Duration::from_millis(20 * attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn processor_options(&self, e: &EventEnvelope, update_id: String) -> (ProcessorState, WorkflowUpdateWithStartOptions) {
        let key = e.first_lock_key();
        let state = ProcessorState::fresh(&e.sm_type, &key, self.processor_idle_seconds);
        let opts = self.options(
            queues::events(&e.sm_type),
            ids::processor(&e.sm_type, &key),
            update_id,
            SearchAttributes::new([SM_TYPE.value_set(e.sm_type.clone()), INSTANCE_KEY.value_set(key)]),
        );
        (state, opts)
    }

    /// UpdateWithStart `process` on the event's processor, waiting for the outcome.
    pub async fn process(&self, request: &ProcessRequest) -> Result<EventOutcome, RouteError> {
        let mut attempt = 1;
        loop {
            let (state, opts) = self.processor_options(&request.event, format!("process:{}", request.request_id));
            match self.client.execute_update_with_start_workflow(EventProcessorWorkflow, state, ProcessorProcess, request.clone(), opts).await {
                Ok(o) => return Ok(o),
                Err(WorkflowUpdateWithStartError::Update(u)) if failure_message(&u).is_some() => {
                    return Err(RouteError::Rejected(failure_message(&u).unwrap_or_default()));
                }
                Err(e) if attempt < 5 => {
                    tokio::time::sleep(Duration::from_millis(20 * attempt)).await;
                    attempt += 1;
                    tracing::debug!("process {} retry: {e}", request.request_id);
                }
                Err(e) => return Err(RouteError::Other(e.into())),
            }
        }
    }

    /// UpdateWithStart `schedule` on the event's processor (a future-dated event); returns once accepted.
    pub async fn schedule(&self, e: &EventEnvelope) -> anyhow::Result<()> {
        let mut attempt = 1;
        loop {
            let (state, opts) = self.processor_options(e, format!("schedule:{}", e.event_id));
            match self.client.start_update_with_start_workflow(EventProcessorWorkflow, state, ProcessorSchedule, e.clone(), opts).await {
                Ok(_) => return Ok(()),
                Err(WorkflowUpdateWithStartError::Update(u)) if failure_message(&u).is_some() => return Err(u.into()),
                Err(err) if attempt < 5 => {
                    tokio::time::sleep(Duration::from_millis(20 * attempt)).await;
                    attempt += 1;
                    tracing::debug!("schedule {} retry: {err}", e.event_id);
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    /// `release` on a running lock.
    pub async fn release(&self, lock_key: &str, request_id: &str) -> anyhow::Result<()> {
        let handle = self.client.get_workflow_handle::<KeyLockWorkflowType>(self.flow.lock_workflow_id(lock_key));
        handle.execute_update(LockRelease, request_id.to_owned(), WorkflowExecuteUpdateOptions::default()).await?;
        Ok(())
    }
}

/// Whether a routed call failed because the target workflow does not exist (or no longer runs).
pub fn is_not_found(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(c.downcast_ref::<WorkflowUpdateError>(), Some(WorkflowUpdateError::NotFound(_)))
            || c.downcast_ref::<temporalio_client::tonic::Status>().is_some_and(|s| s.code() == temporalio_client::tonic::Code::NotFound)
    })
}

/// Waits for the result of a direct-path update.
pub async fn await_result(handle: WorkflowUpdateHandle<Client, TransitionResult>) -> Result<TransitionResult, WorkflowUpdateError> {
    handle.get_result(RpcOptions::default()).await
}
