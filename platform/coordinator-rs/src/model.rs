//! The coordinator's side of the wire contract with the Java code (`core/common`, `io.concert.common` and
//! `io.concert.common.api`). Every type here is the JSON that the Java records produce and accept through
//! Temporal's default Jackson converter: field names are the record component names, and absent fields
//! read as Java defaults (null, 0, false). The Java side ignores unknown fields, and the event envelope
//! keeps any field this code does not know, so a newer producer's fields reach the workers unchanged.
//!
//! Two pieces of logic must give the same answers as Java because both sides compute them on the same
//! events: the lock keys of an event, in Java `String.compareTo` order (Java workers emit events into the
//! same lock chains; a different order could deadlock two chains), and trace sampling (`TraceSampling`).

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub const STYLE_ENTITY: &str = "entity";
pub const STYLE_EVENT: &str = "event";

/// Java `String.compareTo`: lexicographic over UTF-16 code units. It differs from Rust's byte order
/// only between supplementary characters and U+E000..U+FFFF, but lock order must match Java exactly.
pub fn java_cmp(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Java `String.hashCode`.
pub fn java_hash(s: &str) -> i32 {
    s.encode_utf16().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

/// Workflow ids (`WorkflowIds`).
pub mod ids {
    pub fn entity_key(sm_type: &str, instance_key: &str) -> String {
        format!("{sm_type}:{instance_key}")
    }
    pub fn lock(key: &str) -> String {
        format!("lock:{key}")
    }
    pub fn processor(domain: &str, key: &str) -> String {
        format!("evproc:{domain}:{key}")
    }
    pub fn ingest(stream: &str) -> String {
        format!("ingest:{stream}")
    }
}

/// Per-flow isolation (`Flows`). A flow is an ecosystem whose processes all set `CONCERT_FLOW=<flow>`; it gets its
/// own Kinesis stream, orchestration task queue and lock workflow ids, so two flows never share a shard, a poller
/// or a lock, even with the same lock key names. The empty flow is the legacy shared setup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Flow(String);

impl Flow {
    pub fn new(name: &str) -> Self {
        Flow(name.trim().to_owned())
    }

    pub fn name(&self) -> &str {
        &self.0
    }

    pub fn is_legacy(&self) -> bool {
        self.0.is_empty()
    }

    /// `concert-<flow>`, or `concert-events`.
    pub fn stream(&self) -> String {
        if self.is_legacy() { "concert-events".to_owned() } else { format!("concert-{}", self.0) }
    }

    /// `orchestration-<flow>`, or `orchestration`.
    pub fn orchestration_queue(&self) -> String {
        if self.is_legacy() { queues::ORCHESTRATION.to_owned() } else { format!("{}-{}", queues::ORCHESTRATION, self.0) }
    }

    /// `lock:<flow>:<key>`, or `lock:<key>`.
    pub fn lock_workflow_id(&self, key: &str) -> String {
        if self.is_legacy() { ids::lock(key) } else { format!("lock:{}:{key}", self.0) }
    }
}

/// Task queues (`TaskQueues`).
pub mod queues {
    pub const ORCHESTRATION: &str = "orchestration";
    pub fn state_machine(sm_type: &str) -> String {
        format!("sm-{sm_type}")
    }
    pub fn events(domain: &str) -> String {
        format!("ev-{domain}")
    }
}

fn null_as_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

/// `io.concert.common.EventEnvelope` as it arrives (from Kinesis, or inside another payload).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawEnvelope {
    event_id: Option<String>,
    sm_type: Option<String>,
    instance_key: Option<String>,
    event_type: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    lock_keys: Vec<String>,
    payload: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    source_ts_millis: i64,
    #[serde(default, deserialize_with = "null_as_default")]
    ingest_ts_millis: i64,
    style: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    scheduled_at_millis: i64,
    parent_event_id: Option<String>,
    causation_root: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

/// One business event (`io.concert.common.EventEnvelope`), normalized like the Java record's constructor:
/// style lower-cased (default entity), the instance key of an event-style event defaults to its id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "RawEnvelope")]
pub struct EventEnvelope {
    pub event_id: String,
    pub sm_type: String,
    pub instance_key: String,
    pub event_type: String,
    pub lock_keys: Vec<String>,
    pub payload: Option<String>,
    pub source_ts_millis: i64,
    pub ingest_ts_millis: i64,
    pub style: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub scheduled_at_millis: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_root: Option<String>,
    /// Fields this version does not know; written back unchanged.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl TryFrom<RawEnvelope> for EventEnvelope {
    type Error = String;

    fn try_from(r: RawEnvelope) -> Result<Self, String> {
        let event_id = r.event_id.ok_or("eventId")?;
        let sm_type = r.sm_type.ok_or("smType")?;
        let event_type = r.event_type.ok_or("eventType")?;
        let style = match r.style.as_deref().map(str::trim) {
            None | Some("") => STYLE_ENTITY.to_owned(),
            Some(s) => s.to_lowercase(),
        };
        if style != STYLE_ENTITY && style != STYLE_EVENT {
            return Err(format!("style must be {STYLE_ENTITY} or {STYLE_EVENT}: {style}"));
        }
        let instance_key = match r.instance_key {
            Some(k) => k,
            None if style == STYLE_EVENT => event_id.clone(),
            None => return Err("instanceKey".into()),
        };
        Ok(EventEnvelope {
            event_id,
            sm_type,
            instance_key,
            event_type,
            lock_keys: r.lock_keys,
            payload: r.payload,
            source_ts_millis: r.source_ts_millis,
            ingest_ts_millis: r.ingest_ts_millis,
            style,
            scheduled_at_millis: r.scheduled_at_millis,
            parent_event_id: r.parent_event_id,
            causation_root: r.causation_root,
            extra: r.extra,
        })
    }
}

impl EventEnvelope {
    pub fn is_event_style(&self) -> bool {
        self.style == STYLE_EVENT
    }

    /// `smType:instanceKey`: the entity workflow id and its lock key.
    pub fn entity_key(&self) -> String {
        ids::entity_key(&self.sm_type, &self.instance_key)
    }

    /// The keys the event is serialized against, distinct and in global (Java string) order. Entity style
    /// always includes the entity key; event style uses the declared keys, or `<domain>:<eventId>`.
    pub fn effective_lock_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.lock_keys.clone();
        if self.is_event_style() {
            if keys.is_empty() {
                keys.push(ids::entity_key(&self.sm_type, &self.event_id));
            }
        } else {
            keys.push(self.entity_key());
        }
        keys.sort_by(|a, b| java_cmp(a, b));
        keys.dedup();
        keys
    }

    pub fn first_lock_key(&self) -> String {
        self.effective_lock_keys().swap_remove(0)
    }

    pub fn is_single_key(&self) -> bool {
        self.effective_lock_keys().len() == 1
    }

    /// The workflow trace rows point at: the entity, or the processor for event style.
    pub fn trace_workflow_id(&self) -> String {
        if self.is_event_style() {
            ids::processor(&self.sm_type, &self.first_lock_key())
        } else {
            self.entity_key()
        }
    }

    pub fn with_ingest_ts(mut self, ts: i64) -> Self {
        self.ingest_ts_millis = ts;
        self
    }
}

/// `LockRequest`: an event travelling through its lock chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LockRequest {
    pub request_id: String,
    pub event: EventEnvelope,
    #[serde(default, deserialize_with = "null_as_default")]
    pub key_index: usize,
    #[serde(default, deserialize_with = "null_as_default")]
    pub enqueued_at_millis: i64,
    #[serde(default, deserialize_with = "null_as_default")]
    pub waited_ms: BTreeMap<String, i64>,
}

impl LockRequest {
    pub fn first(event: EventEnvelope, now: i64) -> Self {
        LockRequest { request_id: event.event_id.clone(), event, key_index: 0, enqueued_at_millis: now, waited_ms: BTreeMap::new() }
    }

    pub fn keys(&self) -> Vec<String> {
        self.event.effective_lock_keys()
    }

    pub fn is_last_key(&self) -> bool {
        self.key_index + 1 >= self.keys().len()
    }

    /// Waits so far including the current key.
    pub fn waits_including_current(&self, now: i64) -> BTreeMap<String, i64> {
        let keys = self.keys();
        let mut w = BTreeMap::new();
        for k in keys.iter().take(self.key_index) {
            w.insert(k.clone(), self.waited_ms.get(k).copied().unwrap_or(0));
        }
        if let Some(k) = keys.get(self.key_index) {
            w.insert(k.clone(), (now - self.enqueued_at_millis).max(0));
        }
        w
    }

    pub fn forwarded_to(&self, next_index: usize, now: i64) -> Self {
        LockRequest {
            request_id: self.request_id.clone(),
            event: self.event.clone(),
            key_index: next_index,
            enqueued_at_millis: now,
            waited_ms: self.waits_including_current(now),
        }
    }
}

/// `LockState`: input of a lock run, carried across continue-as-new.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LockState {
    pub lock_key: String,
    #[serde(default, deserialize_with = "null_as_default")]
    pub queue: Vec<LockRequest>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub head_forwarded_at_millis: i64,
    #[serde(default, deserialize_with = "null_as_default")]
    pub recent_request_ids: Vec<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub idle_seconds: i64,
    #[serde(default, deserialize_with = "null_as_default")]
    pub head_blocked: bool,
}

impl LockState {
    pub fn fresh(lock_key: &str, idle_seconds: i64) -> Self {
        LockState { lock_key: lock_key.to_owned(), idle_seconds, ..Default::default() }
    }
}

/// `LockSnapshot`: the `snapshot` query of a lock.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LockSnapshot {
    pub lock_key: String,
    pub holder: Option<String>,
    pub holder_state: Option<String>,
    pub waiting: Vec<String>,
    pub ops_this_run: i64,
}

/// `EntityInit`: start input of a (Java) entity workflow; the coordinator only ever starts fresh ones.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityInit {
    pub sm_type: String,
    pub instance_key: String,
    pub state: Option<String>,
    pub data: Option<String>,
    pub version: i64,
    pub recent: Vec<Value>,
    pub created_at_millis: i64,
}

impl EntityInit {
    pub fn fresh(sm_type: &str, instance_key: &str) -> Self {
        EntityInit {
            sm_type: sm_type.to_owned(),
            instance_key: instance_key.to_owned(),
            state: None,
            data: None,
            version: 0,
            recent: vec![],
            created_at_millis: 0,
        }
    }
}

/// `TransitionResult`: what the entity's `handle` update returns.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TransitionResult {
    pub event_id: Option<String>,
    pub from_state: Option<String>,
    pub to_state: Option<String>,
    pub accepted: bool,
    pub message: Option<String>,
    pub version: i64,
}

/// `ProcessorState`: start input of a (Java) event processor; the coordinator only starts fresh ones.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorState {
    pub domain: String,
    pub key: String,
    pub idle_seconds: i64,
    pub scheduled: Vec<Value>,
    pub blocked: Vec<Value>,
    pub parked: Vec<Value>,
    pub recent: Vec<Value>,
    pub recent_done: Vec<String>,
    pub last_version: i64,
}

impl ProcessorState {
    pub fn fresh(domain: &str, key: &str, idle_seconds: i64) -> Self {
        ProcessorState {
            domain: domain.to_owned(),
            key: key.to_owned(),
            idle_seconds,
            scheduled: vec![],
            blocked: vec![],
            parked: vec![],
            recent: vec![],
            recent_done: vec![],
            last_version: 0,
        }
    }
}

/// `ProcessRequest`: the last lock of an event-style chain asks the processor to apply the event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessRequest {
    pub request_id: String,
    pub event: EventEnvelope,
    pub lock_key: String,
    pub lock_wait_ms: BTreeMap<String, i64>,
}

/// `EventOutcome`: what the processor did. Only `status == ERROR_BLOCKING` matters to the coordinator.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct EventOutcome {
    pub event_id: Option<String>,
    pub status: Option<String>,
    pub detail: Option<String>,
    pub attempts: i64,
    #[serde(deserialize_with = "null_as_default")]
    pub children: Vec<String>,
}

impl EventOutcome {
    pub fn blocked(&self) -> bool {
        self.status.as_deref() == Some("ERROR_BLOCKING")
    }
}

/// `IngestConfig`: input of the ingest supervisor.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IngestConfig {
    pub stream: String,
    pub initial_position: String,
    pub shard_list_interval_sec: i64,
    pub consume_run_minutes: i64,
    #[serde(deserialize_with = "null_as_default")]
    pub consumed_shards: Vec<String>,
    pub heartbeat_timeout_sec: i64,
}

/// `IngestStatus`: the supervisor's `status` query.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestStatus {
    pub stream: String,
    pub active_shards: Vec<String>,
    pub consumed_shards: Vec<String>,
    pub consumer_restarts: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardInfo {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub adjacent_parent_shard_id: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardResult {
    pub shard_id: String,
    pub closed: bool,
    pub last_sequence_number: Option<String>,
    pub records_processed: i64,
}

/// `TraceRow.Stage`, stored by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Stage {
    Received,
    Dropped,
    LockWait,
    LockGranted,
    Transition,
    Rejected,
    Released,
    Done,
    Failed,
    Scheduled,
    Blocked,
    Parked,
    Emitted,
}

impl Stage {
    pub fn name(self) -> &'static str {
        match self {
            Stage::Received => "RECEIVED",
            Stage::Dropped => "DROPPED",
            Stage::LockWait => "LOCK_WAIT",
            Stage::LockGranted => "LOCK_GRANTED",
            Stage::Transition => "TRANSITION",
            Stage::Rejected => "REJECTED",
            Stage::Released => "RELEASED",
            Stage::Done => "DONE",
            Stage::Failed => "FAILED",
            Stage::Scheduled => "SCHEDULED",
            Stage::Blocked => "BLOCKED",
            Stage::Parked => "PARKED",
            Stage::Emitted => "EMITTED",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Stage::Done | Stage::Dropped | Stage::Failed | Stage::Rejected)
    }
}

/// `TraceRow`: one step of an event's life, written to `event_trace`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceRow {
    pub event_id: String,
    pub ts_millis: i64,
    pub stage: Stage,
    pub workflow_id: Option<String>,
    pub lock_key: Option<String>,
    pub detail: Option<String>,
}

impl TraceRow {
    pub fn new(
        event_id: impl Into<String>,
        ts_millis: i64,
        stage: Stage,
        workflow_id: Option<String>,
        lock_key: Option<String>,
        detail: Option<String>,
    ) -> Self {
        TraceRow { event_id: event_id.into(), ts_millis, stage, workflow_id, lock_key, detail }
    }
}

/// `TraceSampling`: the same events are sampled by every process (coordinator, Java workers).
#[derive(Clone, Copy, Debug)]
pub struct Sampling {
    rate_bp: i32,
}

impl Sampling {
    pub fn new(rate: f64) -> Self {
        Sampling { rate_bp: (rate.clamp(0.0, 1.0) * 10_000.0).round() as i32 }
    }

    pub fn sampled(&self, event_id: &str) -> bool {
        if self.rate_bp >= 10_000 {
            return true;
        }
        if self.rate_bp <= 0 {
            return false;
        }
        java_hash(event_id).wrapping_mul(0x9E37_79B1_u32 as i32).rem_euclid(10_000) < self.rate_bp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(json: &str) -> EventEnvelope {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn entity_event_includes_its_key_sorted_and_distinct() {
        let e = env(r#"{"eventId":"e1","smType":"order","instanceKey":"7","eventType":"pay",
            "lockKeys":["order:7","account:9"],"payload":"{}","sourceTsMillis":5}"#);
        assert_eq!(e.effective_lock_keys(), vec!["account:9", "order:7"]);
        assert_eq!(e.style, "entity");
        assert_eq!(e.trace_workflow_id(), "order:7");
        assert!(!e.is_single_key());
    }

    #[test]
    fn event_style_defaults_instance_key_and_lock() {
        let e = env(r#"{"eventId":"t1","smType":"ticks","eventType":"Tick","style":" EVENT "}"#);
        assert_eq!(e.instance_key, "t1");
        assert_eq!(e.effective_lock_keys(), vec!["ticks:t1"]);
        assert_eq!(e.trace_workflow_id(), "evproc:ticks:ticks:t1");
    }

    #[test]
    fn missing_instance_key_on_entity_style_is_rejected() {
        assert!(serde_json::from_str::<EventEnvelope>(r#"{"eventId":"e","smType":"o","eventType":"x"}"#).is_err());
        assert!(serde_json::from_str::<EventEnvelope>(r#"{"eventId":"e","smType":"o","instanceKey":"1","eventType":"x","style":"bogus"}"#).is_err());
    }

    #[test]
    fn unknown_fields_round_trip_and_java_defaults_are_omitted() {
        let e = env(r#"{"eventId":"e","smType":"o","instanceKey":"1","eventType":"x","futureField":{"a":1}}"#);
        let out = serde_json::to_value(&e).unwrap();
        assert_eq!(out["futureField"]["a"], 1);
        assert!(out.get("scheduledAtMillis").is_none());
        assert!(out.get("parentEventId").is_none());
        assert_eq!(out["style"], "entity");
    }

    #[test]
    fn java_string_order_uses_utf16_units() {
        // U+FF5E (one unit, 0xFF5E) sorts after U+1F600 (surrogates 0xD83D...) in Java, before it in UTF-8.
        assert_eq!(java_cmp("\u{FF5E}", "\u{1F600}"), Ordering::Greater);
        assert_eq!("\u{FF5E}".cmp("\u{1F600}"), Ordering::Less);
    }

    #[test]
    fn java_hash_matches_string_hash_code() {
        assert_eq!(java_hash(""), 0);
        assert_eq!(java_hash("a"), 97);
        assert_eq!(java_hash("hello"), 99_162_322);
        assert_eq!(java_hash("evt-000123"), "evt-000123".encode_utf16().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32)));
    }

    #[test]
    fn sampling_extremes() {
        assert!(Sampling::new(1.0).sampled("x"));
        assert!(!Sampling::new(0.0).sampled("x"));
        let s = Sampling::new(0.5);
        let n = (0..10_000).filter(|i| s.sampled(&format!("e-{i}"))).count();
        assert!((4_000..6_000).contains(&n), "{n}");
    }

    #[test]
    fn flow_names_match_java_flows() {
        let legacy = Flow::new("");
        assert_eq!((legacy.stream(), legacy.orchestration_queue(), legacy.lock_workflow_id("a:1")),
            ("concert-events".into(), "orchestration".into(), "lock:a:1".into()));
        let t = Flow::new(" trading ");
        assert_eq!((t.stream(), t.orchestration_queue(), t.lock_workflow_id("a:1")),
            ("concert-trading".into(), "orchestration-trading".into(), "lock:trading:a:1".into()));
    }

    #[test]
    fn lock_request_waits_and_forwarding() {
        let e = env(r#"{"eventId":"e","smType":"order","instanceKey":"7","eventType":"pay","lockKeys":["account:9"]}"#);
        let r = LockRequest::first(e, 100);
        assert!(!r.is_last_key());
        let f = r.forwarded_to(1, 130);
        assert!(f.is_last_key());
        assert_eq!(f.waited_ms.get("account:9"), Some(&30));
        let w = f.waits_including_current(150);
        assert_eq!(w.get("order:7"), Some(&20));
        let json = serde_json::to_value(&f).unwrap();
        assert_eq!(json["keyIndex"], 1);
        assert_eq!(json["waitedMs"]["account:9"], 30);
    }
}
