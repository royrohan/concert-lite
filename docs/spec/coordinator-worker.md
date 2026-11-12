# Coordinator and worker specification

Status: describes the implementation on `main`, with entity state in the store (DB-truth, §9.3).
Java reference: `platform/orchestration`, `core/common`, `core/worker-sdk`, `platform/store`. Rust coordinator:
`platform/coordinator-rs`.

This is a reimplementation contract. A coordinator or worker built from it, in any language or Temporal SDK, must
interoperate with the existing ones: same workflow types, update names, payload JSON, workflow ids, task queues and
tables. Key words: **MUST**, **MUST NOT**, **SHOULD** and **MAY**, as in RFC 2119.

## Contents

1. [Scope](#1-scope)
2. [Architecture](#2-architecture)
3. [Naming: ids, queues, keys](#3-naming-ids-queues-keys)
4. [The event envelope](#4-the-event-envelope)
5. [Lock keys and the global order](#5-lock-keys-and-the-global-order)
6. [Coordinator: intake](#6-coordinator-intake)
7. [Coordinator: the lock workflow](#7-coordinator-the-lock-workflow)
8. [Coordinator: dispatch to entities](#8-coordinator-dispatch-to-entities)
9. [Worker: the entity workflow](#9-worker-the-entity-workflow)
10. [Worker: chained events and the outbox](#10-worker-chained-events-and-the-outbox)
11. [External side effects (out of scope, parked)](#11-external-side-effects-out-of-scope-parked)
12. [State store: Postgres and Aurora DSQL](#12-state-store-postgres-and-aurora-dsql)
13. [Failure model and guarantees](#13-failure-model-and-guarantees)
14. [Temporal server requirements](#14-temporal-server-requirements)
15. [Configuration](#15-configuration)
16. [Verification](#16-verification)
17. [Known limitations](#17-known-limitations)
18. [Appendix: payload JSON](#appendix-a-payload-json)

---

## 1. Scope

**In scope:**
- the **coordinator**: deduplication, lock workflows, multi-key serialization, dispatch to entities;
- the **worker**: entity state machine workflows, each transition one database transaction (DB-truth), chained
  events through a transactional outbox;
- the **state store** on Postgres or Aurora DSQL, which is the source of truth for entity state.

**Out of scope:**
- the ingress adapter that reads events from a source (Kinesis shard consumers, checkpoints, the ingest supervisor).
  This spec starts where an adapter hands a batch of events to the coordinator (§6);
- egress for analytics (completed-entity snapshots to Kafka, analytics sinks, reconciliation);
- event-style processing (`style = "event"`, processor workflows). The lock workflow's event-style branch is noted
  where it affects the lock protocol (§7.7) and is otherwise not specified;
- other store backends (DynamoDB, Spanner);
- external side effects from inside transitions: parked (§11).

## 2. Architecture

```
            batch of events (from an ingress adapter, out of scope)
                              │
                ┌─────────────▼─────────────┐  orchestration task queue
                │        COORDINATOR        │  (orchestration-<flow>)
                │ dedupe ─▶ group by K0 ─▶  │
                │ acquire on lock:<K0>      │
                │                           │
                │  KeyLockWorkflow per key  │   K0 ─▶ K1 ─▶ … ─▶ Kn-1   (FIFO chain, global key order)
                │  last key: dispatch ──────┼──────────┐
                └───────────────────────────┘          │ UpdateWithStart handle(event), update id = event id
                                                       ▼
                ┌───────────────────────────┐  sm-<smType> task queue
                │          WORKER           │
                │ EntityWorkflow per entity │  (load) ─▶ decide ─▶ commit ONE transaction:
                │ smType:instanceKey        │    sm_state (version-checked) + applied_event + outbox
                └─────────────┬─────────────┘  ─▶ deliver outbox to lock chains (sweeper catches leftovers)
                              ▼
                     Postgres / Aurora DSQL  (source of truth for entities)
       processed_event · sm_state · applied_event · outbox · event_trace
```

**Responsibilities:**
- **Coordinator:** deduplication, ordering, mutual exclusion over keys, delivery. It **MUST NOT** read payloads or
  entity state, and **MUST NOT** change entity state. One implementation serves every flow.
- **Worker:** the only component that decides transitions and changes entity state. Each transition is one database
  transaction (§9.3).
- **Temporal:** holds all durable orchestration state: lock queues, event order, pending deliveries.
- **The store:** holds the entities (`sm_state`), the record of applied events, the outbox of emitted events, the
  dedupe set and traces.

Any number of coordinator and worker processes **MAY** run. Temporal distributes the work and fails it over; there is
no leader election, lease or peer protocol.

## 3. Naming: ids, queues, keys

A **flow** isolates one deployment's coordinator from others on the same Temporal namespace (`CONCERT_FLOW`; empty
means the default flow).

| item | format | example |
|---|---|---|
| entity key | `<smType>:<instanceKey>` | `settlement:PAY-7` |
| entity workflow id | the entity key | `settlement:PAY-7` |
| lock workflow id | default flow: `lock:<key>`; else `lock:<flow>:<key>` | `lock:payments:account:9` |
| orchestration task queue | default flow: `orchestration`; else `orchestration-<flow>` | `orchestration-payments` |
| worker task queue | `sm-<smType>` | `sm-settlement` |
| child (emitted) event id | `<parentEventId>.<n>`, `n` from 1 in emission order | `e-42.1` |
| `acquire` update id | `<requestId>@<lockKey>` | `e-42@account:9` |
| `handle` update id | the event id | `e-42` |

- `smType` **MUST** be unique across everything that shares a Temporal namespace. Two worker implementations polling
  the same `sm-<smType>` queue would each handle the other's events.
- Workflow ids double as idempotency keys and **MUST** be derived exactly as above.
- Ids **MUST** fit Temporal's id length limit (the reference deployment sets `limit.maxIDLength` to 255).

## 4. The event envelope

Every event is an `EventEnvelope`, carried as JSON in Temporal payloads (Appendix A).

| field | type | rule |
|---|---|---|
| `eventId` | string, required | globally unique; drives dedupe, update ids and child ids |
| `smType` | string, required | the state machine type; selects `sm-<smType>` |
| `instanceKey` | string, required | the entity within the type |
| `eventType` | string, required | the transition trigger, e.g. `settle` |
| `lockKeys` | string[] | extra serialization keys, e.g. `account:9`; may be empty; never includes the entity key |
| `payload` | string or null | opaque JSON text, passed to the worker unchanged |
| `sourceTsMillis` | int64 | producer timestamp (traces only) |
| `ingestTsMillis` | int64 | when ingress read it; 0 until ingested (traces only) |
| `style` | string | `entity` (default; also for null or empty) or `event` (out of scope) |
| `scheduledAtMillis` | int64 | event style only; omitted when 0 |
| `parentEventId` | string | set on emitted events; omitted when null |
| `causationRoot` | string | the first event of an emission chain; omitted when null |
| `direct` | bool | entity style, single key: bypass locks (§6.3); omitted when false |

- Readers **MUST** ignore unknown fields; they **MUST NOT** be rejected. Forwarding them is optional: the Rust
  coordinator passes them through unchanged, while the Java one drops them. Producers therefore **MUST NOT** rely on
  extra envelope fields reaching the worker; data for the worker belongs in `payload`.
- The coordinator **MUST NOT** rewrite `payload`.
- `effectiveLockKeys` (derived, never sent): the distinct union of `lockKeys` and the entity key, sorted in the
  global order (§5). It always contains the entity key, so two events on the same entity never run concurrently.
- `K0` denotes the first effective key. An event is *single-key* when it has exactly one effective key.

## 5. Lock keys and the global order

**Order.** Keys are compared by **UTF-16 code unit order** (Java `String.compareTo`). Every implementation **MUST**
use this order; byte order or locale collation is wrong. The Rust coordinator implements the same comparison
(`model.rs`). Chains built by different implementations then take keys in the same order, which is what makes mixing
them deadlock-free (§7.4).

**Templates.** Producers or the worker derive keys from lock templates such as `account:{accountId}`:
- `{id}` is the target instance key;
- `{field}` is a top-level payload field;
- a missing value is an error.

**Rule for entity designers.** All events of an entity **SHOULD** share the same `K0`. Ordering between events is
guaranteed only through their common first key (§13.2). The generator warns when an entity's lock templates break
this rule.

## 6. Coordinator: intake

The ingress adapter hands the coordinator a batch of envelopes in source order. For each batch the coordinator
**MUST**:

1. **Deduplicate**, in one store round trip (`claimForDispatch`, §12.3). It drops every event that is not claimed,
   and every repeat of an id within the batch.
2. **Group** the survivors by `K0`, preserving batch order within each group.
3. **Submit each group sequentially** in that order. Different groups **MAY** be submitted in parallel; the reference
   bounds this with `INGEST_PARALLEL_GROUPS`, default 256.
4. **Mark** the batch's surviving ids `DISPATCHED` once every submission returned.

If any submission fails, the batch **MUST** fail without marking. The adapter retries it: events still `RECEIVED`
are claimed again, and Temporal's idempotent ids absorb the repeats.

### 6.1 Submitting an event

| event | submission |
|---|---|
| entity style, single key, and (`direct` or `smType` in `DIRECT_SM_TYPES`) | direct dispatch (§6.3) |
| otherwise | `acquire` on `lock:<K0>` with a first request (§7.2) |

A submission returns as soon as the order is fixed. For a lock that is when the `acquire` update is **ACCEPTED**;
for a direct dispatch it is when the entity's `handle` update is **ACCEPTED**. The coordinator **MUST NOT** wait for
the transition itself. Submitting a group sequentially to ACCEPTED is what preserves per-`K0` source order.

### 6.2 Enqueue with start

`acquire` is sent as **UpdateWithStart**:
- workflow id `lock:<K0>` on the orchestration queue, search attribute `LockKeys = [K0]`;
- id conflict policy `USE_EXISTING`;
- start input `LockState.fresh(K0, LOCK_IDLE_SECONDS)`;
- update id `<requestId>@<K0>`, waiting for stage ACCEPTED.

Handling the response:
- **Validator rejection with message `duplicate request`:** the request is already queued or was recently served.
  The caller **MUST** treat it as success.
- **Workflow error because the lock run completed (idle) while the update was in flight:** retry. The reference
  makes 5 attempts with 20 ms × attempt backoff, and a retry starts a fresh run.
- **Any other update failure:** an error.

### 6.3 Direct dispatch

A type whose events never share keys with multi-key events **MAY** skip locks. The coordinator then sends the
entity's `handle` UpdateWithStart (§8.1) and waits for **ACCEPTED** only.

The entity's update ordering keeps such events in order per instance. They are not mutually exclusive with
multi-key events on the same entity, which is why a type **MUST** be listed in `DIRECT_SM_TYPES` (or an event marked
`direct`) only when that cannot happen.

## 7. Coordinator: the lock workflow

One `KeyLockWorkflow` per key (workflow type `KeyLockWorkflow`), on the orchestration queue.

### 7.1 Interface

| member | kind | input | behavior |
|---|---|---|---|
| `run` | workflow method | `LockState` | serves the queue (§7.3) |
| `acquire` | update | `LockRequest` | appends the request unless known; completes immediately |
| `acquire` validator | update validator | `LockRequest` | rejects a missing request or event; rejects with `duplicate request` an id that is queued or in the recent window |
| `release` | update | `requestId` | pops the request if it is the head, otherwise removes it from the queue; unknown ids are a no-op |
| `unblock` | update | `requestId` | event style only (§7.7) |
| `snapshot` | query | — | `LockSnapshot{lockKey, head, headState, waiting[], ops}` |

Lock interactions **MUST** be updates, not signals. Updates reach the worker directly, while signals take an extra
trip through the server's transfer queue, about 3× slower in the reference measurements.

### 7.2 Types

`LockRequest`:

| field | rule |
|---|---|
| `requestId` | the event id |
| `event` | the envelope |
| `keyIndex` | position of this lock's key in `event.effectiveLockKeys` |
| `enqueuedAtMillis` | when it was enqueued on this lock |
| `waitedMs` | map from key to wait, for keys already held |

`LockState`, which is both the start input and the continue-as-new input:

| field | rule |
|---|---|
| `lockKey` | the key |
| `queue` | requests, head first |
| `headForwardedAtMillis` | non-zero while the head has been forwarded and awaits `release` |
| `recentRequestIds` | bounded window of served ids |
| `idleSeconds` | idle shutdown; 0 means 600 |
| `headBlocked` | event style only |

A first request is `{requestId: eventId, event, keyIndex: 0, enqueuedAtMillis: now, waitedMs: {}}`. The request
forwarded from index `i` to `i+1` keeps `requestId` and `event`, sets `keyIndex = i+1` and
`enqueuedAtMillis = now`, and adds this lock's wait to `waitedMs`.

### 7.3 Serving the queue

The head owns the key. The workflow loops:

1. **Idle:** wait up to `idleSeconds` for a non-empty queue. On timeout:
   - wait for in-flight releases and every update handler to finish;
   - if the queue is still empty, **complete**. The next UpdateWithStart starts a fresh run, and an update racing
     the completion is rejected by the server and retried by the caller (§6.2).
2. **Head on its last key** (`keyIndex == size-1`):
   - dispatch the event to its entity (§8) and wait for the outcome;
   - **pop** the head;
   - if `keyIndex > 0`, release the chain's lower keys `K0..Kn-2` asynchronously (§7.5). The next head proceeds
     without waiting for those releases.
   - If dispatch fails (rejected by the entity's validator, or retries exhausted), record a `FAILED` trace and
     still pop, so the key keeps moving.
3. **Head not on its last key:**
   - if not yet forwarded, `acquire` the forwarded request on `lock:<K(i+1)>` (§6.2 rules) and mark the head
     forwarded;
   - wait until the head is released by its chain.
   - Race rule: if the head was released while the forward was in flight, the workflow **MUST NOT** mark the *new*
     head forwarded. Doing so would park a head that was never forwarded until the lease expires.
   - Safety net: if no release arrives within the **forward lease** (1 h), record `FAILED` and pop.
4. **Continue-as-new** after 1000 pops, or when Temporal suggests it. First wait for in-flight releases and handlers,
   then carry `queue`, `headForwardedAtMillis`, `recentRequestIds` and `idleSeconds`.

**Pop:** remove the head, clear the forwarded and blocked flags, count one op, and remember the id in the recent
window. The window holds 4000 ids, evicting the oldest first.

**On start or continue:**
- requests carried in `LockState.queue` go to the front, in order, ahead of updates accepted before `run` executed;
- an id already present is not duplicated;
- the recent window is restored.

The validator applies duplicate checks only after initialization.

### 7.4 Multi-key chains and deadlock freedom

An event with keys `K0 < K1 < … < Kn-1` is queued on `K0`. When it becomes head of `Ki` (with `i < n-1`), that
lock forwards it to `K(i+1)` and **keeps holding** `Ki`. The lock of `Kn-1` dispatches it and then releases
`K0..Kn-2`.

- **Mutual exclusion:** while an event is dispatched, it is head of every one of its keys.
- **No deadlock:** every chain acquires keys in the same global order (§5) and holds them until it is dispatched.
  The wait-for graph between locks therefore has no cycles.

Two implementation rules keep the workflows themselves from deadlocking:
- **Releases MUST run as regular (not local) activities.** A local activity holds the releasing lock's workflow
  task open. If lock A is forwarding to B while B releases A, the two tasks wait on each other.
- **Forwards and dispatches MAY run as local activities** (the reference does). They go to *higher* keys or to
  entities, which never call back into a lower lock synchronously.

### 7.5 Release

The releasing activity sends `release(requestId)` to each lower key's lock **in parallel**.
- If any release fails, the activity **MUST** fail after attempting every key, so it is retried. This happens, for
  example, while the target lock's workflow task is failing during a coordinator restart. A swallowed failure loses
  the release and leaves the key held forever: an earlier Java implementation did this, and the chaos test caught it.
- A lock that no longer exists (workflow not found) counts as released.
- Retry policy: start-to-close 30 s, backoff 50 ms to 10 s, unlimited attempts.

`release` is idempotent:
- the head is popped;
- a non-head is removed from the queue;
- an unknown id is a no-op.

A lock **MUST** drain its pending releases before it completes or continues-as-new.

### 7.6 Activity options (reference values)

| call | kind | start-to-close | schedule-to-close | retry |
|---|---|---|---|---|
| dispatch to entity | local | 30 s | 1 h | 50 ms to 10 s; not on `EntityRejected` |
| forward (`acquire` on the next key) | local | 30 s | 1 h | 50 ms to 10 s |
| release lower keys | regular | 30 s | none | 50 ms to 10 s, unlimited |

### 7.7 Event-style hook (out of scope, for compatibility)

For an event with `style = "event"`:
- the last lock dispatches to a processor workflow instead of an entity;
- if the processor answers *blocking error*, the head stays (`headBlocked`) until `unblock(requestId)` arrives.

A reimplementation that does not support event style **MUST** still keep `headBlocked` and the `unblock` update in
`LockState` and the interface, so its histories and carried state stay compatible.

## 8. Coordinator: dispatch to entities

### 8.1 The call

**UpdateWithStart** `handle(event)` on the entity workflow:

| setting | value |
|---|---|
| workflow id | `<smType>:<instanceKey>` |
| task queue | `sm-<smType>` |
| id conflict policy | `USE_EXISTING` |
| workflow task timeout | `WORKFLOW_TASK_TIMEOUT_MS` |
| search attributes | `SmType`, `InstanceKey` (keyword) |
| start input | `EntityInit.fresh(smType, instanceKey)` |
| update id | the **event id** |
| wait stage | **COMPLETED** from a lock; **ACCEPTED** for direct dispatch |

- The result is a `TransitionResult` (§9.1). A *rejected* transition is a normal result, not an error.
- A validator rejection (`WorkflowUpdateException`) is deterministic. The dispatcher **MUST** surface it as a
  non-retryable failure of type `EntityRejected`.
- A repeated dispatch with the same update id attaches to the same update and returns its result. This includes a
  retry of the local activity after a coordinator crash, or a call that times out while the update is still running.

### 8.2 Long transitions

A transition normally takes one database transaction. A store outage makes the commit retry (§9.4), and the
dispatch's 30 s start-to-close can then pass. The dispatch is retried, re-attaching to the running update, until
its 1 h schedule-to-close.

### 8.3 Traces

Dispatch writes trace rows asynchronously:
- `LOCK_GRANTED` per key, with `waitedMs`;
- `DONE` with the outcome and end-to-end latency.

Tracing **MUST NOT** add latency to the workflows, and **MAY** be sampled (§12.5).

## 9. Worker: the entity workflow

One workflow per entity, workflow type `EntityWorkflow`, on `sm-<smType>`. Every state machine type implements this
same interface; the task queue selects the implementation.

### 9.1 Interface and types

| member | kind | input / output |
|---|---|---|
| `run` | workflow method | `EntityInit` |
| `handle` | update | `EventEnvelope` → `TransitionResult` |
| `handle` validator | update validator | rejects an empty `eventType`, and an event whose `<smType>:<instanceKey>` is not this workflow's id |
| `snapshot` | query | `EntitySnapshot{workflowId, smType, instanceKey, state, data, version, handled, recent[]}` |

`EntityInit`:
- `{smType, instanceKey, state, data, version, recent[], createdAtMillis}`;
- `state == null` means a new entity;
- `recent` holds up to 50 `TransitionRecord{eventId, eventType, from, to, accepted, tsMillis}`.

`TransitionResult`: `{eventId, fromState, toState, accepted, message, version}`.

### 9.2 The transition table

A type declares:
- an initial state;
- edges `FROM -eventType-> TO`, each with an optional payload type;
- terminal states. These default to states without outgoing edges.

Edges **MUST** be deterministic: built once and never changed at runtime. A change that alters existing behavior
needs Temporal versioning (`getVersion`) like any workflow code change.

### 9.3 DB-truth

The entity lives in the state store (§12), not in the workflow. Each event is **one database transaction** that
writes, together or not at all:
- the new `sm_state` row, conditional on the version the decision was made on;
- an `applied_event` record of the event and its result;
- `outbox` rows for the events the transition emitted (§10).

The workflow orders the events and runs the decision code on a **cached copy** of the entity. The commit's version
check detects a stale copy, and the `applied_event` primary key detects a redelivered event. Implementations **MUST
NOT** treat workflow memory as the entity's state: it is a cache, valid only while commits from it succeed.

### 9.4 Handling an event

Updates **MUST** be decided strictly in update-acceptance order, even when a decision yields:

1. **Ticket.** On entry, before any yield, take ticket `t`. Wait until `run` has set up and `served == t`. An update
   can arrive in the same workflow task as the start, so the wait for `run` matters.
2. **Load, if the cache is not valid.** A fresh run, or a run after a redelivery, has no valid cache. Call
   `loadEntity(workflowId, eventId)`: it returns the stored row (none for a new entity, whose cache is then the initial
   state at version 0) and, if this event was applied before, its stored result. A stored result is the answer: go to
   step 6.
3. **Decide** on the cache. Look up `next(state, eventType)`:
   - **No edge:** the result is rejected (`no transition from S on E`).
   - **Edge `S → T`:** call the type's decision. It either rejects with a reason, or accepts with new data and the
     events it emits.
4. **Commit** with `commitTransition` (§12.4), carrying:
   - the expected version (the cache's);
   - the result: accepted with `version + 1`, or rejected with the unchanged version;
   - for an accepted result, the new row and the emitted events;
   - a trace row (`TRANSITION` or `REJECTED`, best effort).

   It is a local activity: start-to-close 10 s, schedule-to-close **1 h** (a store outage is waited out, as long as
   the lock's dispatch waits), backoff 100 ms to 10 s. The outcome decides what happens next:
   - **COMMITTED:** apply the result to the cache.
   - **ALREADY_APPLIED:** the event was applied before (a redelivery). Its stored result is the answer, and the cache
     is marked invalid (it may be behind the store).
   - **CONFLICT:** the store holds a newer version. Replace the cache with the returned row and go back to step 3.
     After 10 conflicts in a row the update fails (`TransitionConflict`); entity events are serialized, so even one
     conflict is unusual.
5. **Release the ticket** (`served += 1`), so the next event can be decided.
6. **After commit**, only if this call committed the transition (not for ALREADY_APPLIED):
   - start the outbox delivery of the emitted events (§10);
   - run the post-commit hooks (e.g. terminal-state handling).
7. **Return** the `TransitionResult`. The update completes, the lock chain is released, and the next event on these
   keys may start.

Rules:
- Decisions **MUST** be deterministic: no I/O, no wall clock (use workflow time), no randomness.
- Decisions **MUST NOT** perform side effects. They run before the commit, are not part of it, and may run more than
  once for one event (after a conflict). Effects on other entities are emitted events (§10). Calls to external
  systems are out of scope (§11).
- A **rejection** leaves state, data and version unchanged. It is still recorded in `applied_event`, so a redelivery
  gets the same rejection, and it is committed only on the version it was decided on.
- For typed (model) machines the decision also covers:
  - binding the payload to the edge's payload type; JSON that does not bind is a rejection;
  - validating payload multiplicities;
  - validating the entity data after the transition. Invalid data rejects the event.

### 9.5 Continue-as-new

`run` waits until all of the following hold, then continues-as-new with
`{state, data, version, recent, createdAtMillis}`:
- the run has handled **500** events, or Temporal suggests continuing;
- every update handler has finished;
- no work the run started is still pending: outbox deliveries, and any asynchronous activities.

The carried state is the cache. It is passed as null (a fresh load) when the cache was marked invalid. Redeliveries
across continue-as-new are recognized by the store (`applied_event`), not by Temporal update ids.

## 10. Worker: chained events and the outbox

During a decision the machine **MAY** emit events to other entities, or to itself:
- explicitly (`emitEntity(smType, instanceKey, eventType, payload, extraLockKeys)`);
- or declaratively, with state-entry emissions from the model.

Rules:
- Emitted events are **buffered** by the decision. They are written to the `outbox` **in the transition's
  transaction**, so they exist if and only if the transition committed. A rejected or conflicting decision emits
  nothing.
- Child ids are `<eventId>.<n>`, with `parentEventId` set and `causationRoot` inherited. Replays, retries and
  redeliveries therefore produce the same ids.
- **Fast path:** right after commit, the workflow delivers the new rows with a **regular activity**
  (`EventEmitDeliverOutbox`, unlimited retries). It sends `acquire` of a first request on each child's `K0` (§6.2
  rules; a duplicate is success), then marks the rows sent.
  - It **MUST NOT** be a local activity: the emitting entity is inside an update that a lock chain is waiting on.
- **Sweeper:** every worker sweeps its own type's outbox rows (`workflow_id LIKE '<smType>:%'`) that are still unsent
  after `OUTBOX_SWEEP_AFTER_SECONDS` (30), every `OUTBOX_SWEEP_SECONDS` (10). It delivers them the same way. This
  covers a worker that died between commit and delivery.
- Delivery is **at least once**, and the *effect* is once:
  - several workers may sweep the same rows;
  - a lock's validator and recent window drop duplicate requests;
  - the target entity's `applied_event` record drops a duplicate that gets through, however late.
- Emitted events skip `processed_event`.
- A child's lock keys are the declared extra keys plus its own entity key.
- Housekeeping: the sweeper deletes sent rows older than `OUTBOX_RETENTION_HOURS` (24), and `applied_event` records
  older than `APPLIED_EVENT_RETENTION_HOURS` (168). A redelivery older than the second window is no longer recognized.

## 11. External side effects (out of scope, parked)

Calling external systems (REST services, Kafka) from inside a transition does not fit DB-truth: the call would happen
before the commit and could not be part of it. The previous design is described in `docs/external-side-effects.md`:
- activities with idempotency keys and Sagas;
- an example and chaos scenarios, parked in git history at `2e0f267`.

A future design **SHOULD** model external effects as outbox intents: the transition records the intended call in its
transaction, and a delivery step performs it with an idempotency key and reports the outcome back as an event.

## 12. State store: Postgres and Aurora DSQL

### 12.1 Role

- **The store is the source of truth for entity state (DB-truth, §9.3):**
  - `sm_state` holds the entities;
  - `applied_event` holds every event applied to an entity, with its result;
  - `outbox` holds the events transitions emitted, until delivered.

  The three are written in one transaction per event.
- Temporal holds the orchestration state: lock queues, event order, pending deliveries.
- `processed_event` is the coordinator's dedupe set.
- `event_trace` is best-effort observability.
- Entity workers require `STORE_KIND` postgres or dsql. The DynamoDB and Spanner stores do not implement
  `commitTransition`.

### 12.2 Schema

The DDL must be DSQL compatible:
- no foreign keys and no sequences;
- one DDL statement per transaction;
- secondary indexes created `ASYNC` on DSQL;
- JSON stored as `jsonb` on Postgres and `text` on DSQL.

```sql
CREATE TABLE IF NOT EXISTS processed_event (
    event_id    text PRIMARY KEY,
    received_at timestamptz NOT NULL,
    status      text NOT NULL            -- RECEIVED | DISPATCHED
);
CREATE TABLE IF NOT EXISTS sm_state (
    workflow_id   text PRIMARY KEY,      -- smType:instanceKey
    sm_type       text NOT NULL,
    state         text NOT NULL,
    data          jsonb,                 -- text on DSQL
    version       bigint NOT NULL,
    last_event_id text,
    updated_at    timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_event (
    workflow_id text NOT NULL,
    event_id    text NOT NULL,
    accepted    boolean NOT NULL,
    from_state  text NOT NULL,
    to_state    text NOT NULL,
    version     bigint NOT NULL,
    message     text,
    applied_at  timestamptz NOT NULL,
    PRIMARY KEY (workflow_id, event_id)
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id    text PRIMARY KEY,        -- child event id <parentEventId>.<n>
    workflow_id text NOT NULL,           -- the emitting entity
    envelope    text NOT NULL,           -- EventEnvelope JSON
    created_at  timestamptz NOT NULL,
    sent_at     timestamptz
);
CREATE TABLE IF NOT EXISTS event_trace (
    trace_id    text PRIMARY KEY,        -- client-generated UUID
    event_id    text NOT NULL,
    ts          timestamptz NOT NULL,
    stage       text NOT NULL,
    workflow_id text,
    lock_key    text,
    detail      text
);
CREATE INDEX ASYNC IF NOT EXISTS event_trace_event_idx ON event_trace (event_id);         -- ASYNC on DSQL only
CREATE INDEX ASYNC IF NOT EXISTS event_trace_workflow_idx ON event_trace (workflow_id);
CREATE INDEX ASYNC IF NOT EXISTS event_trace_ts_idx ON event_trace (ts);
CREATE INDEX ASYNC IF NOT EXISTS processed_event_received_idx ON processed_event (received_at);
CREATE INDEX ASYNC IF NOT EXISTS applied_event_applied_idx ON applied_event (applied_at);
CREATE INDEX ASYNC IF NOT EXISTS outbox_created_idx ON outbox (created_at);
```

### 12.3 Dedupe (`claimForDispatch`)

In one autocommit round trip:

```sql
INSERT INTO processed_event (event_id, received_at, status) VALUES (?, ?, 'RECEIVED'), …
ON CONFLICT (event_id) DO NOTHING RETURNING event_id;
-- for ids not returned:
SELECT event_id FROM processed_event WHERE status = 'RECEIVED' AND event_id IN (…);
```

- The claimed set is the inserted ids plus ids still `RECEIVED` (a crash between claim and mark). Everything else is a
  duplicate.
- After submission: `UPDATE processed_event SET status = 'DISPATCHED' WHERE event_id IN (…)`.
- Rows older than `DEDUPE_TTL_HOURS` (default 24) are swept in batches of 1000, every 10 minutes. A duplicate
  arriving after the sweep is still absorbed downstream, by the lock's recent window and the entity update id, as
  long as those still remember it.

### 12.4 Transition commit (`commitTransition`)

One transaction per event, retried as a whole on `40001` / `40P01` (§12.6). Anything but COMMITTED is rolled back.

```sql
-- 1. the applied-event record; its primary key turns a redelivery into a no-op
INSERT INTO applied_event (workflow_id, event_id, accepted, from_state, to_state, version, message, applied_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (workflow_id, event_id) DO NOTHING;
--    0 rows: SELECT the stored result, roll back            -> ALREADY_APPLIED(stored result)

-- 2a. accepted: move the entity, only from the version the decision was made on
UPDATE sm_state SET state = ?, data = ?, version = ?, last_event_id = ?, updated_at = ?
WHERE workflow_id = ? AND version = :expected;
--    0 rows and :expected = 0: a new entity
INSERT INTO sm_state (workflow_id, sm_type, state, data, version, last_event_id, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (workflow_id) DO NOTHING;
--    still 0 rows: SELECT the current row, roll back         -> CONFLICT(current row)
--    then the emitted events
INSERT INTO outbox (event_id, workflow_id, envelope, created_at) VALUES (?, ?, ?, ?), …
ON CONFLICT (event_id) DO NOTHING;

-- 2b. rejected: nothing changes, but the rejection was decided on :expected
SELECT version FROM sm_state WHERE workflow_id = ? FOR UPDATE;  -- absent = 0
--    different from :expected: roll back                     -> CONFLICT(current row)

COMMIT;                                                       -- COMMITTED
```

Notes:
- A rejected first event creates no `sm_state` row. Its `applied_event` record is written.
- `loadEntity(workflowId, eventId)` is a plain read of the `sm_state` row and the `applied_event` record.
- The outbox queries:
  - pending rows: `SELECT envelope FROM outbox WHERE sent_at IS NULL AND created_at < ? AND workflow_id LIKE ? ORDER
    BY created_at LIMIT ?`;
  - mark sent: `UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL AND event_id IN (…)`, in batches of 1000;
  - housekeeping deletes, batched to at most 2500 rows per statement.
- `upsertState` (the version-guarded upsert) remains for other writers, such as event-style keyed state. Entity
  workflows do not use it.

### 12.5 Traces

- Rows are appended in batches; DSQL caps a transaction at 3000 modified rows.
- Stages: `RECEIVED`, `DROPPED`, `LOCK_WAIT`, `LOCK_GRANTED`, `TRANSITION`, `REJECTED`, `RELEASED`, `DONE`, `FAILED`
  (event style adds more).
- **Sampling:** `TRACE_SAMPLE` (0..1) keeps an event when `floorMod(javaHashCode(eventId) * 0x9E3779B1, 10000) <
  rate × 10000`.
  - `javaHashCode` is Java's specified `String.hashCode`, with 32-bit wraparound.
  - Every process **MUST** use this function, so a sampled event has its complete timeline.

### 12.6 Aurora DSQL specifics

| concern | rule |
|---|---|
| concurrency | optimistic: conflicting transactions fail at commit with SQLSTATE `40001` (`OC000`/`OC001`). Every store call **MUST** retry on `40001` and `40P01`, with jittered backoff (reference: 6 attempts, at most about 200 ms each). Postgres raises the same states, so the local stand-in exercises the path |
| authentication | IAM tokens, minted per connection by the DSQL JDBC connector: `jdbc:aws-dsql:postgresql://<endpoint>/postgres`, user `admin` |
| connections | DSQL closes connections after 60 min; pools **MUST** recycle sooner (reference: 50 min). Pool size `STORE_POOL_SIZE` (32) |
| transaction size | at most 3000 modified rows per transaction: batch bulk writes and sweeps |
| types and DDL | no `jsonb` (store `text`, cast in queries); `CREATE INDEX ASYNC`; one DDL statement per transaction |

## 13. Failure model and guarantees

### 13.1 Delivery

| hop | guarantee | mechanism |
|---|---|---|
| intake to lock | at least once; duplicates dropped | `processed_event` claim; re-claim of `RECEIVED`; `acquire` validator and recent window (4000) |
| lock to lock (forward) | at least once, deduplicated | `acquire` update id `<id>@<key>` and the validator |
| lock to entity | at least once; **applied once** | `handle` update id = event id within a run; `applied_event` across runs and after |
| transition to store | **atomic**: state, applied event and outbox together, or nothing | one transaction, conditional on the expected version (§12.4) |
| entity to emitted event | at least once; **applied once**; exists only if the transition committed | outbox in the transition's transaction; fast path and sweeper; lock dedupe and the target's `applied_event` |

### 13.2 Ordering

- **Per entity:** transitions are applied in the order their `handle` updates were accepted (§9.4).
- **Same first key:** events with the same `K0`, submitted in one source order, are applied in that order (§6, §7.3).
- **Shared later key only:** events that share a key but have different `K0` are **mutually exclusive** but not
  ordered by source. Their relative order is the order in which their chains reach the shared key. Design entities so
  their events share `K0` (§5).
- **Direct types:** ordered per instance by update acceptance, but not exclusive with multi-key events.

### 13.3 Crashes

| failure | effect |
|---|---|
| coordinator process killed | workflow tasks time out (`WORKFLOW_TASK_TIMEOUT_MS`, 2 s) and move to another coordinator. Local activities in flight are re-run; their calls are idempotent |
| worker process killed | entity workflow tasks and local activities move to another worker. A transaction that committed before the kill is found on retry (ALREADY_APPLIED, or the same commit is re-sent and refused as already applied); one that did not commit is redone. Outbox rows committed but not delivered are delivered by a sweeper |
| whole tier down | work queues durably in Temporal until a process returns; nothing is lost |
| store unavailable | dedupe fails the batch, which the adapter retries. Transitions wait: the commit retries for up to 1 h, and nothing is half written. Past 1 h the `handle` update fails and the lock records `FAILED` and moves on. The event is then not applied, consistently: no state, no applied record, no outbox rows |
| Temporal restarted | clients and pollers reconnect; all orchestration state is in Temporal's persistence |

**Hot keys.** A key shared by many events keeps many updates in flight on its lock: an `acquire` per waiting event,
plus releases. The Temporal server's per-workflow in-flight update limit (default 10) **MUST** be raised (§14), or
the key livelocks.

## 14. Temporal server requirements

| setting | value | why |
|---|---|---|
| `frontend.enableUpdateWorkflowExecution` | true | all lock and entity interactions are updates |
| `frontend.enableExecuteMultiOperation` | true | UpdateWithStart |
| `history.maxInFlightUpdates` | ≥ 500 (reference) | hot lock keys |
| `limit.maxIDLength` | ≥ 255 | composite workflow and update ids |
| `system.enableEagerWorkflowStart`, `system.enableActivityEagerExecution` | true (recommended) | latency: releases and first starts run on the calling worker |
| search attributes | `SmType`, `InstanceKey` (Keyword), `LockKeys` (KeywordList), `EventId` (Keyword) | set on workflow start: entities get `SmType` and `InstanceKey`, locks get `LockKeys`. They must be registered on the namespace; `TEMPORAL_SEARCH_ATTRIBUTES=false` turns them off (tests) |

Worker settings: virtual workflow threads, a large workflow cache (`WORKER_WORKFLOW_CACHE`, 50 000), and sticky
schedule-to-start `STICKY_SCHEDULE_TO_START_MS` (1 s).

**One implementation per queue.** Workflow histories are not portable between SDKs. A queue **MUST** be served by one
implementation at a time. The Rust coordinator enforces this by refusing to start while a running workflow on its
queue was last worked on by another implementation (`COORDINATOR_ALLOW_FOREIGN_RUNS=true` skips the check).

## 15. Configuration

| variable | default | component | meaning |
|---|---|---|---|
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | `localhost:7233` / `default` | both | Temporal frontend |
| `CONCERT_FLOW` | empty | coordinator | flow isolation (§3) |
| `STORE_KIND` | `postgres` | both | `postgres` or `dsql` (others are out of scope) |
| `STORE_JDBC_URL` | `jdbc:postgresql://localhost:5433/concert` | both | DSQL: `jdbc:aws-dsql:postgresql://<endpoint>/postgres` |
| `STORE_USER` / `STORE_PASSWORD` | `concert` / `concert` (DSQL: `admin`, IAM token) | both | |
| `STORE_POOL_SIZE` | 32 | both | connection pool |
| `STORE_INIT_SCHEMA` | true | both | apply the schema on start |
| `LOCK_IDLE_SECONDS` | 600 | coordinator | idle lock shutdown |
| `DIRECT_SM_TYPES` | empty | coordinator | types dispatched without locks (§6.3) |
| `INGEST_PARALLEL_GROUPS` | 256 | coordinator | parallel `K0` groups per batch |
| `DEDUPE_TTL_HOURS` | 24 | coordinator | `processed_event` retention |
| `ORCH_WF_POLLERS_MAX` / `ORCH_ACTIVITY_POLLERS_MAX` | 64 / 8 | coordinator | poller autoscaling ceilings |
| `WORKER_WF_POLLERS_MAX` / `WORKER_ACTIVITY_POLLERS_MAX` | 64 / 4 | worker | |
| `WORKER_WF_SLOTS` / `WORKER_LA_SLOTS` | 1000 / 2000 | both | concurrent workflow tasks / local activities |
| `WORKFLOW_TASK_TIMEOUT_MS` | 2000 | both | failover speed (stock Temporal: 10 000) |
| `STICKY_SCHEDULE_TO_START_MS` | 1000 | both | failover speed (stock: 5000) |
| `TRACE_SAMPLE` | 1.0 | both | trace sampling (§12.5) |
| `OUTBOX_SWEEP_SECONDS` / `OUTBOX_SWEEP_AFTER_SECONDS` | 10 / 30 | worker | outbox sweeper interval, and the age of rows it delivers (§10); 0 turns the sweeper off |
| `OUTBOX_RETENTION_HOURS` | 24 | worker | delete sent outbox rows after this |
| `APPLIED_EVENT_RETENTION_HOURS` | 168 | worker | delete applied-event records after this; redeliveries older than this are no longer recognized |

## 16. Verification

A conforming implementation **SHOULD** pass the existing suites:
- **Unit and Temporal test-server suites:**
  - the lock protocol: `OrchestrationFlowTest` and `ForwardReleaseRaceTest` (`platform/orchestration`);
  - the entity workflow: `AbstractStateMachineTest` (including a redelivery after continue-as-new, a stale cache and
    a store outage), `ModelStateMachineTest`, `OutboxSweeperTest` and `LockTemplatesTest` (`core/worker-sdk`);
  - the store: `StateStoreContract`, run by `InMemoryStateStoreTest` and `PostgresStoreTest` (`platform/store`).
    It covers the atomic commit, redelivery, version conflicts, rejections, a concurrent race with exactly one
    winner, and the outbox lifecycle;
  - key order (UTF-16), wire JSON compatibility and the lock queue logic: the Rust coordinator's unit tests
    (`cargo test` in `platform/coordinator-rs`).
- **`scripts/chaos.sh`:** kills and restarts coordinators and workers under load, optionally Temporal
  (`CHAOS_TEMPORAL=1`), and optionally freezes the Postgres store (`CHAOS_STORE=1`). It verifies:
  - no lost events, no double application, per-key order;
  - for entity runs, the outbox: every `CHAIN_EVERY`-th append emits a receipt to ledger `<key>-R`, each receipt
    ledger must count exactly the chained appends, and no outbox row may be left pending.

  `RUST_COORDINATOR=1` mixes in the Rust coordinator.

  Reference run (`RUN_ID=dbtruth2 DURATION=120 RATE=50 KEYS=100 KILL_EVERY=12 CHAOS_STORE=1`, Postgres): 6000
  appends to 100 ledgers (half of them multi-key), with 4 `kill -9` (one coordinator, three workers) and 2 store
  freezes (5 s and 7 s). All checks passed:
  - no lost or double-applied events, order kept;
  - 1200 receipts delivered through the outbox, each applied exactly once, no rows left pending;
  - in the store: 7200 `applied_event` records (6000 appends + 1200 receipts) and 1200 outbox rows, all sent.

  The first run (`dbtruth1`) found a coordinator bug: dropped lock releases (§7.5), now fixed.

## 17. Known limitations

- **Redelivery window.** `applied_event` recognizes redeliveries for `APPLIED_EVENT_RETENTION_HOURS` (default 7
  days). An event redelivered after its record was swept would be applied again.
- **Mutual exclusion is not causal ordering.** See §13.2 for events that share only a later key.
- **Emitted events may be reordered after a crash.** The fast path delivers a transition's events right after its
  commit, in commit order. A row left behind is delivered by the sweeper up to about 40 s later, possibly after
  events emitted by later transitions. Each emitted event is applied once, but a target entity should not depend on
  the arrival order of events from different source transitions.
- **One database round trip per event**, plus one load when a run starts or a cache is invalidated. Throughput per
  entity is bounded by the store's transaction latency.
- **No external side effects** inside transitions (§11).
- **Postgres or DSQL only** for entity workers (§12.1).

## Appendix A: payload JSON

Payloads use Temporal's default JSON converter. Field names are exactly as below; integers are JSON numbers, and
absent optional fields **MAY** be omitted.

`EventEnvelope`:

```json
{"eventId":"e-42","smType":"settlement","instanceKey":"PAY-7","eventType":"settle","lockKeys":["account:9"],
 "payload":"{\"accountId\":\"ACC-1\",\"amount\":125.50,\"currency\":\"EUR\"}",
 "sourceTsMillis":1767225600000,"ingestTsMillis":1767225600050,"style":"entity"}
```

`LockRequest` and `LockState`:

```json
{"requestId":"e-42","event":{…},"keyIndex":1,"enqueuedAtMillis":1767225600060,"waitedMs":{"account:9":3}}
```

```json
{"lockKey":"settlement:PAY-7","queue":[{…}],"headForwardedAtMillis":0,"recentRequestIds":["e-40","e-41"],"idleSeconds":600,"headBlocked":false}
```

`EntityInit` and `TransitionResult`:

```json
{"smType":"settlement","instanceKey":"PAY-7","state":"SETTLING","data":"{…}","version":1,"recent":[],"createdAtMillis":1767225600100}
```

```json
{"eventId":"e-42","fromState":"NEW","toState":"SETTLING","accepted":true,"message":null,"version":1}
```

In the `LockRequest` example, the event's effective keys are `["account:9", "settlement:PAY-7"]`: it was forwarded
from `account:9`, where it waited 3 ms. A rejected `TransitionResult` has `accepted: false`, `toState` equal to
`fromState`, an unchanged `version`, and the reason in `message`.
