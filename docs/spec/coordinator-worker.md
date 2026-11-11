# Coordinator and worker specification

Status: describes the implementation on `main` (Java reference: `platform/orchestration`, `core/common`,
`core/worker-sdk`, `platform/store`; Rust coordinator: `platform/coordinator-rs`).

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
10. [Worker: chained events](#10-worker-chained-events)
11. [Worker: external side effects and Sagas](#11-worker-external-side-effects-and-sagas)
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
- the **worker**: entity state machine workflows, persistence, chained events, external side effects (REST and
  Kafka) with idempotency keys and Sagas;
- the **state store** on Postgres or Aurora DSQL.

**Out of scope:**
- the ingress adapter that reads events from a source (Kinesis shard consumers, checkpoints, the ingest supervisor).
  This spec starts where an adapter hands a batch of events to the coordinator (§6);
- egress for analytics (completed-entity snapshots to Kafka, analytics sinks, reconciliation);
- event-style processing (`style = "event"`, processor workflows). The lock workflow's event-style branch is noted
  where it affects the lock protocol (§7.7) and is otherwise not specified;
- other store backends (DynamoDB, Spanner).

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
                │ EntityWorkflow per entity │  decide ─▶ (activities: REST, Kafka, Saga) ─▶ commit in memory
                │ smType:instanceKey        │  ─▶ persist sm_state (local activity) ─▶ enqueue emitted events
                └─────────────┬─────────────┘
                              ▼
                     Postgres / Aurora DSQL
              processed_event · sm_state · event_trace
```

**Responsibilities:**
- **Coordinator:** deduplication, ordering, mutual exclusion over keys, delivery. It **MUST NOT** read payloads or
  entity state, and **MUST NOT** change entity state. One implementation serves every flow.
- **Worker:** the only component that decides transitions, mutates entity state and performs side effects.
- **Temporal:** holds all durable orchestration state (lock queues, entity state, pending activities). The store holds
  the dedupe set, a projection of entity state and traces.

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

The releasing activity sends `release(requestId)` to each lower key's lock **in parallel** and logs failures without
failing. Retry policy: start-to-close 30 s, backoff 50 ms to 10 s, unlimited attempts.

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

A transition that performs external calls (§11) can outlast the dispatch's 30 s start-to-close. The dispatch is then
retried, re-attaching to the running update, until its 1 h schedule-to-close. Transitions **SHOULD** stay well under
30 s so this path stays exceptional.

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

### 9.3 Handling an event

Updates **MUST** be decided strictly in update-acceptance order, even when a decision yields to call activities:

1. **Ticket.** On entry, before any yield, take ticket `t`. Wait until `run` has loaded state and
   `served == t`. An update can arrive in the same workflow task as the start, so the wait for `run` matters.
2. **Decide.** Look up `next(state, eventType)`:
   - **No edge:** the result is rejected (`no transition from S on E`); state is unchanged.
   - **Edge `S → T`:** call the type's decision. It **MAY** call activities (§11). It either rejects with a reason
     (state unchanged) or accepts with new data.
   - **Accepted:** set `state = T`, `data = new data`, `version += 1`, and append to `recent`.
3. **Release the ticket** (`served += 1`), so the next event can be decided.
4. **Persist.** Call the persistence local activity with the `sm_state` row (§12.4) and a trace row (`TRANSITION`
   or `REJECTED`). Start-to-close is 10 s, schedule-to-close 10 min.
5. **Post-persist.** If accepted, start the hand-off of emitted events (§10).
6. **Return** the `TransitionResult`. The update completes, the lock chain is released, and the next event on these
   keys may start.

Rules:
- Decisions **MUST** be deterministic. That means no I/O, no wall clock (use workflow time) and no randomness; side
  effects go through activities.
- A **rejection** leaves state, data and version unchanged.
- Activities already called before a rejection are **not undone automatically**. Undoing them is the job of a Saga
  (§11.5).
- For typed (model) machines the decision also covers:
  - binding the payload to the edge's payload type; JSON that does not bind is a rejection;
  - validating payload multiplicities;
  - validating the entity data after the transition. Invalid data rejects the event.

### 9.4 Continue-as-new

`run` waits until all of the following hold, then continues-as-new with `{state, data, version, recent, createdAtMillis}`:
- the run has handled **500** events, or Temporal suggests continuing;
- every update handler has finished;
- no work the run started is still pending: hand-offs of emitted events, and any asynchronous activities.

## 10. Worker: chained events

During a decision the machine **MAY** emit events to other entities, or to itself:
- explicitly (`emitEntity(smType, instanceKey, eventType, payload, extraLockKeys)`);
- or declaratively, with state-entry emissions from the model.

Rules:
- Emitted events are **buffered** and handed over **only if the transition is accepted**, after its state row is
  persisted. A rejected transition emits nothing.
- Child ids are `<eventId>.<n>`, with `parentEventId` set and `causationRoot` inherited. Replays and retries
  therefore produce the same ids.
- The hand-off is a **regular activity** (`EventEmitEnqueue`, unlimited retries). For each child it sends `acquire`
  of a first request on the child's `K0`, with the §6.2 rules. A duplicate is success.
  - It **MUST NOT** be a local activity: the emitting entity is inside an update that a lock chain is waiting on.
- Emitted events skip `processed_event`. They are deduplicated by the lock's validator and recent window, and by the
  entity's update id.
- A child's lock keys are the declared extra keys plus its own entity key.

## 11. Worker: external side effects and Sagas

A transition that writes to an external system does so through **activities**, called from the decision. The worker
registers the implementations on `sm-<smType>`. In the Java worker these come from `WorkerActivities` providers via
`ServiceLoader`, and the generated test registers fakes.

### 11.1 What Temporal guarantees, and what it does not

- An activity runs **at least once**:
  - it is retried after a failure, a timeout, or a worker crash;
  - once it has **completed**, its result is in workflow history and it is **never** executed again, whether on
    replay, after a worker crash, or on a repeated delivery of the event (§8.1).
- Temporal provides **no** distributed transaction across external systems: no XA, no two-phase commit. A write whose
  answer was lost is repeated by the retry.
- **Exactly-once effect therefore requires the receiver to deduplicate.** The rest of this section specifies how.

### 11.2 Idempotency keys

- Every external write **MUST** carry an idempotency key that is identical on every attempt, crash and replay.
- Keys **MUST** be derived from stable inputs: the instance key, or the event id plus a step name. They **MUST NOT**
  come from random UUIDs or wall-clock time.

Reference: `<paymentId>:charge`, `<paymentId>:refund`, `<paymentId>:settled`, `<paymentId>:reversed`.

### 11.3 REST services

**Requests:** send the key as the `Idempotency-Key` header.

**Classify the answers:**

| answer | classification |
|---|---|
| 2xx | the result |
| a business refusal (reference: 422) | **non-retryable** (`ApplicationFailure` with `nonRetryable`, type e.g. `Declined`): retrying cannot change it |
| 5xx, timeout, dropped connection | **retryable, outcome unknown**: the retry, with the same key, finds out |

**Timeouts:** a client request timeout shorter than the activity's start-to-close (reference: 4 s request, 5 s
start-to-close, 2 min schedule-to-close, backoff 0.5 s to 5 s). This bounds a hung call ("committed, answer never
came").

**Receiver contract**, which the service **MUST** implement for the scheme to be correct:
- **Atomic commit:** commit the effect and store the answer under the key **in one transaction**, serialized per key.
  The reference uses Postgres with `pg_advisory_xact_lock(hashtext(key))`, then the entry and answer inserts.
- **Replay:** answer a known key with the stored answer, marked `Idempotent-Replayed: true`, without committing again.
- **Safe compensation:** a compensating call for something that never happened (e.g. refund with nothing charged)
  **MUST** succeed as a no-op. Compensation can then run after any ambiguous failure.

### 11.4 Kafka

- **Producer:** `enable.idempotence=true` and `acks=all`. Its timeouts **MUST** end inside the activity's
  start-to-close (reference: request 3 s, delivery 8 s, start-to-close 12 s, schedule-to-close 40 s).
- The idempotent producer only removes duplicates of its **own internal retries within one producer session**. A
  Temporal retry is a new send, so after a lost acknowledgement **the record is written again**. Delivery is at least
  once.
- **Record identity:** every record **MUST** carry its idempotency key in an `idempotency-key` header. The record
  key is the business key (reference: the payment id, so a payment's records stay in one partition), and the header
  is `<id>:settled` or `<id>:reversed`. Consumers **MUST** treat a repeated idempotency key as a duplicate, keeping
  the first. That makes the *effect* once.
- A record cannot be withdrawn. To undo one, publish a compensating record (reference: type `REVERSED`, key
  `<id>:reversed`).

### 11.5 Sagas

A transition that performs several external writes **MUST** leave nothing half done:

1. **Register each compensation before the call it compensates.** A call that failed ambiguously (timeout, lost
   answer) may still have committed, so its undo must already be registered.
   Reference: register the refund, then charge; register the reversal, then publish.
2. On a **non-retryable business refusal** of a step that commits nothing (the decline), do **not** compensate.
3. On any **other failure** of a step (retries or schedule-to-close exhausted), run the compensations in reverse
   order.
4. **Compensations MUST retry until they succeed** (unlimited attempts). An entity can therefore stay in an
   intermediate state while a system it needs is down; this is intended.
5. **Report the outcome as state**, not as an exception:
   - the reference moves to an intermediate state (`SETTLING`);
   - it then emits a chained event to itself (§10): `confirm`, `decline` or `fail`, carrying the receipts and the
     reason;
   - the final state (`SETTLED`, `DECLINED`, `FAILED`) then says what happened, and the emitted event's id
     (`<eventId>.1`) makes the report idempotent.
6. **Do not reject after side effects without compensating.** A rejection does not undo activities (§9.3).

The external calls run inside the decision. That holds the entity's ticket, and therefore the event's lock chain, for
their duration. Keys touched by slow external calls serialize accordingly.

## 12. State store: Postgres and Aurora DSQL

### 12.1 Role

- Temporal is the source of truth for entity state.
- `sm_state` is a **projection**, written by the worker on every handled event and read by tools and tests.
- `processed_event` is the coordinator's dedupe set.
- `event_trace` is best-effort observability.

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

### 12.4 Projection write (`upsertState`)

One autocommit statement per handled event:

```sql
INSERT INTO sm_state (workflow_id, sm_type, state, data, version, last_event_id, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (workflow_id) DO UPDATE SET state = EXCLUDED.state, data = EXCLUDED.data, version = EXCLUDED.version,
  last_event_id = EXCLUDED.last_event_id, updated_at = EXCLUDED.updated_at
WHERE sm_state.version < EXCLUDED.version;
```

- **Monotonic by version:** retried or out-of-order writes converge on the newest version, and the write is
  idempotent.
- A **rejected** event writes the unchanged version, so it is a no-op for an existing row.
- On a brand-new entity whose first event is rejected, the insert creates a version-0 row in the initial state with
  `data` null.
- The trace row is not written in the same transaction; it goes through the batching trace writer.

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
| lock to entity | at least once; **applied once per entity run** | `handle` update id = event id (§17) |
| entity to store | at least once, converging | version-guarded upsert |
| entity to external system | at least once; **effect once** with receiver deduplication | idempotency keys, atomic commit and answer, consumer dedupe (§11) |
| entity to emitted event | at least once, deduplicated | child ids `<id>.<n>`, lock and entity dedupe |

### 13.2 Ordering

- **Per entity:** transitions are applied in the order their `handle` updates were accepted (§9.3).
- **Same first key:** events with the same `K0`, submitted in one source order, are applied in that order (§6, §7.3).
- **Shared later key only:** events that share a key but have different `K0` are **mutually exclusive** but not
  ordered by source. Their relative order is the order in which their chains reach the shared key. Design entities so
  their events share `K0` (§5).
- **Direct types:** ordered per instance by update acceptance, but not exclusive with multi-key events.

### 13.3 Crashes

| failure | effect |
|---|---|
| coordinator process killed | workflow tasks time out (`WORKFLOW_TASK_TIMEOUT_MS`, 2 s) and move to another coordinator. Local activities in flight are re-run; their calls are idempotent |
| worker process killed | entity workflow tasks and local activities move to another worker. An external activity times out (its start-to-close) and is retried with the same idempotency key |
| whole tier down | work queues durably in Temporal until a process returns; nothing is lost |
| store unavailable | dedupe fails the batch, which the adapter retries. Projection writes retry within their 10 min schedule-to-close. Past that the `handle` update fails, the lock records `FAILED` and moves on, and the projection lags until the next transition writes a newer version |
| external system unavailable | the activity retries until its schedule-to-close, then the Saga compensates. Compensations retry until the system returns |
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
| per-ecosystem activity settings | e.g. `LEDGER_URL`, `PAYMENTS_KAFKA_BOOTSTRAP` | worker | external systems (§11) |

## 16. Verification

A conforming implementation **SHOULD** pass the existing suites:
- **Unit and Temporal test-server suites:**
  - the lock protocol: `OrchestrationFlowTest` and `ForwardReleaseRaceTest` (`platform/orchestration`);
  - the entity workflow: `AbstractStateMachineTest`, `ModelStateMachineTest` and `LockTemplatesTest`
    (`core/worker-sdk`);
  - the store: `PostgresStoreTest` (`platform/store`);
  - Sagas against fakes: `SettlementMachineTest`, `LedgerServiceTest` and `LedgerPostgresIT` (`showcases/payments`);
  - key order (UTF-16), wire JSON compatibility and the lock queue logic: the Rust coordinator's unit tests
    (`cargo test` in `platform/coordinator-rs`).
- **`scripts/chaos.sh`:** kills and restarts coordinators and workers, and optionally Temporal, under load. It
  verifies no lost events, no double application and per-key order. `RUST_COORDINATOR=1` mixes in the Rust
  coordinator.
- **`scripts/payments-chaos.sh`:** external side effects under ambiguous commits, lost Kafka acks, worker kills,
  ledger restarts and Kafka outages, plus a no-idempotency control. It verifies that every entity is consistent with
  the ledger and the topic (results: `docs/external-side-effects.md`).

## 17. Known limitations

- **Update-id deduplication is per run.** Temporal deduplicates `handle` update ids within one entity workflow run.
  The duplicate window is narrow: a dispatch is retried after the update completed but before the coordinator
  recorded the result, and the entity continued-as-new in between. An event could then be applied again in the new
  run. The carried `recent` window (50 transitions with event ids) is available for a guard. A reimplementation
  **SHOULD** reject an event whose id is in `recent`; the reference does not yet do so.
- **Mutual exclusion is not causal ordering.** See §13.2 for events that share only a later key.
- **Side-effect calls hold the keys.** External calls inside a decision block the entity and its lock chain for their
  duration (§11.5).
- **The projection lags on store outages** longer than the persistence schedule-to-close (§13.3).

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
