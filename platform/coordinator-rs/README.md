# coordinator-rs: the coordinator in Rust

A drop-in replacement for the Java coordinator (`platform/orchestration`), written with the
[Temporal Rust SDK](https://github.com/temporalio/sdk-rust) (1.1). It does the same job: it reads Kinesis,
removes duplicate events, serializes events per key through FIFO lock chains and hands each event to its
worker. It never reads payloads or entity state. Only the workers (`worker-sdk`, unchanged) run transitions
and mutate state, so this code is the same for every flow.

```
Kinesis ─▶ ConsumeShard (1 per shard) ─▶ dedupe (store) ─▶ group by first lock key ─▶ acquire on lock:<K0>
            IngestSupervisorWorkflow                                                   KeyLockWorkflow chain
                                                                    last key ─▶ EntityWorkflow.handle  (Java worker, sm-<type>)
                                                                             └▶ EventProcessorWorkflow.process (Java, ev-<domain>)
```

## What stays the same (the contract with the Java code)

| | |
|---|---|
| Flows | `CONCERT_FLOW=<flow>` (`io.concert.common.Flows`): stream `concert-<flow>` (created if missing, `INGEST_CREATE_STREAM`, `KINESIS_SHARDS`), task queue `orchestration-<flow>`, lock ids `lock:<flow>:<key>`; unset = `concert-events`, `orchestration`, `lock:<key>` |
| Workflow types | `KeyLockWorkflow` (input `LockState`; updates `acquire` with its validator, `release`, `unblock`; query `snapshot`), `IngestSupervisorWorkflow` (input `IngestConfig`; query `status`) |
| Duplicate signal | the `acquire` validator fails with `duplicate request`, which `EventRouting.enqueueLock` matches |
| Calls into workers | UpdateWithStart `EntityWorkflow.handle` (start input `EntityInit`, update id = event id) and `EventProcessorWorkflow.process` / `schedule`, with the same workflow ids, task queues and search attributes |
| Activities | the Java names and argument lists (`DispatchToEntity(event, lockWaitMs)`, `ConsumeShard(stream, shard, position, minutes)`, ...) |
| Payloads | the Jackson JSON of the `core/common` records; unknown event fields are passed through unchanged |
| Lock key order | Java `String.compareTo` (UTF-16), so chains built by Java emitters and this coordinator never deadlock |
| Trace sampling | the same `TRACE_SAMPLE` hash as `TraceSampling`, so every process samples the same events |
| Store | the same tables and items as `JdbcStateStore` / `DynamoStateStore` (`processed_event`, `shard_checkpoint`, `event_trace`); `sm_state` is never touched |
| Environment | the same variables and defaults (`CONCERT_FLOW`, `TEMPORAL_ADDRESS`, `STORE_KIND`, `STORE_JDBC_URL`, `KINESIS_ENDPOINT`, `DIRECT_SM_TYPES`, `TRACE_SAMPLE`, `INGEST_*`, `WORKFLOW_TASK_TIMEOUT_MS`, ...) |

Stores: `STORE_KIND=postgres` (default), `dsql` (TLS with an IAM token minted for each connection) and
`dynamo`. Spanner stays on the Java coordinator.

## Run

```bash
cd platform/coordinator-rs && cargo build --release
CONCERT_FLOW=samples STORE_KIND=postgres KINESIS_ENDPOINT=http://localhost:4566 ./target/release/concert-coordinator
```

With Docker: `scripts/up.sh postgres --rust-coordinator` (or set `COORDINATOR_TARGET=coordinator-rs` for
`docker compose`). Every flow's coordinator service (`samples-coordinator`, `trading-coordinator`, the generated
`<ecosystem>-coordinator`) then builds the `coordinator-rs` image target, and so does the trace UI's Start flow.
Scaling (`--scale samples-coordinator=3`) works the same way.

Chaos test with Rust coordinators and Java workers: `RUST_COORDINATOR=1 scripts/chaos.sh`.

## Switching an existing deployment

The two implementations share every external interface, but they cannot replay each other's workflow
histories. Run one implementation per Temporal namespace at a time:

Per flow (each flow has its own task queue, so flows can switch one at a time):

1. Stop the flow's producers, or let its stream drain.
2. Stop the flow's Java coordinators. Lock runs finish on their own after `LOCK_IDLE_SECONDS` (600 s) without
   traffic; `temporal workflow list --query 'TaskQueue="orchestration-<flow>"'` should show only the ingest
   supervisor. Terminate `ingest:concert-<flow>`: the new coordinator starts a fresh supervisor, and its
   consumers resume from the checkpoints in the store.
3. Start the Rust coordinators.

Activities can cross over during the switch (same names and arguments), but workflow tasks cannot.

## Layout

| File | Java counterpart |
|---|---|
| `src/model.rs` | `EventEnvelope`, `LockRequest`, `LockState`, ... (`core/common`) |
| `src/workflows/key_lock.rs` | `KeyLockWorkflowImpl` |
| `src/workflows/ingest.rs` | `IngestSupervisorWorkflowImpl` |
| `src/activities.rs` | `DispatchActivitiesImpl`, `ShardConsumerActivitiesImpl` |
| `src/dispatcher.rs` | `IngestDispatcher` |
| `src/routing.rs` | `EventRouting` |
| `src/store/` | `JdbcStateStore` (postgres, dsql), `DynamoStateStore` |
| `src/trace.rs` | `TraceWriter` |

`cargo test` runs the unit tests: wire JSON, lock key order, sampling, the store SQL helpers, dedupe semantics
and the lock queue logic.
