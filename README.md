# concert-temporal

Low-latency state machine orchestration on [Temporal](https://temporal.io): events from Kinesis are
deduplicated (state in Aurora DSQL), serialized across **multiple keys per event**, and applied to
per-entity state machines written as Java Temporal workflows. Everything — including the
"coordinator" — runs as Temporal workflows/activities; there is no bespoke lease or peer protocol.

```
Kinesis ─▶ ShardConsumer activity (1 per shard, heartbeating, checkpoint in heartbeat + DSQL)
             │ batch dedupe: INSERT … ON CONFLICT DO NOTHING (DSQL) → duplicates dropped
             │ group by first lock key; groups in parallel, in-order within a group
             ▼
           acquire update (wait ACCEPTED) on lock:<K0>          [task queue: orchestration]
             │  KeyLockWorkflow = FIFO per key
             │  head, not last key  → forward (acquire update) to lock:<K1>, keep holding K0
             │  head, last key      → UpdateWithStart entity.handle(event)   (local activity)
             │                        then release K0..Kn-2 (async activity)
             ▼
           EntityWorkflow smType:instanceKey                      [task queue: sm-<type>]
             transition (atomic) → onTransition side effects → project to DSQL sm_state
```

* **Ordering**: events sharing any key never run concurrently; per-shard order is kept on each
  event's first sorted key. Keys are always acquired in global sorted order → deadlock-free.
* **Dedupe** at three levels: DSQL `processed_event`, lock request ids, entity update ids.
* **Failover**: kill any orchestration worker; Temporal retries its shard consumers elsewhere from
  the last heartbeat checkpoint.
* **Why updates, not signals**: on Temporal, an update reaches the worker directly; a signal goes
  through the history transfer queue first. Measured here: signal→local-activity p50 28 ms vs
  update round trip p50 9 ms. Every lock interaction is an update.

## Modules

| module | what |
|---|---|
| `common` | `EventEnvelope`, workflow/activity interfaces, env + Temporal client helpers, worker tuning |
| `store` | `StateStore` (JDBC, DSQL-compatible SQL), Postgres stand-in / Aurora DSQL, async `TraceWriter` |
| `worker-sdk` | `AbstractStateMachine` (+ `StateMachineSpec`, `onTransition` hook), `WorkerBootstrap` |
| `orchestration` | ingest supervisor, shard consumer, `KeyLockWorkflow`, dispatch activities, worker main |
| `sample-workers` | `order`, `payment`, `shipment`, `ledger` machines; `SM_TYPE=order ./gradlew :sample-workers:run` |
| `integration-tests` | Testcontainers end-to-end tests + latency / scaling benchmarks |

## Writing a state machine

```java
public class OrderStateMachine extends AbstractStateMachine {
    static final StateMachineSpec SPEC = StateMachineSpec.startingAt("CREATED")
            .on("CREATED", "pay", "PAID")
            .on("PAID", "ship", "SHIPPED")
            .build();

    @Override protected StateMachineSpec spec() { return SPEC; }

    @Override protected void onTransition(String from, String to, EventEnvelope e) {
        // call activities for side effects; locks are held until this returns
    }
}
```

Event (JSON on Kinesis; partition key should be the first sorted lock key):

```json
{"eventId":"e-1","smType":"order","instanceKey":"123","eventType":"pay",
 "lockKeys":["account:9"],"payload":"{...}","sourceTsMillis":1759600000000}
```

## Run locally

Requires JDK 25 (`/opt/homebrew/opt/openjdk@25`) and Docker.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@25
docker compose up -d                       # LocalStack, Temporal (+UI :8080), Postgres (DSQL stand-in :5433)
./gradlew :orchestration:run &             # coordinator(s): run as many as you like
SM_TYPE=order ./gradlew :sample-workers:run &
```

## Tests and benchmarks

```bash
./gradlew test                                          # unit + end-to-end (Docker)
./gradlew :integration-tests:test -Pbench --tests '*LatencyBench' -Dbench.rates=100,500,1000
./gradlew :integration-tests:test -Pbench --tests '*ScalingBench'
```

## Chaos testing

`scripts/chaos.sh` runs coordinators and workers as real processes against the compose stack,
randomly kills them (`kill -9`, `SIGTERM`, a whole tier, optionally the Temporal server) while a
load generator publishes ledger events with per-key sequence numbers, then verifies the end state
in the DSQL stand-in: no lost events, no double applies, per-key order kept.

```bash
scripts/chaos.sh                                            # 180 s @ 100/s, chaos every 15 s
DURATION=300 RATE=200 KILL_EVERY=10 CHAOS_TEMPORAL=1 scripts/chaos.sh
```

Each run writes `build/chaos/<run-id>/` (process logs, `chaos.log`, `manifest.json`, `report.txt`);
the report includes a throughput timeline with every chaos action marked.

Failover is Temporal's own; these settings (`common/.../Failover.java`) control how fast it kicks in:

| var | default | Temporal default | effect |
|---|---|---|---|
| `WORKFLOW_TASK_TIMEOUT_MS` | 2000 | 10000 | in-flight workflow task on a dead worker retried elsewhere |
| `STICKY_SCHEDULE_TO_START_MS` | 1000 | 5000 | cached workflow leaves a dead worker's sticky queue |
| `SHARD_HEARTBEAT_TIMEOUT_SEC` | 3 | — | dead coordinator's shard consumers restart elsewhere |

Lower means faster failover but more redone (idempotent) work when a process merely pauses.

## Configuration (env)

| var | default | |
|---|---|---|
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | `localhost:7233` / `default` | |
| `KINESIS_ENDPOINT` | — | LocalStack: `http://localhost:4566`; unset for AWS |
| `INGEST_STREAM` / `INGEST_POSITION` | `concert-events` / `TRIM_HORIZON` | |
| `STORE_KIND` | `postgres` | `dsql` uses the AWS DSQL JDBC connector (IAM tokens) |
| `STORE_JDBC_URL` | `jdbc:postgresql://localhost:5433/concert` | DSQL: `jdbc:aws-dsql:postgresql://<endpoint>/postgres` |
| `LOCK_IDLE_SECONDS` | `600` | how long an idle lock workflow stays warm |
| `DIRECT_SM_TYPES` | — | types allowed to bypass locks (only if they never share keys) |
| `DEDUPE_TTL_HOURS` | `24` | |
| `TEMPORAL_SEARCH_ATTRIBUTES` | `true` | `EventId`, `SmType`, `InstanceKey`, `LockKeys` |

## Known limits

* Hot keys cap throughput per key: one lock workflow serializes every event on that key.
* A multi-key event can be overtaken on a *secondary* key by a later event whose first key it is.
  Producers needing strict cross-key order should use the same partition key for related events.
* `DsqlStore` path (`STORE_KIND=dsql`) is untested against a real cluster.
