# External side effects: idempotent writes when the commit's answer is lost

The worker in this example writes to two external systems from inside a state machine transition:
- a REST service, a sample ledger that charges accounts;
- Kafka, a settlement record on a new topic, `payments.settled`.

Each write can commit while its answer never comes back, which is the classic two-phase-commit problem. The example
shows what Temporal handles out of the box and what you add: idempotency keys and a Saga. The chaos scenarios check
that every payment ends up consistent with both systems.

- models: [`examples/payments/payment.pure`](../examples/payments/payment.pure) (smType `settlement`)
- machine: [`SettlementMachine`](../showcases/payments/src/main/java/io/concert/eco/payments/SettlementMachine.java)
- activities: [`external/`](../showcases/payments/src/main/java/io/concert/eco/payments/external): the ledger client,
  the Kafka publisher, the sample ledger service (`LedgerService`) and in-memory fakes for tests
- chaos: [`scripts/payments-chaos.sh`](../scripts/payments-chaos.sh) and the verifier
  [`PaymentsChaosCheck`](../showcases/payments/src/main/java/io/concert/eco/payments/PaymentsChaosCheck.java)

## The flow

```
settle ─▶ SETTLING ── charge (REST, Idempotency-Key <id>:charge) ── publish (Kafka, key <id>, header <id>:settled) ──▶ confirm ─▶ SETTLED
                     └ 422 refused ─────────────────────────────────────────────────────────────────────────────────▶ decline ─▶ DECLINED
                     └ cannot complete in time ─▶ Saga: publish REVERSED, refund (both retried until done) ─────────────▶ fail    ─▶ FAILED
```

The `settle` transition calls both systems as activities and sends the outcome back to the payment as its next
event (`confirm`, `decline` or `fail`), so the final state always says what happened. The outcome event's id is
`<settle event id>.1`, so a retry or replay cannot send it twice.

## What Temporal gives you out of the box

| problem | Temporal feature | in this example |
|---|---|---|
| the service failed or answered 500 | activity retries with backoff (`RetryOptions`) | charge: 0.5 s to 5 s backoff, for up to 2 min; publish: 1 s to 5 s, for up to 40 s |
| the service committed but never answered | **start-to-close timeout**: the attempt is abandoned and retried | charge: 5 s per attempt (the ledger's hang lasts 8 s) |
| the worker died mid-call | the activity times out and is retried on another worker | `worker-kill` scenario |
| an error that retrying cannot fix | non-retryable failures (`ApplicationFailure.newNonRetryableFailure`) | the ledger's 422 becomes `Declined`, with no retry |
| redoing work that already completed | **durable history**: a completed activity's result is recorded and never re-executed on replay, crash or redelivery | a crash after the charge completed never charges again |
| the same event delivered twice | workflow update ids (the event id) deduplicate the transition | `aRetriedSettleEventIsAppliedOnce` |
| undoing a half-done multi-step write | `io.temporal.workflow.Saga` | refund plus reversal record |
| knowing how often it retried | `Activity.getExecutionContext().getInfo().getAttempt()` | `ledgerAttempts` / `kafkaAttempts` on the payment |

**What Temporal does not do: exactly-once side effects.** It is not a distributed transaction coordinator (no
XA, no 2PC across the ledger and Kafka). An activity runs *at least once*, so after an ambiguous failure the
retry repeats the write. Exactly-once *effect* comes from the receiver deduplicating:

- **REST:** every request carries `Idempotency-Key`, derived from the payment id, which is the same on every attempt,
  crash and replay. The ledger commits a key once and answers repeats with the stored result
  (`Idempotent-Replayed: true`). So the retry doubles as the status check: "did my charge commit?" is answered
  by sending it again.
- **Kafka:** the producer is idempotent (`enable.idempotence`, `acks=all`), but that only removes duplicates from the
  producer's *own* internal retries within one session. A Temporal retry is a new send, and Kafka cannot tell it apart
  from a new record. Records therefore carry their idempotency key (record key = payment id, header
  `idempotency-key`), and consumers keep the first record per key. Delivery is at least once; the effect is once.
  (Alternatives: a transactional outbox, or a consumer that upserts by key.)
- **Compensations are registered before each ambiguous call**, not after it succeeds. A charge that timed out may
  still have committed, so its refund is registered first. A refund when nothing was charged is a no-op, which makes
  compensation safe to run. In the same way, a reversal record is registered before the publish. A Kafka record
  cannot be taken back, only answered with a REVERSED record.
- **The receiver's commit and its stored answer are one transaction.** The ledger keeps both in Postgres
  (`ledger_*` tables) and writes them together under a per-key lock. A crash therefore never leaves a charge without its
  stored answer: after a restart a key is either unknown, so the retry commits it, or known, so the retry gets the
  stored answer. The `ledger-restart` scenario kills the ledger between commit and answer to check exactly this.
- **Compensations retry until they succeed** (`setMaximumAttempts(0)`). A refund must not be given up on, so a payment
  can wait in SETTLING until the systems it needs come back.

## Activities in a generated ecosystem

Activities are registered through `io.concert.sdk.WorkerActivities`, discovered with `ServiceLoader`
([`PaymentsActivities`](../showcases/payments/src/main/java/io/concert/eco/payments/PaymentsActivities.java),
listed in `META-INF/services/io.concert.sdk.WorkerActivities`):
- `create(smType)`: the real clients. The generated worker registers them on `sm-<smType>`.
- `forTests(smType)`: fakes. The generated test registers them, so its happy path runs without the services.

Machines create the stubs as fields (`Workflow.newActivityStub(...)`) and call them from `onTransition`.

`showcases/payments/compose.extra.yml` is the hand-written companion of the generated `compose.yml`.
`./deploy-concert-ecosystem` and the dashboard add it automatically. It adds three things:
- the ledger service, storing its state in the platform's Postgres (`LEDGER_JDBC_URL`; without it the ledger keeps
  state in memory);
- the worker's `LEDGER_URL`;
- Kafka for the `payments` profile, so the flow runs with `--no-analytics` too.

## Running it

```bash
./deploy-concert-ecosystem payments --no-analytics
```

```bash
./showcases/payments/send-flow all
```

```bash
scripts/payments-chaos.sh
```

The verifier sends `COUNT` payments per scenario (every 10th is over the ledger's limit, so it must be declined).
It waits for every payment to reach an outcome, then checks each one against the ledger (`GET :8097/charges`)
and the topic:

| outcome | must hold |
|---|---|
| SETTLED | exactly one live charge; a settlement record in Kafka; no reversal; within the limit |
| DECLINED | nothing committed; no settlement record; over the limit |
| FAILED | no live charge (refunded); a reversal for any settlement record that was written |
| any | an outcome (none stuck in NEW / SETTLING); at most one ledger commit |

Scenarios:

| scenario | fault |
|---|---|
| `baseline` | none |
| `commit-then-500` | the ledger commits, then answers 500 (on `FAULT_RATE` = 40% of requests, retries included) |
| `commit-then-timeout` | the ledger commits, then answers after 8 s; the worker gives up after 4 s |
| `commit-then-drop` | the ledger commits, then drops the connection without answering |
| `fail-before-commit` | the ledger answers 503 without committing |
| `kafka-ack-loss` | Kafka writes the record and the ack is lost (simulated in the worker after the real write) |
| `worker-kill` | `kill -9` a payments worker every 6 s |
| `ledger-restart` | the ledger answers 400 ms after committing and is `kill -9`'d every 4 s, then restarted (80 payments) |
| `kafka-pause` | Kafka frozen (`docker pause`) for 15 s: publishes retry until it is back |
| `kafka-outage` | Kafka frozen for 70 s, past the 40 s publish budget: the Saga refunds and reverses |
| `storm` | commit-then-drop and ack loss (30% each) plus worker kills |
| `control-no-idempotency` | commit-then-500 with the ledger's idempotency keys **off**: double charges are expected, and the run passes when the verifier finds them |

Faults can be switched by hand too:

```bash
curl -X POST localhost:8097/chaos -d '{"mode":"commit-then-timeout","rate":0.5}'
```

```bash
curl -X POST localhost:8097/chaos -d '{"kafkaAckLossRate":0.5}'
```

```bash
curl -X POST localhost:8097/chaos -d '{"mode":"none","rate":0,"kafkaAckLossRate":0,"idempotency":true,"latencyMs":0}'
```

Tests:
- `./gradlew :payments:test`: the machine against in-memory fakes (lost answers, lost acks, decline, Saga), and the
  ledger over HTTP.
- `./gradlew :payments:integrationTest`: the Postgres ledger across a restart (needs the local stack's Postgres).

## Results

<!-- results -->
