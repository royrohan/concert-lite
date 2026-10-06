# ecosystem-gen

Turns Pure models whose state machines (`concert::sm`) and / or event-style event types (`concert::event`) are
declared **in the models themselves** into a runnable showcase module (`showcases/<name>`): typed POJOs, specs and
machine stubs, event catalogs and handler stubs, workers, event sender, sample payloads and flows, compose override
and analytics config.

```bash
./generate-concert-ecosystem examples/insurance --name insurance   # [--package io.concert.eco.insurance] [--force]
./generate-concert-ecosystem examples/order-events --name order-events   # event style
./generate-concert-ecosystem check examples/insurance             # validate only
./deploy-concert-ecosystem insurance --store spanner              # [--no-analytics]
./showcases/insurance/send-flow all
```

Both scripts are thin wrappers (bash 3.2 compatible) over `EcosystemCli` (`generate`, `check`, `summary`).

## Declaring a state machine

The `concert::sm` profile is built in (`core/model-pure/src/main/resources/concert/concert.pure`; it is copied
into every module as `src/main/pure/concert/concert.pure`, so don't add one to your models dir):

```
Profile concert::sm
{
  stereotypes: [root, command];
  tags: [type, initial, transitions, terminal, locks, statusProperty, idProperty];
}
```

Mark the aggregate root with `<<concert::sm.root>>` and declare the machine in its tagged values; mark
payload classes `<<concert::sm.command>>`:

```
Class <<concert::sm.root>>
{
  concert::sm.type = 'claim',
  concert::sm.initial = 'NEW',
  concert::sm.transitions = '
    NEW      -file->    OPEN     : FileClaim;
    OPEN     -assess->  ASSESSED : AssessClaim;
    ASSESSED -approve-> APPROVED : ApproveClaim;
    ASSESSED -reject->  REJECTED : RejectClaim;
    APPROVED -pay->     PAID     : PayClaim',
  concert::sm.locks = '
    file, approve: policy:{policyId};
    pay: policy:{policyId}, payee:{payeeAccount}',
  concert::sm.statusProperty = 'status',
  concert::sm.idProperty = 'claimId'
}
insurance::Claim { claimId: String[1]; status: ClaimStatus[1]; policyId: String[0..1]; ... }

Class <<concert::sm.command>> insurance::FileClaim { policyId: String[1]; ... }
```

| tag | value | default |
|---|---|---|
| `type` | smType, `[a-z][a-z0-9_]*`, unique (task queue `sm-<type>`, workflow ids `<type>:<key>`) | required |
| `transitions` | entries `FROM -event-> TO [: Payload]`, separated by `;` or new lines; `A \| B -event-> C` lists sources; tag may repeat | required |
| `initial` | initial state | first transition's source |
| `terminal` | comma-separated states | states without outgoing transitions |
| `locks` | entries `event[, event] : template[, template]`, `*` = every event; `{id}` = instance key, `{field}` = top-level payload field | no extra locks |
| `statusProperty` | to-one enum property kept equal to the state (enum values must cover the states) | `status` if it is an enum |
| `idProperty` | String property set to the instance key | `<rootName>Id`, then `id` |

A payload name resolves in the root's package first, then as a unique simple name (or write it qualified).
Lock templates follow the platform rule that every event of an entity should share its *first sorted* lock
key (the Kinesis partition key and the per-shard ordering key): the validator warns when they don't.

**Validation** reports every problem with `file:line:col`, pointing inside the tagged value: unknown payload
classes (or enums), unknown states in `initial` / `terminal`, unreachable states, malformed entries, duplicate
transitions or smTypes, lock events that don't exist, lock fields missing on (or optional / non-scalar in) a
payload class, untyped events used by `{field}` templates, status enum mismatches (state not an enum value =
error, unused value = warning), payloads not marked `<<concert::sm.command>>` (warning), tags without
`<<concert::sm.root>>`, plus all parse and resolver errors of the models.

## Declaring event types (`concert::event`)

The `concert::event` profile is built in next to `concert::sm` (same `concert/concert.pure`):

```
Profile concert::event
{
  stereotypes: [event, state];
  tags: [domain, locks, onError, retries, key, name, emits];
}
```

Mark each event (payload) class `<<concert::event.event>>` and each keyed-state class `<<concert::event.state>>`:

```
Class <<concert::event.event>>
{
  concert::event.domain = 'orders',
  concert::event.locks = 'order:{orderId}, customer:{customerId}',
  concert::event.emits = 'OrderAcceptedEvent, OrderRejectedEvent'
}
orders::OrderCreateEvent { orderId: String[1]; customerId: String[1]; lines: OrderLine[1..*]; ... }

Class <<concert::event.event>>
{
  concert::event.domain = 'inventory',
  concert::event.locks = 'sku:{sku}',
  concert::event.onError = 'NON_BLOCKING',
  concert::event.retries = '2'
}
inventory::ReserveInventoryEvent { orderId: String[1]; sku: String[1]; quantity: Integer[1]; }

Class <<concert::event.state>> { concert::event.key = 'order:{orderId}' } orders::Order { ... }
```

| tag | on | value | default |
|---|---|---|---|
| `domain` | event | handler application, `[a-z][a-z0-9_]*`: task queue `ev-<domain>`, processors `evproc:<domain>:<key>` | required |
| `locks` | event | lock key templates separated by `,` `;` or new lines; `{field}` = to-one primitive / enum payload field, `{id}` = event id; the processor is the first sorted key's | `<domain>:<eventId>` |
| `onError` | event | `BLOCKING` (keys stay held) or `NON_BLOCKING` (parked, keys released) once the attempts are exhausted | `BLOCKING` |
| `retries` | event | handler retries after the first attempt, `0..20` | `2` (3 attempts) |
| `emits` | event | event types (or event class names) the handler may emit: flow diagrams, chained-flow samples, docs (not enforced) | none |
| `name` | event, state | the `eventType` on the wire / the state type name | the class's simple name |
| `key` | state | key template from the state's own fields, e.g. `order:{orderId}` (rows `state:<key>`) | none: handlers pass the key to `ctx.save(key, state)` |

**Validation** (`file:line:col`, inside the tagged value): missing or malformed domain, lock / key template fields
that are not properties of the class or not to-one scalars (optional ones warn), unbalanced braces, invalid
`onError` / `retries`, tags that do not apply (e.g. `key` on an event), tags without the stereotype, a class marked
both event and state or event and `<<concert::sm.root>>`, duplicate event / state names (also after snake-casing,
as they name the analytics tables), an event named like an smType, `emits` entries that name no event, and a
warning for a state key no event can lock (handlers may only touch state under their event's lock keys).

A model may declare machines, events or both; `check` prints what it found.

## What gets generated

| file | owner |
|---|---|
| `showcases/<name>/src/main/pure/*.pure` (+ `concert/concert.pure`) | copies, regenerated (stale copies deleted) |
| `<Name>Models` (`@LegendModel`: POJOs via the model-codegen processor) | regenerated |
| `<Root>Spec`: states, transitions, payload classes, terminal states, `LOCKS` templates | regenerated |
| `<Root>Machine extends ModelStateMachine<Root>`: `initialData`, `switch` per payload with `// TODO`s and field copies | **yours**: only written when absent; `--force` backs it up as `.bak-<timestamp>` |
| `<Name>Machines` (registry + `MachineCatalog` SPI), `<Name>WorkerMain`, `<Name>Events` (Kinesis sender, both styles) | regenerated |
| `<Domain>EventCatalog` per domain (`EventCatalog` SPI: types, lock templates, onError, attempts, emits, states, diagrams) | regenerated |
| `<Event>Handler implements EventHandler<Event>`: `@Handles`, `// TODO` with `ctx.state` / `ctx.save` / `ctx.emit` / `ctx.emitAt` / error examples | **yours**: only written when absent; `--force` backs it up as `.bak-<timestamp>` |
| `<Name>EventTypes` (catalogs, handlers per domain, flow + model Mermaid), `<Name>EventWorkerMain` (one worker per domain, `bin/ecosystem-event-worker`) | regenerated |
| `<Name>EventFlowsTest`: event samples bind; every `samples/event-flows/*.json` settles through your handlers on the real lock chain (optional `"expect": {"Type": "STATUS"}`) | regenerated |
| `samples/events/<EventType>.json`, `samples/event-flows/<RootType>-flow.json` (one per event no other emits, chain from `emits`) | regenerated unless you edited them; extra flows you add are kept |
| `<Name>GeneratedTest`: samples bind to their classes; every happy path reaches a terminal state through your machines | regenerated |
| `samples/<smType>/<event>.json`, `samples/flows/<smType>-happy-path.json` | regenerated unless you edited them (hashes in `ecosystem.json`) |
| `build.gradle.kts` (+ your optional `extra.gradle.kts`), `compose.yml`, `send`, `send-flow`, `README.md`, `ecosystem.json` | regenerated |
| `infra/deephaven/app.d/ecosystems/<name>.py` | regenerated |
| `settings.gradle.kts` | one line between `// <concert-ecosystems>` markers, added once |

Samples: values by type and name (amounts, prices, dates, emails, currencies, enum defaults or first values,
nested objects, one-element lists); a field named like a machine's id property (e.g. `policyId`) holds that
machine's sample key (`POLICY-1001`), so the events of an ecosystem address and lock consistent entities. The
happy path is the shortest event sequence to the farthest terminal state, preferring non-failure names.

Event sender (`./showcases/<name>/send`): `send <EventType> [payload] [--at +5m|ISO] [--id ID]` sends an
event-style envelope (`style: "event"`, lock keys rendered from the type's templates, `scheduledAt` from `--at`);
without a payload it sends `samples/events/<EventType>.json`. Flow files mix both styles: an entry with
`"style": "event"` takes `eventType`, `payload` and optionally `at` / `id`.

## How it plugs into the platform

- **Trace UI**: modules provide their machines through the `io.concert.sdk.MachineCatalog` SPI and their event
  types through `io.concert.sdk.events.EventCatalog` (Events tab: types, flow diagrams, payload models; event pages);
  the trace UI build adds every project with an `ecosystem.json` as a runtime dependency, so a new ecosystem
  needs no trace UI edit (rebuild the image: `deploy` does).
- **Worker image**: the generic `showcase-worker` Dockerfile target, parameterised by `MODULE` / `MODULE_DIR`;
  the event worker service (`<name>-event-worker`) runs the same image with entrypoint `bin/ecosystem-event-worker`
  (`<NAME>_EVENT_DOMAINS` restricts the domains).
- **Event rows in the sinks**: `SINK_ROOTS_<NAME>` also lists `evt_<type>:<event class>` (lifecycle rows: the sinks
  add the lifecycle columns, then the payload class's columns read below `payload`) and `st_<type>:<state class>`.
- **Sinks**: the override adds `SINK_ROOTS_<NAME>` / `MODELS_DIR_<NAME>` (appended to `SINK_ROOTS` /
  `MODELS_DIR` by the DuckDB sink and the ClickHouse provisioner) and bind-mounts the module's models, so
  overrides of several ecosystems compose. All deployed models are resolved together: don't deploy two
  ecosystems that define the same Pure names (the generator warns).
- **Deephaven**: `concert.py` execs `app.d/ecosystems/<name>.py` when `CONCERT_ECOSYSTEM_<NAME>` is set (the
  override sets it): `<name>_<table>_latest` per root, `<name>_latest`, `<name>_state_distribution`,
  `<name>_completions_per_minute`; for events `<name>_events_latest`, `_events_by_status`, `_event_errors`,
  `_events_scheduled`, `_causation_depth` and `<name>_st_<type>s_latest` per keyed state.

## Examples

- `examples/order-events` → `showcases/order-events`: event style only, domains `orders` and `inventory` (handlers
  implemented by hand; extra flows with expectations in `samples/event-flows/order-*.json`, `payment-poison.json`).
- `examples/insurance` → `showcases/insurance`: Policy, Claim, Payout (stubs filled in by hand).
- `examples/trading-gen` → `showcases/trading-gen`: the trading models annotated to match the hand-written
  `TradingFlows` (smTypes prefixed `gen_`); `showcases/trading`'s `GeneratedTradingEquivalenceTest` asserts
  states, transitions, payload classes, terminal states and lock templates are equal. Not deployed (it shares
  its Pure names with `trading-model`).
