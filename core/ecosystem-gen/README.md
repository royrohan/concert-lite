# ecosystem-gen

Turns Pure models whose state machines are declared **in the models themselves** into a runnable showcase
module (`showcases/<name>`): typed POJOs, specs, worker stubs, worker main, event sender, sample payloads,
compose override and analytics config.

```bash
./generate-concert-ecosystem examples/insurance --name insurance   # [--package io.concert.eco.insurance] [--force]
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

## What gets generated

| file | owner |
|---|---|
| `showcases/<name>/src/main/pure/*.pure` (+ `concert/concert.pure`) | copies, regenerated (stale copies deleted) |
| `<Name>Models` (`@LegendModel`: POJOs via the model-codegen processor) | regenerated |
| `<Root>Spec`: states, transitions, payload classes, terminal states, `LOCKS` templates | regenerated |
| `<Root>Machine extends ModelStateMachine<Root>`: `initialData`, `switch` per payload with `// TODO`s and field copies | **yours**: only written when absent; `--force` backs it up as `.bak-<timestamp>` |
| `<Name>Machines` (registry + `MachineCatalog` SPI), `<Name>WorkerMain`, `<Name>Events` (Kinesis sender) | regenerated |
| `<Name>GeneratedTest`: samples bind to their classes; every happy path reaches a terminal state through your machines | regenerated |
| `samples/<smType>/<event>.json`, `samples/flows/<smType>-happy-path.json` | regenerated unless you edited them (hashes in `ecosystem.json`) |
| `build.gradle.kts` (+ your optional `extra.gradle.kts`), `compose.yml`, `send`, `send-flow`, `README.md`, `ecosystem.json` | regenerated |
| `infra/deephaven/app.d/ecosystems/<name>.py` | regenerated |
| `settings.gradle.kts` | one line between `// <concert-ecosystems>` markers, added once |

Samples: values by type and name (amounts, prices, dates, emails, currencies, enum defaults or first values,
nested objects, one-element lists); a field named like a machine's id property (e.g. `policyId`) holds that
machine's sample key (`POLICY-1001`), so the events of an ecosystem address and lock consistent entities. The
happy path is the shortest event sequence to the farthest terminal state, preferring non-failure names.

## How it plugs into the platform

- **Trace UI**: modules provide their machines through the `io.concert.sdk.MachineCatalog` SPI (ServiceLoader);
  the trace UI build adds every project with an `ecosystem.json` as a runtime dependency, so a new ecosystem
  needs no trace UI edit (rebuild the image: `deploy` does).
- **Worker image**: the generic `showcase-worker` Dockerfile target, parameterised by `MODULE` / `MODULE_DIR`.
- **Sinks**: the override adds `SINK_ROOTS_<NAME>` / `MODELS_DIR_<NAME>` (appended to `SINK_ROOTS` /
  `MODELS_DIR` by the DuckDB sink and the ClickHouse provisioner) and bind-mounts the module's models, so
  overrides of several ecosystems compose. All deployed models are resolved together: don't deploy two
  ecosystems that define the same Pure names (the generator warns).
- **Deephaven**: `concert.py` execs `app.d/ecosystems/<name>.py` when `CONCERT_ECOSYSTEM_<NAME>` is set (the
  override sets it): `<name>_<table>_latest` per root, `<name>_latest`, `<name>_state_distribution`,
  `<name>_completions_per_minute`.

## Examples

- `examples/insurance` → `showcases/insurance`: Policy, Claim, Payout (stubs filled in by hand).
- `examples/trading-gen` → `showcases/trading-gen`: the trading models annotated to match the hand-written
  `TradingFlows` (smTypes prefixed `gen_`); `showcases/trading`'s `GeneratedTradingEquivalenceTest` asserts
  states, transitions, payload classes, terminal states and lock templates are equal. Not deployed (it shares
  its Pure names with `trading-model`).
