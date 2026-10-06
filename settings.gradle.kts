plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "concert-temporal"

// Modules live in group directories; project names stay flat (":store", ":trading", ...), so
// project(":x") references, task paths (./gradlew :trading:run) and artifact names are unaffected.
val modules = linkedMapOf(
    // core: shared API, the Pure model toolchain, the trading domain model and the worker SDK
    "common" to "core",
    "model-pure" to "core",
    "model-runtime" to "core",
    "model-codegen" to "core",
    "trading-model" to "core",
    "worker-sdk" to "core",
    "ecosystem-gen" to "core",
    // platform: the orchestration runtime, state stores and the trace UI
    "orchestration" to "platform",
    "store" to "platform",
    "store-dynamo" to "platform",
    "store-spanner" to "platform",
    "trace-ui" to "platform",
    // analytics: completed-entity snapshots, the DuckDB / ClickHouse sinks and the market data simulator
    "sink-core" to "analytics",
    "sink-duckdb" to "analytics",
    "sink-clickhouse" to "analytics",
    "marketdata-sim" to "analytics",
    // showcases: sample and trading state machines, load generators and verification tools
    "sample-workers" to "showcases",
    "trading" to "showcases",
    "tools" to "showcases",
    // generated ecosystems (./generate-concert-ecosystem adds one line each, between the markers)
    // <concert-ecosystems>
    "insurance" to "showcases",
    "trading-gen" to "showcases",
    // </concert-ecosystems>
    // tests: Docker-based end-to-end tests and benchmarks
    "integration-tests" to "tests",
)

modules.forEach { (name, group) ->
    include(name)
    project(":$name").projectDir = file("$group/$name")
}
