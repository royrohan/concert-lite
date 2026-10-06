plugins { application }

// One-shot ClickHouse provisioner: generates the Kafka engine tables, materialized views and typed
// ReplacingMergeTree tables from the Pure models and applies them over ClickHouse's HTTP interface
// (java.net.http, no client library). ClickHouse itself does the consuming.
dependencies {
    implementation(project(":sink-core"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}

// kafka-clients (tests' producer) loads zstd-jni, a native library.
val nativeAccess = "--enable-native-access=ALL-UNNAMED"

application {
    mainClass.set("io.concert.sink.clickhouse.ClickHouseProvisionerMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseSerialGC", nativeAccess)
}

tasks.test {
    systemProperty("concert.modelsDir", rootProject.file("showcases/sample-workers/src/main/pure").absolutePath)
    systemProperty("concert.tradingModelsDir", rootProject.file("core/trading-model/src/main/pure").absolutePath)
    // DeephavenColumnsTest compares (or with -Dconcert.regenerate=true rewrites) the Deephaven script's column file.
    systemProperty("concert.deephavenColumns", rootProject.file("infra/deephaven/app.d/marketdata_columns.json").absolutePath)
    systemProperty("concert.regenerate", System.getProperty("concert.regenerate") ?: "false")
    jvmArgs(nativeAccess)
}
