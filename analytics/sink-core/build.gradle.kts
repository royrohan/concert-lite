// Completed-entity snapshots: the record and its Kafka encoding, the producer used by workers, and the
// consumer framework the analytics sinks build on (SinkTarget SPI, consumer loop, SchemaMapper).
// Deliberately free of Temporal so sink applications stay small; worker-sdk wraps the producer in an
// activity.
dependencies {
    api(libs.kafka.clients)
    api(project(":model-pure"))
    api(platform(libs.jackson.bom))
    api(libs.jackson.databind)
    api(libs.slf4j.api)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}

tasks.test {
    // SchemaMapper tests run against the sample workers' models.
    systemProperty("concert.modelsDir", rootProject.file("showcases/sample-workers/src/main/pure").absolutePath)
    systemProperty("concert.tradingModelsDir", rootProject.file("core/trading-model/src/main/pure").absolutePath)
    // zstd-jni (snapshot compression) loads its native library.
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}
