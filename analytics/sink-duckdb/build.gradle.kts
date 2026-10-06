plugins { application }

// Kafka -> DuckDB sink for completed entities, plus a read-only SQL endpoint for the trace UI.
dependencies {
    implementation(project(":sink-core"))
    implementation(libs.duckdb.jdbc)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}

application {
    mainClass.set("io.concert.sink.duckdb.DuckDbSinkMain")
    // DuckDB loads its native library through JNI.
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders", "--enable-native-access=ALL-UNNAMED")
}

tasks.test {
    systemProperty("concert.modelsDir", rootProject.file("showcases/sample-workers/src/main/pure").absolutePath)
    systemProperty("concert.tradingModelsDir", rootProject.file("core/trading-model/src/main/pure").absolutePath)
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}
