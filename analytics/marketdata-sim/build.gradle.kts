plugins { application }

// Market data that bypasses concert: reference data and ticks go straight to Kafka. No Temporal or
// AWS here, so the trading module can reuse the symbol universe and price model cheaply.
dependencies {
    // Instruments, accounts, venues, quotes, ticks and bars are classes generated from trading-model's
    // Pure files; values are written with ModelJson.
    api(project(":trading-model"))
    implementation(libs.kafka.clients)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}

// kafka-clients loads zstd-jni, a native library.
val nativeAccess = "--enable-native-access=ALL-UNNAMED"

application {
    mainClass.set("io.concert.marketdata.MarketDataSimMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders", nativeAccess)
}

tasks.test {
    jvmArgs(nativeAccess)
}
