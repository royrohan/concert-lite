dependencies {
    testImplementation(project(":orchestration"))
    testImplementation(project(":sample-workers"))
    testImplementation(platform(libs.aws.bom))
    testImplementation(libs.aws.kinesis)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.hdrhistogram)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}

tasks.test {
    // Integration tests need Docker; the benchmark is opt-in: -Pbench
    if (!providers.gradleProperty("bench").isPresent) {
        useJUnitPlatform { excludeTags("bench") }
    }
    maxHeapSize = "2g"
    // Forward -Dbench.* overrides to the test JVM.
    System.getProperties().filter { it.key.toString().startsWith("bench.") }
        .forEach { systemProperty(it.key.toString(), it.value) }
}
