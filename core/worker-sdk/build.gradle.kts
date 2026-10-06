dependencies {
    api(project(":common"))
    api(project(":store"))
    api(project(":model-runtime"))
    // EntitySnapshot and the Kafka producer behind the EntitySnapshotPublisher activity.
    api(project(":sink-core"))
    runtimeOnly(libs.slf4j.simple)

    // Test models in src/test/pure (wired by the root build's Pure model convention).
    testAnnotationProcessor(project(":model-codegen"))
    testImplementation(libs.temporal.testing)
    // event-style tests run the real lock chain (KeyLockWorkflowImpl, DispatchActivitiesImpl, IngestDispatcher)
    testImplementation(project(":orchestration"))
    // ReconcileWorkflowDuckDbTest: a real DuckDB sink behind its HTTP API
    testImplementation(project(":sink-duckdb"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

tasks.test {
    systemProperty("concert.testModelsDir", file("src/test/pure").absolutePath)
    // DuckDB loads its native library through JNI.
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}
