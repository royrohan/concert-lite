plugins { application }

dependencies {
    api(project(":common"))
    api(project(":store"))
    // STORE_KIND=dynamo|spanner backends, discovered via ServiceLoader
    runtimeOnly(project(":store-dynamo"))
    runtimeOnly(project(":store-spanner"))
    implementation(platform(libs.aws.bom))
    implementation(libs.aws.kinesis)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(project(":worker-sdk"))
    testImplementation(libs.temporal.testing)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jqwik)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass.set("io.concert.orchestration.OrchestrationWorkerMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders")
}

tasks.test {
    // The in-process Temporal test server (protobuf, grpc-netty) uses sun.misc.Unsafe and native access;
    // allow them explicitly so the test JVM prints no JDK 25 warnings.
    jvmArgs("--sun-misc-unsafe-memory-access=allow", "--enable-native-access=ALL-UNNAMED")
}

// bin/events: operator CLI for event-style events (scripts/events.sh)
val eventsScripts = tasks.register<CreateStartScripts>("eventsScripts") {
    mainClass.set("io.concert.orchestration.events.EventsCli")
    applicationName = "events"
    outputDir = layout.buildDirectory.dir("events-scripts").get().asFile
    classpath = tasks.named<Jar>("jar").get().outputs.files + configurations.runtimeClasspath.get()
}
distributions.named("main") {
    contents { from(eventsScripts) { into("bin") } }
}
