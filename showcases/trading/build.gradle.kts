plugins { application }

// Trading showcase: the state machine definitions as data (TradingFlows) and a generator publishing
// realistic order flow to Kinesis (TradingLoadGen), and the typed trading state machines
// (io.concert.trading.machines, worker main TradingWorkerMain). The domain is defined in Pure in
// trading-model.
dependencies {
    implementation(project(":trading-model")) // generated domain classes: the only model types used here
    implementation(project(":orchestration")) // KinesisClients; EventEnvelope and Json via common
    implementation(project(":marketdata-sim")) // symbol universe and price model shared with md.ticks
    implementation(platform(libs.aws.bom))
    implementation(libs.aws.kinesis)
    // Typed state machines (ModelStateMachine, WorkerBootstrap, snapshot publisher).
    implementation(project(":worker-sdk"))
    // STORE_KIND=dynamo|spanner backends for the worker, discovered via ServiceLoader
    runtimeOnly(project(":store-dynamo"))
    runtimeOnly(project(":store-spanner"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(project(":model-pure"))
    // GeneratedTradingEquivalenceTest: the trading machines declared in Pure (examples/trading-gen, generated into
    // showcases/trading-gen) must equal TradingFlows
    testImplementation(project(":trading-gen"))
    testImplementation(libs.temporal.testing)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

val jvmArgs = listOf("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders")

application {
    // bin/trading = the load generator; bin/trading-worker (below) = the four state machine workers.
    mainClass.set("io.concert.trading.TradingLoadGen")
    applicationDefaultJvmArgs = jvmArgs
}

val workerMain = "io.concert.trading.TradingWorkerMain"

val workerStartScripts = tasks.register<CreateStartScripts>("workerStartScripts") {
    description = "Start scripts for TradingWorkerMain (bin/trading-worker)."
    mainClass.set(workerMain)
    applicationName = "trading-worker"
    defaultJvmOpts = jvmArgs
    classpath = tasks.startScripts.get().classpath
    outputDir = layout.buildDirectory.dir("worker-scripts").get().asFile
}

distributions.main {
    contents { from(workerStartScripts) { into("bin") } }
}

// ./gradlew :trading:runWorker  (same env as the worker container: TEMPORAL_*, STORE_KIND, KAFKA_BOOTSTRAP)
tasks.register<JavaExec>("runWorker") {
    group = "application"
    description = "Runs TradingWorkerMain (workers for the four trading state machines)."
    mainClass.set(workerMain)
    classpath = sourceSets.main.get().runtimeClasspath
    jvmArgs(jvmArgs)
}

tasks.test {
    systemProperty("trading.pureDir", rootProject.layout.projectDirectory.dir("core/trading-model/src/main/pure").asFile.absolutePath)
}
