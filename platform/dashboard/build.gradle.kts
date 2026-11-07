plugins { application }

dependencies {
    // State machines to draw, discovered at runtime through the MachineCatalog SPI (worker-sdk): the
    // sample and trading machines ...
    runtimeOnly(project(":sample-workers"))
    // ... trading also at compile time: the flow view's data generator uses its TradingFlowGenerator
    implementation(project(":trading"))
    // ... and every generated ecosystem (a project with an ecosystem.json, ./generate-concert-ecosystem),
    // so a new ecosystem needs no edit here.
    rootProject.subprojects
        .filter { it.projectDir.resolve("ecosystem.json").isFile }
        .forEach { runtimeOnly(project(it.path)) }
    implementation(project(":worker-sdk"))
    // EventOps: operator retry / skip and lifecycle listings of event-style events
    implementation(project(":orchestration"))
    // Model query tab: the Legend store layer generated from the sinks' schema (legend-lite plans, concert executes)
    implementation(project(":legend-model"))
    // Flow view: tails the ingest stream for published/s per flow (KinesisTail)
    implementation(platform(libs.aws.bom))
    implementation(libs.aws.kinesis)
    // Performance page: past runs, their samples and timelines in a DuckDB file (PerfStore, PERF_DB)
    implementation(libs.duckdb.jdbc)
    // STORE_KIND=dynamo|spanner backends, discovered via ServiceLoader
    runtimeOnly(project(":store-dynamo"))
    runtimeOnly(project(":store-spanner"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass.set("io.concert.dashboard.DashboardMain")
    // DuckDB (PerfStore) loads its native library
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

tasks.test {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

// The flow view's "Generate data flow" replays the showcases' sample flows (samples/flows/*.json for entity
// flows, samples/event-flows/*.json for event flows), packaged as resources concert-samples/<project>/...
// with an index, so a new ecosystem's samples are picked up without an edit here.
val sampleFlows by tasks.registering(Sync::class) {
    val out = layout.buildDirectory.dir("generated/sample-flows")
    rootProject.subprojects.filter { it.projectDir.resolve("samples").isDirectory }.forEach { p ->
        from(p.projectDir.resolve("samples")) {
            include("flows/*.json", "event-flows/*.json")
            into("concert-samples/${p.name}")
        }
    }
    into(out)
    doLast {
        val root = out.get().dir("concert-samples").asFile
        root.mkdirs()
        val files = root.walkTopDown().filter { it.isFile && it.name.endsWith(".json") }
            .map { it.relativeTo(root).invariantSeparatorsPath }.sorted().toList()
        root.resolve("index.txt").writeText(files.joinToString("\n", postfix = "\n"))
    }
}
sourceSets.main { resources.srcDir(sampleFlows) }
