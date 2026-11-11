// Yours: applied by the generated build.gradle.kts.
// - the Kafka client for KafkaNotifyActivities
// - bin/ledger-service: the sample ledger REST service (an external system for the worker; compose.extra.yml runs it)
// - bin/payments-chaos: sends payments and verifies every one against the ledger and the Kafka topic
//   (scripts/payments-chaos.sh runs it once per fault scenario)
dependencies {
    // (applied scripts have no `libs` accessor: look the catalog up)
    "implementation"(project.extensions.getByType(VersionCatalogsExtension::class.java).named("libs").findLibrary("kafka-clients").get())
}

val ledgerScripts = tasks.register<CreateStartScripts>("ledgerStartScripts") {
    mainClass.set("io.concert.eco.payments.external.LedgerService")
    applicationName = "ledger-service"
    classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
    outputDir = layout.buildDirectory.dir("ledger-scripts").get().asFile
}

val chaosScripts = tasks.register<CreateStartScripts>("chaosStartScripts") {
    mainClass.set("io.concert.eco.payments.PaymentsChaosCheck")
    applicationName = "payments-chaos"
    defaultJvmOpts = listOf("-XX:+UseSerialGC")
    classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
    outputDir = layout.buildDirectory.dir("chaos-scripts").get().asFile
}

extensions.configure<DistributionContainer> {
    named("main") {
        contents {
            from(ledgerScripts) { into("bin") }
            from(chaosScripts) { into("bin") }
        }
    }
}

// Integration tests (@Tag("integration")) need the local stack's Postgres: `test` skips them,
// `integrationTest` runs only them.
tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.register<Test>("integrationTest") {
    group = "verification"
    description = "Runs the @Tag(\"integration\") tests against the local stack"
    val test = tasks.named<Test>("test").get()
    testClassesDirs = test.testClassesDirs
    classpath = test.classpath
    useJUnitPlatform { includeTags("integration") }
    outputs.upToDateWhen { false }
}
