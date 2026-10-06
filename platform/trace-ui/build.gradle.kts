plugins { application }

dependencies {
    // State machines to draw, discovered at runtime through the MachineCatalog SPI (worker-sdk): the
    // sample and trading machines ...
    runtimeOnly(project(":sample-workers"))
    runtimeOnly(project(":trading"))
    // ... and every generated ecosystem (a project with an ecosystem.json, ./generate-concert-ecosystem),
    // so a new ecosystem needs no edit here.
    rootProject.subprojects
        .filter { it.projectDir.resolve("ecosystem.json").isFile }
        .forEach { runtimeOnly(project(it.path)) }
    implementation(project(":worker-sdk"))
    // EventOps: operator retry / skip and lifecycle listings of event-style events
    implementation(project(":orchestration"))
    // STORE_KIND=dynamo|spanner backends, discovered via ServiceLoader
    runtimeOnly(project(":store-dynamo"))
    runtimeOnly(project(":store-spanner"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass.set("io.concert.traceui.TraceUiMain")
}
