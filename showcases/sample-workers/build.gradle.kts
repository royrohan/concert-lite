plugins { application; `java-library` }

dependencies {
    api(project(":worker-sdk"))
    // Typed machines: models in src/main/pure generate classes into io.concert.samples.model.
    // The root build passes src/main/pure to the processor and makes it a compileJava input.
    implementation(project(":model-runtime"))
    annotationProcessor(project(":model-codegen"))
    // STORE_KIND=dynamo|spanner backends, discovered via ServiceLoader
    runtimeOnly(project(":store-dynamo"))
    runtimeOnly(project(":store-spanner"))

    testImplementation(libs.temporal.testing)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    // Pick the state machine with SM_TYPE=order|payment|shipment.
    mainClass.set("io.concert.samples.SampleWorkerMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders")
}
