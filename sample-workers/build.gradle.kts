plugins { application; `java-library` }

dependencies {
    api(project(":worker-sdk"))
}

application {
    // Pick the state machine with SM_TYPE=order|payment|shipment.
    mainClass.set("io.concert.samples.SampleWorkerMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders")
}
