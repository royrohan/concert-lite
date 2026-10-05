plugins { application }

dependencies {
    api(project(":common"))
    api(project(":store"))
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
