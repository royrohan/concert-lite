plugins { application }

// The ecosystem generator behind ./generate-concert-ecosystem and ./deploy-concert-ecosystem: reads Pure
// models whose state machines are declared with the concert::sm profile, validates them and writes a
// showcase module (showcases/<name>) with its worker, specs, stubs, samples, compose and analytics config.
dependencies {
    implementation(project(":model-pure"))
    implementation(project(":model-codegen")) // JavaNames: the generated classes' names and packages
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    // In-process compile of a generated module (javax.tools) against the real platform classes.
    testImplementation(project(":worker-sdk"))
    testImplementation(project(":orchestration"))
    testImplementation(project(":model-runtime"))
    testImplementation(platform(libs.aws.bom))
    testImplementation(libs.aws.kinesis)
}

application {
    mainClass.set("io.concert.ecosystem.EcosystemCli")
    applicationName = "ecosystem-gen"
}

tasks.test {
    systemProperty("concert.repoRoot", rootProject.layout.projectDirectory.asFile.absolutePath)
    // The compile test runs the model-codegen annotation processor in-process.
    systemProperty("concert.processorPath", configurations.named("testRuntimeClasspath").get().asPath)
}
