// Yours: applied by the generated build.gradle.kts. Integration tests (@Tag("integration")) need the deployed
// stack, so `test` skips them and `integrationTest` runs only them:
//   ./deploy-concert-ecosystem library && ./gradlew :library:integrationTest
tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.register<Test>("integrationTest") {
    group = "verification"
    description = "Runs the @Tag(\"integration\") tests against the deployed stack"
    val test = tasks.named<Test>("test").get()
    testClassesDirs = test.testClassesDirs
    classpath = test.classpath
    useJUnitPlatform { includeTags("integration") }
    // the local stack's LocalStack, like ./showcases/library/send; set KINESIS_ENDPOINT / DASHBOARD_URL to override
    environment("KINESIS_ENDPOINT", System.getenv("KINESIS_ENDPOINT") ?: "http://localhost:4566")
    environment("AWS_REGION", System.getenv("AWS_REGION") ?: "us-east-1")
    outputs.upToDateWhen { false }
}
