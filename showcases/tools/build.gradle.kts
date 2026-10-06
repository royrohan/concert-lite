plugins { application }

dependencies {
    implementation(project(":orchestration"))
    implementation(project(":sample-workers"))
    // STORE_KIND=dynamo|spanner backends, discovered via ServiceLoader
    runtimeOnly(project(":store-dynamo"))
    runtimeOnly(project(":store-spanner"))
    implementation(libs.hdrhistogram)
    implementation(platform(libs.aws.bom))
    implementation(libs.aws.kinesis)
    runtimeOnly(libs.slf4j.simple)
}

application {
    // tools loadgen ... | tools verify ...
    mainClass.set("io.concert.tools.ToolsMain")
}
