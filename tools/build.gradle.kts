plugins { application }

dependencies {
    implementation(project(":orchestration"))
    implementation(project(":sample-workers"))
    implementation(libs.hdrhistogram)
    implementation(platform(libs.aws.bom))
    implementation(libs.aws.kinesis)
    runtimeOnly(libs.slf4j.simple)
}

application {
    // tools loadgen ... | tools verify ...
    mainClass.set("io.concert.tools.ToolsMain")
}
