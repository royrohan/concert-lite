plugins { application }

dependencies {
    implementation(project(":sample-workers"))
    runtimeOnly(libs.slf4j.simple)
}

application {
    mainClass.set("io.concert.traceui.TraceUiMain")
}
