// Front end for Legend Pure domain models: no runtime dependencies on purpose, so the annotation
// processor that consumes it stays lightweight.
dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}
