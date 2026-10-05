dependencies {
    api(project(":common"))
    api(project(":store"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(libs.temporal.testing)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}
