dependencies {
    api(project(":store"))
    implementation(platform(libs.gcloud.bom))
    implementation(libs.gcloud.spanner)

    testImplementation(testFixtures(project(":store")))
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}
