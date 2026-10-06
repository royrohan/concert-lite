plugins { `java-test-fixtures` }

dependencies {
    api(project(":common"))
    api(libs.hikari)
    implementation(libs.postgres)
    // Only needed when STORE_KIND=dsql (IAM-token auth against real Aurora DSQL).
    implementation(libs.dsql.connector)

    // StateStoreContract: the behavior every backend must provide; backend modules run it too.
    testFixturesApi(platform(libs.junit.bom))
    testFixturesApi(libs.junit.jupiter)
    testFixturesApi(platform(libs.testcontainers.bom))
    testFixturesApi(libs.testcontainers)

    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}
