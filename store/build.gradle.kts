dependencies {
    api(project(":common"))
    api(libs.hikari)
    implementation(libs.postgres)
    // Only needed when STORE_KIND=dsql (IAM-token auth against real Aurora DSQL).
    implementation(libs.dsql.connector)
}
