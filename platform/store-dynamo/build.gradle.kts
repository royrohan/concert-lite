dependencies {
    api(project(":store"))
    implementation(platform(libs.aws.bom))
    implementation(libs.aws.dynamodb)
    // EnhancedDocument: JSON <-> native DynamoDB map attributes for entity data
    implementation("software.amazon.awssdk:dynamodb-enhanced")
    implementation(libs.aws.apache)

    testImplementation(testFixtures(project(":store")))
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.slf4j.simple)
}
