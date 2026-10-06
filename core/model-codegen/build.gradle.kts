// Generates Java classes from Pure models; packaged as an annotation processor (LegendModelProcessor).
dependencies {
    api(project(":model-pure"))
    implementation(project(":model-runtime"))
    api(libs.javapoet)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}
