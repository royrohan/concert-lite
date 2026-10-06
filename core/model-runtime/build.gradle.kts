// Runtime support for classes generated from Pure models: annotations, the ModelObject contract and
// the deterministic JSON mapper. Kept small because generated code and workflow code depend on it.
dependencies {
    api(platform(libs.jackson.bom))
    api(libs.jackson.databind)
    api(libs.jackson.jsr310)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}
