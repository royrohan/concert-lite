plugins { application }

// The Legend store layer (Database, Mapping, Connection, Runtime as Pure) of the analytics sinks, generated from the
// same SchemaSpec the sinks build their tables from, for legend-lite to plan model queries against DuckDB / ClickHouse.
dependencies {
    api(project(":sink-core"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass.set("io.concert.legend.LegendStoreMain")
}

tasks.test {
    systemProperty("concert.repo", rootProject.projectDir.absolutePath)
    // Set to a running legend-lite (e.g. http://localhost:8095) to compile every model set and the generated store.
    System.getenv("LEGEND_LITE_URL")?.let { environment("LEGEND_LITE_URL", it) }
}
