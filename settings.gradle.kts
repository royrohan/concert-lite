plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "concert-temporal"

include(
    "common",
    "store",
    "worker-sdk",
    "orchestration",
    "sample-workers",
    "integration-tests",
    "trace-ui",
    "tools",
)
