subprojects {
    apply(plugin = "java-library")

    group = "io.concert"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        // -parameters lets Jackson bind record components without annotations.
        options.compilerArgs.addAll(listOf("-parameters", "-Xlint:all,-serial,-processing"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders")
        testLogging {
            events("passed", "failed", "skipped")
            showStandardStreams = providers.gradleProperty("showLogs").isPresent
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }
}
