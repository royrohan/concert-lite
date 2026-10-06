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

    // Pure models: a source set with a src/<name>/pure directory passes it to the LegendModelProcessor
    // (model-codegen) and declares it as a compile input, so editing a model recompiles. The project
    // still adds the processor itself, e.g. annotationProcessor(project(":model-codegen")) and
    // implementation(project(":model-runtime")).
    extensions.getByType<SourceSetContainer>().configureEach {
        val pureDir = layout.projectDirectory.dir("src/$name/pure")
        if (pureDir.asFile.isDirectory) {
            tasks.named<JavaCompile>(compileJavaTaskName) {
                options.compilerArgs.add("-Aconcert.modelRoots=${pureDir.asFile.absolutePath}")
                inputs.dir(pureDir).withPathSensitivity(PathSensitivity.RELATIVE).withPropertyName("pureModels")
            }
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Temporal, netty and protobuf still use sun.misc.Unsafe; allow it quietly on JDK 24+.
        jvmArgs("-XX:+UseZGC", "-XX:+UseCompactObjectHeaders", "--sun-misc-unsafe-memory-access=allow")
        testLogging {
            events("passed", "failed", "skipped")
            showStandardStreams = providers.gradleProperty("showLogs").isPresent
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }
}
