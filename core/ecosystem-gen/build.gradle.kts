plugins { application }

// The ecosystem generator behind ./generate-concert-ecosystem and ./deploy-concert-ecosystem: reads Pure
// models whose state machines are declared with the concert::sm profile, validates them and writes a
// showcase module (showcases/<name>) with its worker, specs, stubs, samples, compose and analytics config.
dependencies {
    implementation(project(":model-pure"))
    implementation(project(":model-codegen")) // JavaNames: the generated classes' names and packages
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    // In-process compile of a generated module (javax.tools) against the real platform classes.
    testImplementation(project(":worker-sdk"))
    testImplementation(project(":orchestration"))
    testImplementation(project(":model-runtime"))
    testImplementation(platform(libs.aws.bom))
    testImplementation(libs.aws.kinesis)
}

application {
    mainClass.set("io.concert.ecosystem.EcosystemCli")
    applicationName = "ecosystem-gen"
}

tasks.test {
    systemProperty("concert.repoRoot", rootProject.layout.projectDirectory.asFile.absolutePath)
    // The compile test runs the model-codegen annotation processor in-process.
    systemProperty("concert.processorPath", configurations.named("testRuntimeClasspath").get().asPath)
}

// generateEcosystems: the generator as a Gradle task, so it can be run with a click from the IDE's Gradle panel
// (Tasks > concert). It generates (or updates) showcases/<name> for every examples/<name> directory, or only
// -Pecosystem=<name>. Same as ./generate-concert-ecosystem examples/<name> --name <name>; options recorded in
// showcases/<name>/ecosystem.json (package, coordinator) are kept.
abstract class GenerateEcosystems @Inject constructor(private val execOps: ExecOperations) : DefaultTask() {
    @get:Classpath abstract val toolClasspath: ConfigurableFileCollection
    @get:Input abstract val repoDir: Property<String>
    @get:Input @get:Optional abstract val only: Property<String>
    @get:Input abstract val javaExecutable: Property<String>

    @TaskAction
    fun generate() {
        val repo = File(repoDir.get())
        val dirs = File(repo, "examples").listFiles { f -> f.isDirectory && f.listFiles { p -> p.name.endsWith(".pure") }!!.isNotEmpty() }
            ?.sortedBy { it.name }.orEmpty()
            .filter { !only.isPresent || it.name == only.get() }
        if (dirs.isEmpty()) {
            throw GradleException("no examples/<name> directory with .pure files" + (if (only.isPresent) " named ${only.get()}" else ""))
        }
        dirs.forEach { dir ->
            logger.lifecycle("generate showcases/${dir.name} from examples/${dir.name}")
            execOps.javaexec {
                executable = javaExecutable.get()
                classpath = toolClasspath
                mainClass.set("io.concert.ecosystem.EcosystemCli")
                args("generate", "examples/${dir.name}", "--name", dir.name, "--repo", repo.absolutePath)
                workingDir = repo
            }
        }
        logger.lifecycle("new ecosystems are added to settings.gradle.kts: reload the Gradle project in the IDE")
    }
}

tasks.register<GenerateEcosystems>("generateEcosystems") {
    group = "concert"
    description = "Generates showcases/<name> from every examples/<name> (or -Pecosystem=<name>)"
    toolClasspath.from(sourceSets.main.map { it.runtimeClasspath })
    repoDir.set(rootProject.layout.projectDirectory.asFile.absolutePath)
    only.set(providers.gradleProperty("ecosystem"))
    javaExecutable.set(javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(25)) }
        .map { it.executablePath.asFile.absolutePath })
    outputs.upToDateWhen { false }
}
