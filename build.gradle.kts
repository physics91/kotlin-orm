import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.0.21" apply false
    kotlin("plugin.serialization") version "2.0.21" apply false
    kotlin("plugin.spring") version "2.0.21" apply false
    id("org.jetbrains.dokka") version "1.9.20" apply false
    jacoco
}

allprojects {
    group = property("group").toString()
    version = property("version").toString()

    repositories {
        mavenCentral()
    }
}

subprojects {
    // Skip BOM module for standard plugin application
    if (name == "korma-bom") return@subprojects

    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "jacoco")

    // JaCoCo configuration
    configure<JacocoPluginExtension> {
        toolVersion = "0.8.12"
    }

    // Java compatibility
    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }

    // Kotlin configuration
    tasks.withType<KotlinCompile>().configureEach {
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_21)
            freeCompilerArgs.addAll(
                "-Xjsr305=strict",
                "-Xcontext-receivers",
                "-opt-in=kotlin.RequiresOptIn",
                "-opt-in=kotlin.contracts.ExperimentalContracts"
            )
        }
    }

    // Test configuration
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            showExceptions = true
            showCauses = true
            showStackTraces = true
        }
        finalizedBy(tasks.named("jacocoTestReport"))
    }

    // JaCoCo test report configuration
    tasks.withType<JacocoReport>().configureEach {
        dependsOn(tasks.withType<Test>())
        reports {
            xml.required.set(true)
            html.required.set(true)
            csv.required.set(false)
        }
    }

    // JaCoCo coverage verification
    tasks.withType<JacocoCoverageVerification>().configureEach {
        dependsOn(tasks.named("jacocoTestReport"))
        violationRules {
            rule {
                limit {
                    counter = "INSTRUCTION"
                    value = "COVEREDRATIO"
                    minimum = "0.60".toBigDecimal()
                }
            }
        }
    }

    // Common dependencies for all modules
    dependencies {
        val implementation by configurations
        val testImplementation by configurations
        val testRuntimeOnly by configurations

        // Kotlin stdlib
        implementation(kotlin("stdlib"))
        implementation(kotlin("reflect"))

        // Coroutines
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")

        // Testing
        testImplementation(kotlin("test"))
        testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
        testImplementation("io.kotest:kotest-runner-junit5:${property("kotestVersion")}")
        testImplementation("io.kotest:kotest-assertions-core:${property("kotestVersion")}")
        testImplementation("io.mockk:mockk:${property("mockkVersion")}")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")

        // TestContainers
        testImplementation("org.testcontainers:testcontainers:${property("testcontainersVersion")}")
        testImplementation("org.testcontainers:junit-jupiter:${property("testcontainersVersion")}")
        testImplementation("org.testcontainers:postgresql:${property("testcontainersVersion")}")
        testImplementation("org.testcontainers:mysql:${property("testcontainersVersion")}")
    }
}

// Dokka documentation for all modules
tasks.register("dokkaHtmlMultiModule") {
    dependsOn(subprojects.mapNotNull { it.tasks.findByName("dokkaHtml") })
}
