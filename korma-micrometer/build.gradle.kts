plugins {
    kotlin("jvm")
}

description = "Korma Micrometer Integration - Metrics and Monitoring"

dependencies {
    // Korma Core
    api(project(":korma-core"))
    compileOnly(project(":korma-jdbc"))
    compileOnly(project(":korma-r2dbc"))

    // Micrometer
    api("io.micrometer:micrometer-core:${property("micrometerVersion")}")

    // Kotlin Coroutines (for R2DBC metrics)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")

    // Testing
    testImplementation(project(":korma-jdbc"))
    testImplementation(project(":korma-dialect-h2"))
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testImplementation("io.kotest:kotest-assertions-core:${property("kotestVersion")}")
    testImplementation("io.micrometer:micrometer-test:${property("micrometerVersion")}")
}
