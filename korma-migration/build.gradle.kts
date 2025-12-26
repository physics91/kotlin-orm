plugins {
    kotlin("jvm")
}

description = "Korma Migration - Database schema migration and version control"

dependencies {
    // Korma core and JDBC
    api(project(":korma-core"))
    api(project(":korma-jdbc"))

    // Kotlin Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")

    // SLF4J Logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Test dependencies
    testImplementation(project(":korma-dialect-h2"))
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}
