plugins {
    kotlin("jvm")
}

description = "Korma Cache - Caching abstraction and session-level cache"

dependencies {
    // Korma core
    api(project(":korma-core"))

    // Kotlin Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")

    // SLF4J Logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Test dependencies
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}
