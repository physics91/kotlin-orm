plugins {
    kotlin("jvm")
}

description = "Korma Cache Caffeine - In-memory L1 cache implementation"

dependencies {
    // Korma cache abstraction
    api(project(":korma-cache"))

    // Caffeine cache
    api("com.github.ben-manes.caffeine:caffeine:3.1.8")

    // Kotlin Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")

    // SLF4J Logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Test dependencies
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}
