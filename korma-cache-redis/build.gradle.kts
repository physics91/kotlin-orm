plugins {
    kotlin("jvm")
}

description = "Korma Cache Redis - Distributed L2 cache implementation"

dependencies {
    // Korma cache abstraction
    api(project(":korma-cache"))

    // Lettuce Redis client
    api("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // Kotlin Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:${property("kotlinxCoroutinesVersion")}")

    // Kotlinx Serialization for value serialization
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")

    // SLF4J Logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Test dependencies
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
    // Testcontainers for Redis integration tests
    testImplementation("org.testcontainers:testcontainers:1.20.4")
}
