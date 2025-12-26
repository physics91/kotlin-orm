plugins {
    kotlin("jvm")
}

description = "Korma R2DBC - Reactive database access with Kotlin coroutines"

dependencies {
    // Korma core
    api(project(":korma-core"))

    // R2DBC
    api("io.r2dbc:r2dbc-spi:${property("r2dbcSpiVersion")}")
    api("io.r2dbc:r2dbc-pool:${property("r2dbcPoolVersion")}")

    // Reactor (for Publisher to Flow conversion)
    api("io.projectreactor:reactor-core:3.6.12")

    // Kotlin Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:${property("kotlinxCoroutinesVersion")}")

    // SLF4J Logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Test dependencies
    testImplementation("io.r2dbc:r2dbc-h2:${property("r2dbcH2Version")}")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${property("kotlinxCoroutinesVersion")}")
}
