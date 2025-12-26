plugins {
    kotlin("jvm")
}

description = "Korma Test Infrastructure - TestContainers, Fixtures, and Test Utilities"

dependencies {
    // Korma Core
    api(project(":korma-core"))
    api(project(":korma-jdbc"))
    api(project(":korma-r2dbc"))

    // Dialects
    api(project(":korma-dialect-h2"))
    api(project(":korma-dialect-postgresql"))
    api(project(":korma-dialect-mysql"))

    // Kotlin Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:${property("kotlinxCoroutinesVersion")}")

    // Testing Framework
    api("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    api("org.junit.jupiter:junit-jupiter-params:${property("junitVersion")}")
    api("io.kotest:kotest-runner-junit5:${property("kotestVersion")}")
    api("io.kotest:kotest-assertions-core:${property("kotestVersion")}")
    api("io.mockk:mockk:${property("mockkVersion")}")

    // TestContainers
    api("org.testcontainers:testcontainers:${property("testcontainersVersion")}")
    api("org.testcontainers:junit-jupiter:${property("testcontainersVersion")}")
    api("org.testcontainers:postgresql:${property("testcontainersVersion")}")
    api("org.testcontainers:mysql:${property("testcontainersVersion")}")
    api("org.testcontainers:r2dbc:${property("testcontainersVersion")}")

    // Database Drivers
    api("com.h2database:h2:${property("h2Version")}")
    api("org.postgresql:postgresql:${property("postgresqlVersion")}")
    api("com.mysql:mysql-connector-j:${property("mysqlVersion")}")

    // R2DBC Drivers
    api("io.r2dbc:r2dbc-h2:${property("r2dbcH2Version")}")
    api("org.postgresql:r2dbc-postgresql:${property("r2dbcPostgresqlVersion")}")
    api("io.asyncer:r2dbc-mysql:${property("r2dbcMysqlVersion")}")

    // Logging
    api("org.slf4j:slf4j-api:2.0.9")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.14")
}

// No coverage verification for test infrastructure module
tasks.withType<JacocoCoverageVerification>().configureEach {
    enabled = false
}
