plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma Ktor Plugin - Ktor integration for Korma ORM with coroutine-native support"

val ktorVersion = property("ktorVersion").toString()

dependencies {
    // Korma modules
    api(project(":korma-core"))
    api(project(":korma-jdbc"))

    // H2 dialect (for auto-detection fallback - internal use only)
    implementation(project(":korma-dialect-h2"))

    // Optional dialect support
    compileOnly(project(":korma-dialect-postgresql"))
    compileOnly(project(":korma-dialect-mysql"))
    compileOnly(project(":korma-dialect-sqlite"))

    // Optional R2DBC support for async operations
    compileOnly(project(":korma-r2dbc"))

    // Optional cache support
    compileOnly(project(":korma-cache"))
    compileOnly(project(":korma-cache-caffeine"))

    // Ktor server
    api("io.ktor:ktor-server-core:$ktorVersion")

    // Coroutines (comes transitively from Ktor, but we need explicit version)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")

    // Testing
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-server-netty:$ktorVersion")
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation(project(":korma-dialect-h2"))
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Ktor Plugin")
                description.set("Ktor integration for Korma ORM with coroutine-native support")
            }
        }
    }
}
