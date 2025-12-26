plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    `maven-publish`
}

description = "Korma Core - Type-safe SQL DSL and Schema definitions"

dependencies {
    // Kotlinx
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("kotlinxCoroutinesVersion")}")
    api("org.jetbrains.kotlinx:kotlinx-datetime:${property("kotlinxDatetimeVersion")}")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:${property("kotlinxSerializationVersion")}")

    // SLF4J for logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Testing
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Core")
                description.set("Core module for Korma ORM - Type-safe SQL DSL")
            }
        }
    }
}
