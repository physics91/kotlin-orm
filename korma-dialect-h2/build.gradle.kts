plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma Dialect H2 - H2 database dialect for testing"

dependencies {
    api(project(":korma-core"))

    // H2 driver (provided for runtime)
    compileOnly("com.h2database:h2:${property("h2Version")}")

    // Testing
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Dialect H2")
                description.set("H2 database dialect for Korma ORM")
            }
        }
    }
}
