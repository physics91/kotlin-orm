plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma Dialect PostgreSQL - PostgreSQL database dialect"

dependencies {
    api(project(":korma-core"))

    // PostgreSQL driver (provided for runtime)
    compileOnly("org.postgresql:postgresql:42.7.4")

    // Testing
    testImplementation("org.postgresql:postgresql:42.7.4")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Dialect PostgreSQL")
                description.set("PostgreSQL database dialect for Korma ORM")
            }
        }
    }
}
