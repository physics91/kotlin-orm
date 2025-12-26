plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma Dialect SQLite - SQLite database dialect"

dependencies {
    api(project(":korma-core"))

    // SQLite driver (provided for runtime)
    compileOnly("org.xerial:sqlite-jdbc:3.47.1.0")

    // Testing
    testImplementation("org.xerial:sqlite-jdbc:3.47.1.0")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Dialect SQLite")
                description.set("SQLite database dialect for Korma ORM")
            }
        }
    }
}
