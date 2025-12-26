plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma Dialect MySQL - MySQL/MariaDB database dialect"

dependencies {
    api(project(":korma-core"))

    // MySQL driver (provided for runtime)
    compileOnly("com.mysql:mysql-connector-j:9.1.0")

    // Testing
    testImplementation("com.mysql:mysql-connector-j:9.1.0")
    testImplementation("org.testcontainers:mysql:1.20.4")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Dialect MySQL")
                description.set("MySQL/MariaDB database dialect for Korma ORM")
            }
        }
    }
}
