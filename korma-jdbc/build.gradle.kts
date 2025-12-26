plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma JDBC - JDBC implementation with HikariCP connection pooling"

dependencies {
    api(project(":korma-core"))

    // HikariCP connection pool
    api("com.zaxxer:HikariCP:${property("hikariVersion")}")

    // Testing
    testImplementation(project(":korma-dialect-h2"))
    testImplementation(project(":korma-dialect-mysql"))
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation("com.mysql:mysql-connector-j:${property("mysqlVersion")}")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma JDBC")
                description.set("JDBC implementation for Korma ORM with HikariCP")
            }
        }
    }
}
