plugins {
    kotlin("jvm")
    `maven-publish`
}

description = "Korma DAO - Entity mapping and relationship support"

dependencies {
    api(project(":korma-core"))

    // Testing
    testImplementation(project(":korma-jdbc"))
    testImplementation(project(":korma-dialect-h2"))
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma DAO")
                description.set("Entity and relationship mapping for Korma ORM")
            }
        }
    }
}
