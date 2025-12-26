plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    `maven-publish`
}

description = "Korma Spring Boot Starter - Spring Boot Auto-configuration for Korma ORM"

val springBootVersion = "3.3.0"

dependencies {
    // Korma modules
    api(project(":korma-core"))
    api(project(":korma-jdbc"))

    // H2 dialect (for auto-detection fallback)
    api(project(":korma-dialect-h2"))

    // Optional dialect support
    compileOnly(project(":korma-dialect-postgresql"))
    compileOnly(project(":korma-dialect-mysql"))
    compileOnly(project(":korma-dialect-sqlite"))

    // Optional cache support
    compileOnly(project(":korma-cache"))
    compileOnly(project(":korma-cache-caffeine"))

    // Spring Boot
    api("org.springframework.boot:spring-boot-starter:$springBootVersion")
    api("org.springframework.boot:spring-boot-starter-jdbc:$springBootVersion")

    // Spring Boot auto-configuration
    compileOnly("org.springframework.boot:spring-boot-autoconfigure-processor:$springBootVersion")
    annotationProcessor("org.springframework.boot:spring-boot-autoconfigure-processor:$springBootVersion")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:$springBootVersion")

    // Transaction support
    api("org.springframework:spring-tx:6.1.8")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test:$springBootVersion")
    testImplementation("com.h2database:h2:2.3.232")
    testImplementation(project(":korma-dialect-h2"))
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Korma Spring Boot Starter")
                description.set("Spring Boot Auto-configuration for Korma ORM")
            }
        }
    }
}
