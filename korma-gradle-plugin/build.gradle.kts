plugins {
    kotlin("jvm")
    `java-gradle-plugin`
}

description = "Korma Gradle Plugin - Generate Kotlin table definitions from database schemas"

dependencies {
    // Gradle API
    implementation(gradleApi())

    // Korma Codegen
    implementation(project(":korma-codegen"))

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testImplementation(kotlin("test"))
    testImplementation(gradleTestKit())
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.14")
}

gradlePlugin {
    plugins {
        create("korma") {
            id = "com.physics91.korma"
            implementationClass = "com.physics91.korma.gradle.KormaPlugin"
            displayName = "Korma Code Generation Plugin"
            description = "Generate Kotlin table definitions from database schemas"
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
