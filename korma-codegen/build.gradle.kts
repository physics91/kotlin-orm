plugins {
    kotlin("jvm")
}

description = "Korma Code Generation - Generate Kotlin code from database schemas"

dependencies {
    // Korma Core
    api(project(":korma-core"))
    implementation(project(":korma-jdbc"))

    // KotlinPoet for code generation
    implementation("com.squareup:kotlinpoet:1.15.3")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")

    // Database drivers (provided by user at runtime)
    compileOnly("com.h2database:h2:${property("h2Version")}")
    compileOnly("org.postgresql:postgresql:${property("postgresqlVersion")}")
    compileOnly("com.mysql:mysql-connector-j:${property("mysqlVersion")}")
    compileOnly("org.xerial:sqlite-jdbc:${property("sqliteVersion")}")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testImplementation(kotlin("test"))
    testImplementation("com.h2database:h2:${property("h2Version")}")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.14")
}

tasks.test {
    useJUnitPlatform()
}
