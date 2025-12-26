plugins {
    `java-platform`
    `maven-publish`
}

javaPlatform {
    allowDependencies()
}

dependencies {
    constraints {
        // ============== Phase 1 Modules ==============
        api(project(":korma-core"))
        api(project(":korma-jdbc"))
        api(project(":korma-dialect-h2"))

        // ============== Future Phases ==============
        // Uncomment as modules are added

        // Phase 2+: Advanced modules
        // api(project(":korma-r2dbc"))
        // api(project(":korma-dao"))

        // Phase 5: Migration
        // api(project(":korma-migration"))

        // Phase 6: Cache modules
        // api(project(":korma-cache"))
        // api(project(":korma-cache-caffeine"))
        // api(project(":korma-cache-redis"))

        // Phase 7: Dialects
        // api(project(":korma-dialect-postgresql"))
        // api(project(":korma-dialect-mysql"))
        // api(project(":korma-dialect-sqlite"))

        // Phase 8: Framework integrations
        // api(project(":korma-spring-boot-starter"))
        // api(project(":korma-ktor-plugin"))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["javaPlatform"])

            pom {
                name.set("Korma BOM")
                description.set("Bill of Materials for Korma ORM Framework")
            }
        }
    }
}
