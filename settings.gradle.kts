rootProject.name = "korma"

// Enable type-safe project accessors
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

// Plugin management
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

// Dependency resolution management
dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

// ============== Phase 1 Modules ==============
include(
    // Core modules
    "korma-bom",
    "korma-core",
    "korma-jdbc",

    // Dialect modules (for testing)
    "korma-dialect-h2",

    // Phase 3: DAO module
    "korma-dao",

    // Phase 4: R2DBC module
    "korma-r2dbc",

    // Phase 5: Migration module
    "korma-migration",

    // Phase 6: Cache modules
    "korma-cache",
    "korma-cache-caffeine",
    "korma-cache-redis",

    // Phase 7: Dialect modules
    "korma-dialect-postgresql",
    "korma-dialect-mysql",
    "korma-dialect-sqlite",

    // Phase 8: Framework integrations
    "korma-spring-boot-starter",
    "korma-ktor-plugin",

    // Test Infrastructure
    "korma-test",

    // Monitoring and Metrics
    "korma-micrometer",

    // Code Generation
    "korma-codegen",
    "korma-gradle-plugin"
)

// ============== Future Phases (Commented Out) ==============
// Uncomment these as we progress through phases

// Phase 2+: Advanced modules
// include("korma-r2dbc")
// include("korma-dao")

// Phase 5: Migration
// include("korma-migration")

// Phase 6: Cache modules
// include("korma-cache")
// include("korma-cache-caffeine")
// include("korma-cache-redis")

// Phase 7: Dialect modules
// include("korma-dialect-postgresql")
// include("korma-dialect-mysql")
// include("korma-dialect-sqlite")

// Phase 8: Framework integrations
// include("korma-spring-boot-starter")
// include("korma-ktor-plugin")
