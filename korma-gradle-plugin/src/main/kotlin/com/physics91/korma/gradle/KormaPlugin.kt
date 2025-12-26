package com.physics91.korma.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * Korma Gradle Plugin for generating Kotlin table definitions from database schemas.
 *
 * Usage in build.gradle.kts:
 * ```kotlin
 * plugins {
 *     id("com.physics91.korma") version "0.1.0"
 * }
 *
 * korma {
 *     packageName.set("com.example.db.tables")
 *     outputDirectory.set(file("src/main/kotlin"))
 *
 *     database {
 *         postgresql("localhost", 5432, "mydb")
 *         username.set("user")
 *         password.set("pass")
 *     }
 *
 *     codegen {
 *         generateEntities.set(true)
 *         include("users", "orders")
 *     }
 * }
 * ```
 *
 * Then run:
 * ```shell
 * ./gradlew generateKormaTables
 * ```
 */
class KormaPlugin : Plugin<Project> {

    override fun apply(project: Project) {
        // Create extension
        val extension = project.extensions.create("korma", KormaExtension::class.java)

        // Set default output directory
        extension.outputDirectory.convention(
            project.layout.projectDirectory.dir("src/main/kotlin").asFile
        )

        // Register main task
        val generateTask = project.tasks.register(
            "generateKormaTables",
            GenerateKormaTablesTask::class.java
        )
        generateTask.configure { task ->
            // Bind task properties to extension
            task.packageName.set(extension.packageName)
            task.outputDirectory.set(extension.outputDirectory.map {
                project.layout.projectDirectory.dir(it.path)
            })

            // Database properties
            task.databaseUrl.set(extension.database.url)
            task.databaseUsername.set(extension.database.username)
            task.databasePassword.set(extension.database.password)
            task.driverClassName.set(extension.database.driverClassName)
            task.schema.set(extension.database.schema)

            // Codegen properties
            task.generateEntities.set(extension.codegen.generateEntities)
            task.generateKdoc.set(extension.codegen.generateKdoc)
            task.useNullableTypes.set(extension.codegen.useNullableTypes)
            task.fileHeader.set(extension.codegen.fileHeader)
            task.includePatterns.set(extension.codegen.includePatterns)
            task.excludePatterns.set(extension.codegen.excludePatterns)
        }

        // Register preview task
        val previewTask = project.tasks.register(
            "previewKormaTables",
            PreviewKormaTablesTask::class.java
        )
        previewTask.configure { task ->
            task.packageName.set(extension.packageName)
            task.databaseUrl.set(extension.database.url)
            task.databaseUsername.set(extension.database.username)
            task.databasePassword.set(extension.database.password)
            task.driverClassName.set(extension.database.driverClassName)
            task.databaseSchema.set(extension.database.schema)
            task.generateEntities.set(extension.codegen.generateEntities)
            task.generateKdoc.set(extension.codegen.generateKdoc)
            task.useNullableTypes.set(extension.codegen.useNullableTypes)
            task.fileHeader.set(extension.codegen.fileHeader)
            task.includePatterns.set(extension.codegen.includePatterns)
            task.excludePatterns.set(extension.codegen.excludePatterns)
        }

        // Register introspect task
        val introspectTask = project.tasks.register(
            "introspectKormaSchema",
            IntrospectKormaSchemaTask::class.java
        )
        introspectTask.configure { task ->
            task.databaseUrl.set(extension.database.url)
            task.databaseUsername.set(extension.database.username)
            task.databasePassword.set(extension.database.password)
            task.driverClassName.set(extension.database.driverClassName)
            task.databaseSchema.set(extension.database.schema)
        }
    }
}
