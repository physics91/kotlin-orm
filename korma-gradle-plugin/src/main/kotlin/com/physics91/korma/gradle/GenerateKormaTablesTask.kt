package com.physics91.korma.gradle

import com.physics91.korma.codegen.KormaCodegen
import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import java.nio.file.Path

/**
 * Gradle task to generate Korma table definitions from a database schema.
 *
 * Usage:
 * ```shell
 * ./gradlew generateKormaTables
 * ```
 */
abstract class GenerateKormaTablesTask : DefaultTask() {

    init {
        group = "korma"
        description = "Generate Kotlin table definitions from database schema"
    }

    /**
     * Package name for generated classes.
     */
    @get:Input
    abstract val packageName: Property<String>

    /**
     * Output directory for generated files.
     */
    @get:OutputDirectory
    abstract val outputDirectory: DirectoryProperty

    /**
     * JDBC URL for database connection.
     */
    @get:Input
    abstract val databaseUrl: Property<String>

    /**
     * Database username.
     */
    @get:Input
    @get:Optional
    abstract val databaseUsername: Property<String>

    /**
     * Database password.
     */
    @get:Input
    @get:Optional
    abstract val databasePassword: Property<String>

    /**
     * JDBC driver class name.
     */
    @get:Input
    @get:Optional
    abstract val driverClassName: Property<String>

    /**
     * Database schema to introspect.
     */
    @get:Input
    @get:Optional
    abstract val schema: Property<String>

    /**
     * Whether to generate entity classes.
     */
    @get:Input
    abstract val generateEntities: Property<Boolean>

    /**
     * Whether to generate KDoc comments.
     */
    @get:Input
    abstract val generateKdoc: Property<Boolean>

    /**
     * Whether to use nullable types for nullable columns.
     */
    @get:Input
    abstract val useNullableTypes: Property<Boolean>

    /**
     * File header comment.
     */
    @get:Input
    @get:Optional
    abstract val fileHeader: Property<String>

    /**
     * Table name patterns to include.
     */
    @get:Input
    abstract val includePatterns: ListProperty<String>

    /**
     * Table name patterns to exclude.
     */
    @get:Input
    abstract val excludePatterns: ListProperty<String>

    @TaskAction
    fun generate() {
        val outputPath: Path = outputDirectory.get().asFile.toPath()

        logger.lifecycle("Generating Korma tables...")
        logger.lifecycle("  Package: ${packageName.get()}")
        logger.lifecycle("  Output: $outputPath")
        logger.lifecycle("  Database: ${databaseUrl.get()}")

        // Build database config
        val dbConfig = DatabaseConfig(
            url = databaseUrl.get(),
            username = databaseUsername.orNull,
            password = databasePassword.orNull,
            driverClassName = driverClassName.orNull
        )

        // Build codegen config
        val config = CodegenConfig(
            packageName = packageName.get(),
            outputDirectory = outputPath,
            databaseConfig = dbConfig,
            schema = schema.orNull,
            generateEntities = generateEntities.get(),
            generateKdoc = generateKdoc.get(),
            useNullableTypes = useNullableTypes.get(),
            fileHeader = fileHeader.orNull,
            includePatterns = includePatterns.get().map { Regex(it) },
            excludePatterns = excludePatterns.get().map { Regex(it) }
        )

        // Generate code
        val paths = KormaCodegen.generate(config)

        if (paths.isEmpty()) {
            logger.warn("No tables found matching the configuration")
        } else {
            logger.lifecycle("Generated ${paths.size} files:")
            paths.forEach { path ->
                logger.lifecycle("  - $path")
            }
        }
    }
}
