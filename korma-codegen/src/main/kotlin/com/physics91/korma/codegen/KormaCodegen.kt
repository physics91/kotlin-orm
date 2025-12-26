package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Main entry point for Korma code generation.
 *
 * Usage:
 * ```kotlin
 * // Using DSL configuration
 * KormaCodegen.generate {
 *     packageName("com.example.db.tables")
 *     outputDirectory("src/main/kotlin")
 *     database {
 *         postgresql("localhost", 5432, "mydb")
 *         username("user")
 *         password("pass")
 *     }
 *     generateEntities(true)
 *     generateDaos(false)
 *     include("user.*", "order.*")
 *     exclude(".*_backup")
 * }
 *
 * // Using config object
 * val config = CodegenConfig(...)
 * KormaCodegen.generate(config)
 * ```
 */
object KormaCodegen {

    private val logger = LoggerFactory.getLogger(KormaCodegen::class.java)

    /**
     * Generate code from database schema using DSL configuration.
     *
     * @param block Configuration DSL block
     * @return List of paths to generated files
     */
    fun generate(block: CodegenConfig.Builder.() -> Unit): List<Path> {
        val config = CodegenConfig.Builder().apply(block).build()
        return generate(config)
    }

    /**
     * Generate code from database schema using configuration object.
     *
     * @param config Code generation configuration
     * @return List of paths to generated files
     */
    fun generate(config: CodegenConfig): List<Path> {
        logger.info("Starting code generation")
        logger.info("  Package: ${config.packageName}")
        logger.info("  Output: ${config.outputDirectory}")
        logger.info("  Database: ${config.databaseConfig.url}")

        // Introspect database schema
        val introspector = SchemaIntrospector(config)
        val schema = introspector.introspect()

        if (schema.tables.isEmpty()) {
            logger.warn("No tables found matching the configuration")
            return emptyList()
        }

        logger.info("Found ${schema.tables.size} tables to generate")

        // Generate code
        val generator = KotlinCodeGenerator(config)
        val paths = generator.generateAndWrite(schema)

        logger.info("Code generation complete. Generated ${paths.size} files")
        return paths
    }

    /**
     * Preview generated code without writing to disk.
     *
     * @param block Configuration DSL block
     * @return Map of file names to generated code content
     */
    fun preview(block: CodegenConfig.Builder.() -> Unit): Map<String, String> {
        val config = CodegenConfig.Builder().apply(block).build()
        return preview(config)
    }

    /**
     * Preview generated code without writing to disk.
     *
     * @param config Code generation configuration
     * @return Map of file names to generated code content
     */
    fun preview(config: CodegenConfig): Map<String, String> {
        val introspector = SchemaIntrospector(config)
        val schema = introspector.introspect()

        val generator = KotlinCodeGenerator(config)
        val files = generator.generate(schema)

        return files.associate { fileSpec ->
            val fileName = "${fileSpec.packageName.replace('.', '/')}/${fileSpec.name}.kt"
            fileName to fileSpec.toString()
        }
    }

    /**
     * Introspect database schema without generating code.
     *
     * @param databaseConfig Database connection configuration
     * @param schema Optional schema name to introspect
     * @return Database schema metadata
     */
    fun introspect(
        databaseConfig: DatabaseConfig,
        schema: String? = null
    ): DatabaseSchema {
        val config = CodegenConfig(
            packageName = "temp",
            databaseConfig = databaseConfig,
            schema = schema
        )
        return SchemaIntrospector(config).introspect()
    }

    /**
     * Introspect database schema using DSL.
     *
     * @param block Database configuration block
     * @return Database schema metadata
     */
    fun introspect(block: DatabaseConfig.Builder.() -> Unit): DatabaseSchema {
        val databaseConfig = DatabaseConfig.Builder().apply(block).build()
        return introspect(databaseConfig)
    }
}

/**
 * Extension function to generate code from a database configuration.
 */
fun DatabaseConfig.generateTables(
    packageName: String,
    outputDirectory: String = "src/main/kotlin",
    block: CodegenConfig.Builder.() -> Unit = {}
): List<Path> {
    return KormaCodegen.generate {
        packageName(packageName)
        outputDirectory(outputDirectory)
        database(this@generateTables)
        block()
    }
}
