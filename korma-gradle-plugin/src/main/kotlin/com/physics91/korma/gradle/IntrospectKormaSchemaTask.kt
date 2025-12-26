package com.physics91.korma.gradle

import com.physics91.korma.codegen.KormaCodegen
import com.physics91.korma.codegen.config.DatabaseConfig
import org.gradle.api.DefaultTask
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction

/**
 * Gradle task to introspect and display database schema.
 */
abstract class IntrospectKormaSchemaTask : DefaultTask() {

    init {
        group = "korma"
        description = "Introspect and display database schema"
    }

    @get:Input
    abstract val databaseUrl: Property<String>

    @get:Input
    @get:Optional
    abstract val databaseUsername: Property<String>

    @get:Input
    @get:Optional
    abstract val databasePassword: Property<String>

    @get:Input
    @get:Optional
    abstract val driverClassName: Property<String>

    @get:Input
    @get:Optional
    abstract val databaseSchema: Property<String>

    @TaskAction
    fun introspect() {
        val dbConfig = DatabaseConfig(
            url = databaseUrl.get(),
            username = databaseUsername.orNull,
            password = databasePassword.orNull,
            driverClassName = driverClassName.orNull
        )

        val schema = KormaCodegen.introspect(dbConfig, databaseSchema.orNull)

        println("\n=== Database Schema ===")
        println("Found ${schema.tables.size} tables:\n")

        schema.tables.forEach { table ->
            println("Table: ${table.name}")
            table.remarks?.let { println("  Description: $it") }

            println("  Columns:")
            table.columns.forEach { column ->
                val nullable = if (column.nullable) "NULL" else "NOT NULL"
                val pk = if (table.primaryKeyColumns.contains(column.name)) " [PK]" else ""
                val autoInc = if (column.autoIncrement) " [AUTO]" else ""
                println("    - ${column.name}: ${column.typeName} $nullable$pk$autoInc")
            }

            if (table.foreignKeys.isNotEmpty()) {
                println("  Foreign Keys:")
                table.foreignKeys.forEach { fk ->
                    println("    - ${fk.name}: ${fk.localColumns} -> ${fk.foreignTable}(${fk.foreignColumns})")
                }
            }

            println()
        }
    }
}
