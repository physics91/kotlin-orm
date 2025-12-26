package com.physics91.korma.migration

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.SqlDialect

/**
 * Compares entity definitions with actual database schema
 * and generates migration operations to reconcile differences.
 */
class SchemaComparator(
    private val executor: MigrationExecutor,
    private val dialect: SqlDialect
) {
    /**
     * Compare a Table definition with the database schema.
     */
    fun compare(table: Table): SchemaComparisonResult {
        val dbSchema = fetchDatabaseSchema(table.tableName)

        if (dbSchema == null) {
            // Table doesn't exist
            return SchemaComparisonResult(
                tableName = table.tableName,
                tableExists = false,
                missingColumns = table.columns.map { it.toMigrationColumn() },
                extraColumns = emptyList(),
                modifiedColumns = emptyList(),
                missingIndexes = emptyList(),
                extraIndexes = emptyList()
            )
        }

        val entityColumns = table.columns.associateBy { it.name.lowercase() }
        val dbColumns = dbSchema.columns.associateBy { it.name.lowercase() }

        // Find missing columns (in entity but not in DB)
        val missingColumns = entityColumns.filter { (name, _) ->
            name !in dbColumns
        }.values.map { it.toMigrationColumn() }

        // Find extra columns (in DB but not in entity)
        val extraColumns = dbColumns.filter { (name, _) ->
            name !in entityColumns
        }.values.toList()

        // Find modified columns (type or constraints changed)
        val modifiedColumns = entityColumns.filter { (name, entityCol) ->
            val dbCol = dbColumns[name] ?: return@filter false
            !isColumnEqual(entityCol, dbCol)
        }.values.map { ColumnModification(it.name, it.toMigrationColumn()) }

        return SchemaComparisonResult(
            tableName = table.tableName,
            tableExists = true,
            missingColumns = missingColumns,
            extraColumns = extraColumns,
            modifiedColumns = modifiedColumns,
            missingIndexes = emptyList(),
            extraIndexes = emptyList()
        )
    }

    /**
     * Generate migration operations from comparison result.
     */
    fun generateMigrationOperations(result: SchemaComparisonResult): List<MigrationOperation> {
        val operations = mutableListOf<MigrationOperation>()

        // Add missing columns
        result.missingColumns.forEach { column ->
            operations.add(MigrationOperation.AddColumn(result.tableName, column))
        }

        // Modify changed columns
        result.modifiedColumns.forEach { modification ->
            operations.add(MigrationOperation.ModifyColumn(result.tableName, modification.newDefinition))
        }

        // Note: Dropping extra columns is not done automatically for safety
        // User should explicitly decide to drop columns

        return operations
    }

    /**
     * Generate a migration class that syncs the schema.
     */
    fun generateMigrationCode(
        tables: List<Table>,
        version: String,
        description: String
    ): String {
        val comparisons = tables.map { compare(it) }
        val allOperations = comparisons.flatMap { generateMigrationOperations(it) }

        if (allOperations.isEmpty()) {
            return "// No schema differences found"
        }

        return buildString {
            appendLine("class ${sanitizeClassName(version)} : BaseMigration(")
            appendLine("    version = \"$version\",")
            appendLine("    description = \"$description\"")
            appendLine(") {")
            appendLine("    override fun MigrationContext.up() {")

            for (comparison in comparisons) {
                if (!comparison.tableExists) {
                    appendLine("        // Create table: ${comparison.tableName}")
                    appendLine("        createTable(\"${comparison.tableName}\") {")
                    for (col in comparison.missingColumns) {
                        appendLine("            ${generateColumnCode(col)}")
                    }
                    appendLine("        }")
                } else {
                    for (col in comparison.missingColumns) {
                        appendLine("        addColumn(\"${comparison.tableName}\", \"${col.name}\") {")
                        appendLine("            ${generateColumnBuilderCode(col)}")
                        appendLine("        }")
                    }
                    for (mod in comparison.modifiedColumns) {
                        appendLine("        // Modify column: ${mod.columnName}")
                        appendLine("        modifyColumn(\"${comparison.tableName}\", \"${mod.columnName}\") {")
                        appendLine("            ${generateColumnBuilderCode(mod.newDefinition)}")
                        appendLine("        }")
                    }
                }
            }

            appendLine("    }")
            appendLine()
            appendLine("    override fun MigrationContext.down() {")

            // Generate reverse operations
            for (comparison in comparisons.reversed()) {
                if (!comparison.tableExists) {
                    appendLine("        dropTable(\"${comparison.tableName}\")")
                } else {
                    for (col in comparison.missingColumns.reversed()) {
                        appendLine("        dropColumn(\"${comparison.tableName}\", \"${col.name}\")")
                    }
                }
            }

            appendLine("    }")
            appendLine("}")
        }
    }

    // ============== Private Methods ==============

    private fun fetchDatabaseSchema(tableName: String): DatabaseTableInfo? {
        // Query information_schema for table structure
        // This is database-specific; using ANSI SQL standard queries

        val columnsSql = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE LOWER(table_name) = LOWER('$tableName')
            ORDER BY ordinal_position
        """.trimIndent()

        val results = try {
            executor.executeQuery(columnsSql)
        } catch (e: Exception) {
            // Table might not exist or information_schema not available
            return null
        }

        if (results.isEmpty()) {
            return null
        }

        val columns = results.map { row ->
            DatabaseColumnInfo(
                name = row["column_name"]?.toString() ?: row["COLUMN_NAME"]?.toString() ?: "",
                dataType = row["data_type"]?.toString() ?: row["DATA_TYPE"]?.toString() ?: "",
                maxLength = (row["character_maximum_length"] as? Number)?.toInt()
                    ?: (row["CHARACTER_MAXIMUM_LENGTH"] as? Number)?.toInt(),
                precision = (row["numeric_precision"] as? Number)?.toInt()
                    ?: (row["NUMERIC_PRECISION"] as? Number)?.toInt(),
                scale = (row["numeric_scale"] as? Number)?.toInt()
                    ?: (row["NUMERIC_SCALE"] as? Number)?.toInt(),
                isNullable = (row["is_nullable"]?.toString() ?: row["IS_NULLABLE"]?.toString())
                    ?.uppercase() == "YES",
                defaultValue = row["column_default"]?.toString() ?: row["COLUMN_DEFAULT"]?.toString()
            )
        }

        return DatabaseTableInfo(tableName, columns)
    }

    private fun isColumnEqual(entityCol: Column<*>, dbCol: DatabaseColumnInfo): Boolean {
        // Compare nullability
        if (entityCol.nullable != dbCol.isNullable) {
            return false
        }

        // Type comparison is complex and database-specific
        // This is a simplified check
        val entityTypeName = dialect.sqlTypeName(entityCol.type).uppercase()
        val dbTypeName = dbCol.dataType.uppercase()

        // Basic type matching
        return when {
            entityTypeName.contains("INT") && dbTypeName.contains("INT") -> true
            entityTypeName.contains("VARCHAR") && dbTypeName.contains("VARCHAR") -> true
            entityTypeName.contains("TEXT") && (dbTypeName.contains("TEXT") || dbTypeName.contains("CLOB")) -> true
            entityTypeName.contains("BOOLEAN") && (dbTypeName.contains("BOOL") || dbTypeName.contains("BIT")) -> true
            entityTypeName.contains("TIMESTAMP") && dbTypeName.contains("TIMESTAMP") -> true
            entityTypeName == dbTypeName -> true
            else -> false
        }
    }

    private fun generateColumnCode(col: MigrationColumn<*>): String {
        val type = col.type
        val typeName = type.sqlType().lowercase()

        val builder = StringBuilder()
        builder.append(when {
            typeName.contains("bigint") -> "long(\"${col.name}\")"
            typeName.contains("smallint") -> "smallint(\"${col.name}\")"
            typeName.contains("int") -> "integer(\"${col.name}\")"
            typeName.contains("double") -> "double(\"${col.name}\")"
            typeName.contains("float") || typeName.contains("real") -> "float(\"${col.name}\")"
            typeName.contains("decimal") || typeName.contains("numeric") ->
                "decimal(\"${col.name}\", ${extractPrecision(typeName)}, ${extractScale(typeName)})"
            typeName.contains("varchar") -> "varchar(\"${col.name}\", ${extractLength(typeName)})"
            typeName.contains("char") -> "char(\"${col.name}\", ${extractLength(typeName)})"
            typeName.contains("text") -> "text(\"${col.name}\")"
            typeName.contains("blob") -> "blob(\"${col.name}\")"
            typeName.contains("binary") || typeName.contains("bytea") -> "binary(\"${col.name}\")"
            typeName.contains("boolean") || typeName.contains("bool") -> "boolean(\"${col.name}\")"
            typeName.contains("uuid") -> "uuid(\"${col.name}\")"
            typeName.contains("timestamp") && typeName.contains("time zone") -> "timestamp(\"${col.name}\")"
            typeName.contains("timestamp") -> "datetime(\"${col.name}\")"
            typeName.contains("datetime") -> "datetime(\"${col.name}\")"
            typeName.contains("time") -> "time(\"${col.name}\")"
            typeName.contains("date") -> "date(\"${col.name}\")"
            else -> "/* Unknown type: $typeName */"
        })

        if (col.isAutoIncrement) {
            builder.append(".autoIncrement()")
        } else if (col.isPrimaryKey) {
            builder.append(".primaryKey()")
        }
        if (col.isUnique) builder.append(".unique()")
        if (col.isNullable) builder.append(".nullable()")
        generateDefaultCall(col.defaultValue)?.let { builder.append(".").append(it) }

        return builder.toString()
    }

    private fun generateColumnBuilderCode(col: MigrationColumn<*>): String {
        val type = col.type
        val typeName = type.sqlType().lowercase()

        val parts = mutableListOf<String>()
        parts.add(when {
            typeName.contains("bigint") -> "long()"
            typeName.contains("smallint") -> "smallint()"
            typeName.contains("int") -> "integer()"
            typeName.contains("double") -> "double()"
            typeName.contains("float") || typeName.contains("real") -> "float()"
            typeName.contains("decimal") || typeName.contains("numeric") -> "decimal(${extractPrecision(typeName)}, ${extractScale(typeName)})"
            typeName.contains("varchar") -> "varchar(${extractLength(typeName)})"
            typeName.contains("char") -> "char(${extractLength(typeName)})"
            typeName.contains("text") -> "text()"
            typeName.contains("blob") -> "blob()"
            typeName.contains("binary") || typeName.contains("bytea") -> "binary()"
            typeName.contains("boolean") || typeName.contains("bool") -> "boolean()"
            typeName.contains("uuid") -> "uuid()"
            typeName.contains("timestamp") && typeName.contains("time zone") -> "timestamp()"
            typeName.contains("timestamp") -> "datetime()"
            typeName.contains("datetime") -> "datetime()"
            typeName.contains("time") -> "time()"
            typeName.contains("date") -> "date()"
            else -> "/* Unknown type */"
        })

        if (col.isAutoIncrement) {
            parts.add("autoIncrement()")
        } else if (col.isPrimaryKey) {
            parts.add("primaryKey()")
        }
        if (col.isNullable) parts.add("nullable()")
        if (col.isUnique) parts.add("unique()")
        generateDefaultCall(col.defaultValue)?.let { parts.add(it) }

        return parts.joinToString(".")
    }

    private fun extractLength(typeName: String): Int {
        val match = Regex("\\((\\d+)\\)").find(typeName)
        return match?.groupValues?.get(1)?.toIntOrNull() ?: 255
    }

    private fun extractPrecision(typeName: String): Int {
        val match = Regex("\\((\\d+)").find(typeName)
        return match?.groupValues?.get(1)?.toIntOrNull() ?: 10
    }

    private fun extractScale(typeName: String): Int {
        val match = Regex(",\\s*(\\d+)\\)").find(typeName)
        return match?.groupValues?.get(1)?.toIntOrNull() ?: 2
    }

    private fun generateDefaultCall(defaultValue: Any?): String? {
        return when (defaultValue) {
            null -> null
            is SqlDefault -> "defaultExpression(\"${escapeKotlinString(defaultValue.sql)}\")"
            is String -> "default(\"${escapeKotlinString(defaultValue)}\")"
            is Char -> "default('${escapeKotlinChar(defaultValue)}')"
            is Boolean -> "default($defaultValue)"
            is Number -> "default($defaultValue)"
            else -> "default(\"${escapeKotlinString(defaultValue.toString())}\")"
        }
    }

    private fun escapeKotlinString(value: String): String {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
    }

    private fun escapeKotlinChar(value: Char): String {
        return when (value) {
            '\\' -> "\\\\"
            '\'' -> "\\'"
            else -> value.toString()
        }
    }

    private fun sanitizeClassName(version: String): String {
        return "Migration_${version.replace(Regex("[^a-zA-Z0-9]"), "_")}"       
    }
}

/**
 * Database table information.
 */
data class DatabaseTableInfo(
    val name: String,
    val columns: List<DatabaseColumnInfo>
)

/**
 * Database column information.
 */
data class DatabaseColumnInfo(
    val name: String,
    val dataType: String,
    val maxLength: Int?,
    val precision: Int?,
    val scale: Int?,
    val isNullable: Boolean,
    val defaultValue: String?
)

/**
 * Result of schema comparison.
 */
data class SchemaComparisonResult(
    val tableName: String,
    val tableExists: Boolean,
    val missingColumns: List<MigrationColumn<*>>,
    val extraColumns: List<DatabaseColumnInfo>,
    val modifiedColumns: List<ColumnModification>,
    val missingIndexes: List<String>,
    val extraIndexes: List<String>
) {
    val hasDifferences: Boolean
        get() = missingColumns.isNotEmpty() ||
                extraColumns.isNotEmpty() ||
                modifiedColumns.isNotEmpty() ||
                missingIndexes.isNotEmpty() ||
                extraIndexes.isNotEmpty() ||
                !tableExists
}

/**
 * Column modification details.
 */
data class ColumnModification(
    val columnName: String,
    val newDefinition: MigrationColumn<*>
)
