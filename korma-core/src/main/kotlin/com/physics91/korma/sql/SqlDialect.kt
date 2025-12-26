package com.physics91.korma.sql

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table

/**
 * Database dialect interface for SQL generation.
 *
 * Different databases have different SQL syntax variations.
 * This interface abstracts those differences.
 */
interface SqlDialect {
    /** Name of this dialect (e.g., "PostgreSQL", "MySQL", "H2") */
    val name: String

    // ============== Feature Support ==============

    /** Whether this dialect supports RETURNING clause */
    val supportsReturning: Boolean

    /** Whether this dialect supports ON CONFLICT (upsert) */
    val supportsOnConflict: Boolean

    /** Whether this dialect supports ILIKE (case-insensitive LIKE) */
    val supportsILike: Boolean

    /** Whether this dialect supports Common Table Expressions (WITH) */
    val supportsCTE: Boolean

    /** Whether this dialect supports window functions */
    val supportsWindowFunctions: Boolean

    /** Whether this dialect supports LIMIT/OFFSET syntax */
    val supportsLimitOffset: Boolean

    /** Whether this dialect supports boolean type natively */
    val supportsBooleanType: Boolean

    // ============== Identifier Quoting ==============

    /** Quote character for identifiers (e.g., '"' for PostgreSQL, '`' for MySQL) */
    val identifierQuoteChar: Char get() = '"'

    /**
     * Quote an identifier (table name, column name, etc.)
     * to handle reserved words and special characters.
     */
    fun quoteIdentifier(identifier: String): String {
        return "$identifierQuoteChar$identifier$identifierQuoteChar"
    }

    // ============== Type Mapping ==============

    /**
     * Get the SQL type name for a column type.
     * Dialects may override this for database-specific types.
     */
    fun sqlTypeName(type: ColumnType<*>): String = type.sqlType()

    /**
     * Get the SQL for auto-increment column.
     */
    fun autoIncrementType(baseType: ColumnType<*>): String

    // ============== Query Generation ==============

    /**
     * Generate LIMIT/OFFSET clause.
     * @return SQL fragment for pagination or empty string if not supported
     */
    fun limitOffsetClause(limit: Int?, offset: Long?): String {
        if (limit == null && offset == null) return ""

        val limitPart = limit?.let { "LIMIT $it" } ?: ""
        val offsetPart = offset?.let { "OFFSET $it" } ?: ""

        return listOf(limitPart, offsetPart)
            .filter { it.isNotEmpty() }
            .joinToString(" ")
    }

    /**
     * Generate INSERT ... RETURNING clause.
     * @return SQL fragment or empty string if not supported
     */
    fun returningClause(columns: List<Column<*>>): String {
        if (!supportsReturning || columns.isEmpty()) return ""
        return "RETURNING " + columns.joinToString(", ") { quoteIdentifier(it.name) }
    }

    /**
     * Generate ON CONFLICT clause for upsert.
     */
    fun onConflictClause(
        conflictColumns: List<Column<*>>,
        updateColumns: List<Column<*>>
    ): String {
        if (!supportsOnConflict) {
            throw UnsupportedOperationException("$name does not support ON CONFLICT")
        }

        val conflictTarget = conflictColumns.joinToString(", ") { quoteIdentifier(it.name) }
        val updates = updateColumns.joinToString(", ") {
            "${quoteIdentifier(it.name)} = EXCLUDED.${quoteIdentifier(it.name)}"
        }

        return "ON CONFLICT ($conflictTarget) DO UPDATE SET $updates"
    }

    /**
     * Generate ON CONFLICT DO NOTHING clause.
     */
    fun onConflictDoNothing(conflictColumns: List<Column<*>>): String {
        if (!supportsOnConflict) {
            throw UnsupportedOperationException("$name does not support ON CONFLICT")
        }

        val conflictTarget = conflictColumns.joinToString(", ") { quoteIdentifier(it.name) }
        return "ON CONFLICT ($conflictTarget) DO NOTHING"
    }

    // ============== DDL Generation ==============

    /**
     * Generate CREATE TABLE statement.
     */
    fun createTableStatement(table: Table, ifNotExists: Boolean = false): String {
        val ifNotExistsClause = if (ifNotExists) "IF NOT EXISTS " else ""

        val columns = table.columns.joinToString(",\n    ") { column ->
            columnDefinition(column)
        }

        val primaryKeyClause = if (table.primaryKey.isNotEmpty()) {
            ",\n    PRIMARY KEY (${table.primaryKey.joinToString(", ") { quoteIdentifier(it.name) }})"
        } else ""

        return """
            |CREATE TABLE $ifNotExistsClause${quoteIdentifier(table.tableName)} (
            |    $columns$primaryKeyClause
            |)
        """.trimMargin()
    }

    /**
     * Generate column definition for CREATE TABLE.
     */
    fun columnDefinition(column: Column<*>): String {
        val parts = mutableListOf<String>()

        parts.add(quoteIdentifier(column.name))

        // Type
        if (column.isAutoIncrement) {
            parts.add(autoIncrementType(column.type))
        } else {
            parts.add(sqlTypeName(column.type))
        }

        // NOT NULL
        if (column.isNotNull && !column.nullable) {
            parts.add("NOT NULL")
        }

        // UNIQUE
        if (column.isUnique && !column.isPrimaryKey) {
            parts.add("UNIQUE")
        }

        // DEFAULT
        column.defaultValue?.let {
            parts.add("DEFAULT $it")
        }

        return parts.joinToString(" ")
    }

    /**
     * Generate DROP TABLE statement.
     */
    fun dropTableStatement(table: Table, ifExists: Boolean = true): String {
        val ifExistsClause = if (ifExists) "IF EXISTS " else ""
        return "DROP TABLE $ifExistsClause${quoteIdentifier(table.tableName)}"
    }

    /**
     * Generate CREATE INDEX statement.
     */
    fun createIndexStatement(
        indexName: String,
        table: Table,
        columns: List<Column<*>>,
        unique: Boolean = false,
        ifNotExists: Boolean = false
    ): String {
        val uniqueClause = if (unique) "UNIQUE " else ""
        val ifNotExistsClause = if (ifNotExists) "IF NOT EXISTS " else ""
        val columnNames = columns.joinToString(", ") { quoteIdentifier(it.name) }

        return "CREATE ${uniqueClause}INDEX $ifNotExistsClause${quoteIdentifier(indexName)} " +
                "ON ${quoteIdentifier(table.tableName)} ($columnNames)"
    }

    /**
     * Generate DROP INDEX statement.
     */
    fun dropIndexStatement(indexName: String, table: Table? = null, ifExists: Boolean = true): String {
        val ifExistsClause = if (ifExists) "IF EXISTS " else ""
        return "DROP INDEX $ifExistsClause${quoteIdentifier(indexName)}"
    }

    // ============== Batch Operations ==============

    /**
     * Whether this dialect supports multi-row INSERT.
     * INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)
     */
    val supportsMultiRowInsert: Boolean get() = true

    /**
     * Maximum number of rows in a single INSERT statement.
     * Returns null if no limit.
     */
    val maxInsertBatchSize: Int? get() = null

    // ============== Utility ==============

    /**
     * Get the current timestamp expression.
     */
    fun currentTimestampExpression(): String = "CURRENT_TIMESTAMP"

    /**
     * Get the current date expression.
     */
    fun currentDateExpression(): String = "CURRENT_DATE"

    /**
     * Escape a string literal.
     */
    fun escapeString(value: String): String = value.replace("'", "''")

    /**
     * Get a string literal.
     */
    fun stringLiteral(value: String): String = "'${escapeString(value)}'"
}

/**
 * Base implementation with common defaults.
 */
abstract class BaseSqlDialect : SqlDialect {
    override val supportsReturning: Boolean = false
    override val supportsOnConflict: Boolean = false
    override val supportsILike: Boolean = false
    override val supportsCTE: Boolean = true
    override val supportsWindowFunctions: Boolean = true
    override val supportsLimitOffset: Boolean = true
    override val supportsBooleanType: Boolean = true
}
