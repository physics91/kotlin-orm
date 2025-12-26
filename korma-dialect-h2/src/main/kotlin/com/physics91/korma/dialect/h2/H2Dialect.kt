package com.physics91.korma.dialect.h2

import com.physics91.korma.schema.*
import com.physics91.korma.sql.BaseSqlDialect
import com.physics91.korma.sql.SqlDialect

/**
 * H2 Database dialect implementation.
 *
 * H2 is an in-memory database commonly used for testing.
 * It supports most SQL standard features.
 *
 * @see <a href="https://h2database.com">H2 Database</a>
 */
object H2Dialect : BaseSqlDialect() {

    override val name: String = "H2"

    // ============== Feature Support ==============

    override val supportsReturning: Boolean = false // H2 doesn't support RETURNING
    override val supportsOnConflict: Boolean = true // H2 supports MERGE
    override val supportsILike: Boolean = true // H2 supports ILIKE
    override val supportsCTE: Boolean = true
    override val supportsWindowFunctions: Boolean = true
    override val supportsLimitOffset: Boolean = true
    override val supportsBooleanType: Boolean = true

    // ============== Identifier Quoting ==============

    override val identifierQuoteChar: Char = '"'

    // ============== Type Mapping ==============

    override fun sqlTypeName(type: ColumnType<*>): String = when (type) {
        is IntColumnType -> "INT"
        is LongColumnType -> "BIGINT"
        is ShortColumnType -> "SMALLINT"
        is FloatColumnType -> "REAL"
        is DoubleColumnType -> "DOUBLE PRECISION"
        is BooleanColumnType -> "BOOLEAN"
        is VarcharColumnType -> "VARCHAR(${type.length})"
        is CharColumnType -> "CHAR(${type.length})"
        is TextColumnType -> "CLOB"
        is DecimalColumnType -> "DECIMAL(${type.precision}, ${type.scale})"
        is TimestampColumnType -> "TIMESTAMP WITH TIME ZONE"
        is DateColumnType -> "DATE"
        is TimeColumnType -> "TIME"
        is DateTimeColumnType -> "TIMESTAMP"
        is BinaryColumnType -> "BINARY"
        is BlobColumnType -> "BLOB"
        is UUIDColumnType -> "UUID"
        is NullableColumnType<*> -> sqlTypeName(type.delegate)
        else -> type.sqlType()
    }

    override fun autoIncrementType(baseType: ColumnType<*>): String = when (baseType) {
        is IntColumnType -> "INT AUTO_INCREMENT"
        is LongColumnType -> "BIGINT AUTO_INCREMENT"
        else -> "${sqlTypeName(baseType)} AUTO_INCREMENT"
    }

    // ============== ON CONFLICT (Using MERGE) ==============

    /**
     * H2 uses MERGE statement instead of ON CONFLICT.
     * This generates a MERGE ... ON DUPLICATE KEY UPDATE syntax.
     */
    override fun onConflictClause(
        conflictColumns: List<Column<*>>,
        updateColumns: List<Column<*>>
    ): String {
        // H2 2.x supports ON DUPLICATE KEY UPDATE
        val updates = updateColumns.joinToString(", ") {
            "${quoteIdentifier(it.name)} = VALUES(${quoteIdentifier(it.name)})"
        }
        return "ON DUPLICATE KEY UPDATE $updates"
    }

    override fun onConflictDoNothing(conflictColumns: List<Column<*>>): String {
        return "ON DUPLICATE KEY UPDATE ${conflictColumns.first().name} = ${conflictColumns.first().name}"
    }

    // ============== DDL Customization ==============

    override fun createTableStatement(table: Table, ifNotExists: Boolean): String {
        val ifNotExistsClause = if (ifNotExists) "IF NOT EXISTS " else ""

        val columns = table.columns.joinToString(",\n    ") { column ->
            columnDefinition(column)
        }

        // H2 handles PRIMARY KEY inline or as constraint
        val primaryKeyClause = if (table.primaryKey.size > 1) {
            // Composite primary key
            ",\n    PRIMARY KEY (${table.primaryKey.joinToString(", ") { quoteIdentifier(it.name) }})"
        } else if (table.primaryKey.size == 1 && !table.primaryKey.first().isAutoIncrement) {
            // Single column PK (if not auto-increment, which is defined inline)
            ",\n    PRIMARY KEY (${quoteIdentifier(table.primaryKey.first().name)})"
        } else {
            ""
        }

        return """
            |CREATE TABLE $ifNotExistsClause${quoteIdentifier(table.tableName)} (
            |    $columns$primaryKeyClause
            |)
        """.trimMargin()
    }

    override fun columnDefinition(column: Column<*>): String {
        val parts = mutableListOf<String>()

        parts.add(quoteIdentifier(column.name))

        // Type (with AUTO_INCREMENT if applicable)
        if (column.isAutoIncrement) {
            parts.add(autoIncrementType(column.type))
            // AUTO_INCREMENT columns are implicitly PRIMARY KEY in H2
            parts.add("PRIMARY KEY")
        } else {
            parts.add(sqlTypeName(column.type))

            // NOT NULL
            if (column.isNotNull && !column.nullable) {
                parts.add("NOT NULL")
            }

            // PRIMARY KEY (for non-auto-increment single column PK)
            if (column.isPrimaryKey && column.table.primaryKey.size == 1) {
                parts.add("PRIMARY KEY")
            }
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

    // ============== Utility ==============

    override fun currentTimestampExpression(): String = "CURRENT_TIMESTAMP"

    override fun currentDateExpression(): String = "CURRENT_DATE"

    /**
     * H2 connection URL format.
     */
    fun inMemoryUrl(name: String = "test"): String =
        "jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1;MODE=PostgreSQL"

    fun fileUrl(path: String): String =
        "jdbc:h2:file:$path"
}
