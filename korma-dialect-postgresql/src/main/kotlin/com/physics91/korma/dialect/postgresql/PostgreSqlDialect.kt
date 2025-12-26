package com.physics91.korma.dialect.postgresql

import com.physics91.korma.schema.*
import com.physics91.korma.sql.BaseSqlDialect
import java.sql.Types

/**
 * PostgreSQL database dialect implementation.
 *
 * PostgreSQL is a powerful, open-source object-relational database system
 * with a strong reputation for reliability, feature robustness, and performance.
 *
 * Features:
 * - RETURNING clause support
 * - ON CONFLICT (upsert) support
 * - ILIKE case-insensitive matching
 * - JSONB type support
 * - Array type support
 * - Full CTE and window function support
 *
 * @see <a href="https://www.postgresql.org/">PostgreSQL</a>
 */
object PostgreSqlDialect : BaseSqlDialect() {

    override val name: String = "PostgreSQL"

    // ============== Feature Support ==============

    override val supportsReturning: Boolean = true
    override val supportsOnConflict: Boolean = true
    override val supportsILike: Boolean = true
    override val supportsCTE: Boolean = true
    override val supportsWindowFunctions: Boolean = true
    override val supportsLimitOffset: Boolean = true
    override val supportsBooleanType: Boolean = true

    // ============== Identifier Quoting ==============

    override val identifierQuoteChar: Char = '"'

    // ============== Type Mapping ==============

    override fun sqlTypeName(type: ColumnType<*>): String = when (type) {
        is IntColumnType -> "INTEGER"
        is LongColumnType -> "BIGINT"
        is ShortColumnType -> "SMALLINT"
        is FloatColumnType -> "REAL"
        is DoubleColumnType -> "DOUBLE PRECISION"
        is BooleanColumnType -> "BOOLEAN"
        is VarcharColumnType -> "VARCHAR(${type.length})"
        is CharColumnType -> "CHAR(${type.length})"
        is TextColumnType -> "TEXT"
        is DecimalColumnType -> "NUMERIC(${type.precision}, ${type.scale})"
        is TimestampColumnType -> "TIMESTAMP WITH TIME ZONE"
        is DateColumnType -> "DATE"
        is TimeColumnType -> "TIME"
        is DateTimeColumnType -> "TIMESTAMP"
        is BinaryColumnType -> "BYTEA"
        is BlobColumnType -> "BYTEA"
        is UUIDColumnType -> "UUID"
        is NullableColumnType<*> -> sqlTypeName(type.delegate)
        // PostgreSQL-specific types
        is JsonColumnType -> "JSON"
        is JsonbColumnType -> "JSONB"
        is ArrayColumnType<*> -> "${sqlTypeName(type.elementType)}[]"
        is InetColumnType -> "INET"
        is IntervalColumnType -> "INTERVAL"
        else -> type.sqlType()
    }

    override fun autoIncrementType(baseType: ColumnType<*>): String = when (baseType) {
        is IntColumnType -> "SERIAL"
        is LongColumnType -> "BIGSERIAL"
        is ShortColumnType -> "SMALLSERIAL"
        else -> sqlTypeName(baseType)
    }

    // ============== DDL Customization ==============

    override fun columnDefinition(column: Column<*>): String {
        val parts = mutableListOf<String>()

        parts.add(quoteIdentifier(column.name))

        // Type (with SERIAL if applicable)
        if (column.isAutoIncrement) {
            parts.add(autoIncrementType(column.type))
        } else {
            parts.add(sqlTypeName(column.type))
        }

        // NOT NULL
        if (!column.isAutoIncrement && column.isNotNull && !column.nullable) {
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

    // ============== Utility ==============

    override fun currentTimestampExpression(): String = "NOW()"

    override fun currentDateExpression(): String = "CURRENT_DATE"

    /**
     * Generate EXPLAIN ANALYZE for query analysis.
     */
    fun explainAnalyze(sql: String): String = "EXPLAIN ANALYZE $sql"

    /**
     * Generate TRUNCATE with CASCADE option.
     */
    fun truncateTable(table: Table, cascade: Boolean = false, restartIdentity: Boolean = false): String {
        val cascadeClause = if (cascade) " CASCADE" else ""
        val restartClause = if (restartIdentity) " RESTART IDENTITY" else ""
        return "TRUNCATE TABLE ${quoteIdentifier(table.tableName)}$restartClause$cascadeClause"
    }

    /**
     * PostgreSQL connection URL format.
     */
    fun connectionUrl(host: String, port: Int = 5432, database: String): String =
        "jdbc:postgresql://$host:$port/$database"
}

// ============== PostgreSQL-Specific Column Types ==============

/**
 * PostgreSQL JSON column type.
 */
class JsonColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.OTHER
    override fun sqlType(): String = "JSON"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * PostgreSQL JSONB column type (binary JSON with indexing support).
 */
class JsonbColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.OTHER
    override fun sqlType(): String = "JSONB"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * PostgreSQL Array column type.
 */
class ArrayColumnType<T>(val elementType: ColumnType<T>) : ColumnType<List<T>>() {
    override val jdbcType: Int = Types.ARRAY
    override fun sqlType(): String = "${elementType.sqlType()}[]"

    override fun toDb(value: List<T>): Any = value.map { elementType.toDb(it) }.toTypedArray()

    @Suppress("UNCHECKED_CAST")
    override fun fromDb(value: Any?): List<T>? = when (value) {
        is java.sql.Array -> {
            val array = value.array
            when (array) {
                is Array<*> -> array.mapNotNull { elementType.fromDb(it) }
                else -> null
            }
        }
        else -> null
    }
}

/**
 * PostgreSQL INET column type for IP addresses.
 */
class InetColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.OTHER
    override fun sqlType(): String = "INET"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * PostgreSQL INTERVAL for time intervals.
 */
class IntervalColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.OTHER
    override fun sqlType(): String = "INTERVAL"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

// ============== Table Extensions for PostgreSQL Types ==============

/**
 * Define a JSON column.
 */
fun Table.json(name: String): Column<String> = registerCustomColumn(name, JsonColumnType())

/**
 * Define a JSONB column (recommended for most use cases).
 */
fun Table.jsonb(name: String): Column<String> = registerCustomColumn(name, JsonbColumnType())

/**
 * Define an array column.
 */
fun <T> Table.array(name: String, elementType: ColumnType<T>): Column<List<T>> =
    registerCustomColumn(name, ArrayColumnType(elementType))

/**
 * Define an integer array column.
 */
fun Table.intArray(name: String): Column<List<Int>> = array(name, IntColumnType)

/**
 * Define a text array column.
 */
fun Table.textArray(name: String): Column<List<String>> = array(name, TextColumnType)

/**
 * Define an INET column for IP addresses.
 */
fun Table.inet(name: String): Column<String> = registerCustomColumn(name, InetColumnType())

/**
 * Define an INTERVAL column.
 */
fun Table.interval(name: String): Column<String> = registerCustomColumn(name, IntervalColumnType())
