package com.physics91.korma.dialect.mysql

import com.physics91.korma.schema.*
import com.physics91.korma.sql.BaseSqlDialect
import java.sql.Types

/**
 * MySQL/MariaDB database dialect implementation.
 *
 * MySQL is one of the most popular open-source relational database management systems.
 * MariaDB is a community-developed fork of MySQL with additional features.
 *
 * Features:
 * - ON DUPLICATE KEY UPDATE for upsert operations
 * - AUTO_INCREMENT for identity columns
 * - ENUM and SET types
 * - JSON type support (MySQL 5.7+)
 * - Full-text search
 *
 * Limitations:
 * - No RETURNING clause (use LAST_INSERT_ID() instead)
 * - Limited window function support in older versions
 *
 * @see <a href="https://www.mysql.com/">MySQL</a>
 * @see <a href="https://mariadb.org/">MariaDB</a>
 */
object MySqlDialect : BaseSqlDialect() {

    override val name: String = "MySQL"

    // ============== Feature Support ==============

    override val supportsReturning: Boolean = false
    override val supportsOnConflict: Boolean = false  // Uses ON DUPLICATE KEY UPDATE instead
    override val supportsILike: Boolean = false  // Use LOWER() or COLLATE instead
    override val supportsCTE: Boolean = true  // MySQL 8.0+
    override val supportsWindowFunctions: Boolean = true  // MySQL 8.0+
    override val supportsLimitOffset: Boolean = true
    override val supportsBooleanType: Boolean = false  // Uses TINYINT(1)

    /**
     * MySQL uses ON DUPLICATE KEY UPDATE instead of ON CONFLICT.
     */
    val supportsOnDuplicateKeyUpdate: Boolean = true

    // ============== Identifier Quoting ==============

    override val identifierQuoteChar: Char = '`'

    // ============== Type Mapping ==============

    override fun sqlTypeName(type: ColumnType<*>): String = when (type) {
        is IntColumnType -> "INT"
        is LongColumnType -> "BIGINT"
        is ShortColumnType -> "SMALLINT"
        is FloatColumnType -> "FLOAT"
        is DoubleColumnType -> "DOUBLE"
        is BooleanColumnType -> "TINYINT(1)"
        is VarcharColumnType -> "VARCHAR(${type.length})"
        is CharColumnType -> "CHAR(${type.length})"
        is TextColumnType -> "TEXT"
        is DecimalColumnType -> "DECIMAL(${type.precision}, ${type.scale})"
        is TimestampColumnType -> "TIMESTAMP"
        is DateColumnType -> "DATE"
        is TimeColumnType -> "TIME"
        is DateTimeColumnType -> "DATETIME"
        is BinaryColumnType -> "VARBINARY(255)"
        is BlobColumnType -> "BLOB"
        is UUIDColumnType -> "CHAR(36)"  // MySQL doesn't have native UUID
        is NullableColumnType<*> -> sqlTypeName(type.delegate)
        // MySQL-specific types
        is TinyIntColumnType -> "TINYINT"
        is MediumIntColumnType -> "MEDIUMINT"
        is TinyTextColumnType -> "TINYTEXT"
        is MediumTextColumnType -> "MEDIUMTEXT"
        is LongTextColumnType -> "LONGTEXT"
        is TinyBlobColumnType -> "TINYBLOB"
        is MediumBlobColumnType -> "MEDIUMBLOB"
        is LongBlobColumnType -> "LONGBLOB"
        is JsonColumnType -> "JSON"
        is EnumColumnType -> "ENUM(${type.values.joinToString(",") { "'$it'" }})"
        is SetColumnType -> "SET(${type.values.joinToString(",") { "'$it'" }})"
        is YearColumnType -> "YEAR"
        is BitColumnType -> "BIT(${type.length})"
        else -> type.sqlType()
    }

    override fun autoIncrementType(baseType: ColumnType<*>): String = sqlTypeName(baseType)

    // ============== DDL Customization ==============

    override fun columnDefinition(column: Column<*>): String {
        val parts = mutableListOf<String>()

        parts.add(quoteIdentifier(column.name))
        parts.add(sqlTypeName(column.type))

        // NOT NULL
        if (column.isNotNull && !column.nullable) {
            parts.add("NOT NULL")
        }

        // AUTO_INCREMENT
        if (column.isAutoIncrement) {
            parts.add("AUTO_INCREMENT")
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

    override fun createTableStatement(table: Table, ifNotExists: Boolean): String {
        val columns = table.columns.joinToString(",\n    ") { columnDefinition(it) }

        val primaryKeys = table.columns.filter { it.isPrimaryKey }
        val pkClause = if (primaryKeys.isNotEmpty()) {
            ",\n    PRIMARY KEY (${primaryKeys.joinToString(", ") { quoteIdentifier(it.name) }})"
        } else ""

        val ifNotExistsClause = if (ifNotExists) "IF NOT EXISTS " else ""
        return """
            |CREATE TABLE $ifNotExistsClause${quoteIdentifier(table.tableName)} (
            |    $columns$pkClause
            |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.trimMargin()
    }

    // ============== MySQL-Specific Operations ==============

    /**
     * Generate ON DUPLICATE KEY UPDATE clause.
     * MySQL's alternative to UPSERT.
     */
    fun onDuplicateKeyUpdate(updateColumns: List<Column<*>>): String {
        if (updateColumns.isEmpty()) return ""
        val updates = updateColumns.joinToString(", ") { col ->
            "${quoteIdentifier(col.name)} = VALUES(${quoteIdentifier(col.name)})"
        }
        return "ON DUPLICATE KEY UPDATE $updates"
    }

    /**
     * Generate INSERT IGNORE statement.
     */
    fun insertIgnore(table: Table): String = "INSERT IGNORE INTO ${quoteIdentifier(table.tableName)}"

    /**
     * Generate REPLACE INTO statement.
     */
    fun replaceInto(table: Table): String = "REPLACE INTO ${quoteIdentifier(table.tableName)}"

    // ============== Utility ==============

    override fun currentTimestampExpression(): String = "NOW()"

    override fun currentDateExpression(): String = "CURDATE()"

    /**
     * Get last inserted auto-increment ID.
     */
    fun lastInsertId(): String = "LAST_INSERT_ID()"

    /**
     * Generate EXPLAIN for query analysis.
     */
    fun explain(sql: String): String = "EXPLAIN $sql"

    /**
     * Generate EXPLAIN ANALYZE (MySQL 8.0.18+).
     */
    fun explainAnalyze(sql: String): String = "EXPLAIN ANALYZE $sql"

    /**
     * MySQL connection URL format.
     */
    fun connectionUrl(
        host: String,
        port: Int = 3306,
        database: String,
        useSSL: Boolean = false,
        serverTimezone: String = "UTC"
    ): String {
        val params = mutableListOf<String>()
        params.add("useSSL=$useSSL")
        params.add("serverTimezone=$serverTimezone")
        params.add("characterEncoding=UTF-8")
        return "jdbc:mysql://$host:$port/$database?${params.joinToString("&")}"
    }
}

// ============== MySQL-Specific Column Types ==============

/**
 * MySQL TINYINT column type (-128 to 127 or 0 to 255 unsigned).
 */
class TinyIntColumnType : ColumnType<Byte>() {
    override val jdbcType: Int = Types.TINYINT
    override fun sqlType(): String = "TINYINT"
    override fun toDb(value: Byte): Any = value
    override fun fromDb(value: Any?): Byte? = when (value) {
        is Byte -> value
        is Number -> value.toByte()
        else -> null
    }
}

/**
 * MySQL MEDIUMINT column type (-8388608 to 8388607).
 */
class MediumIntColumnType : ColumnType<Int>() {
    override val jdbcType: Int = Types.INTEGER
    override fun sqlType(): String = "MEDIUMINT"
    override fun toDb(value: Int): Any = value
    override fun fromDb(value: Any?): Int? = when (value) {
        is Int -> value
        is Number -> value.toInt()
        else -> null
    }
}

/**
 * MySQL TINYTEXT column type (max 255 bytes).
 */
class TinyTextColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.VARCHAR
    override fun sqlType(): String = "TINYTEXT"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * MySQL MEDIUMTEXT column type (max 16MB).
 */
class MediumTextColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.LONGVARCHAR
    override fun sqlType(): String = "MEDIUMTEXT"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * MySQL LONGTEXT column type (max 4GB).
 */
class LongTextColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.LONGVARCHAR
    override fun sqlType(): String = "LONGTEXT"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * MySQL TINYBLOB column type (max 255 bytes).
 */
class TinyBlobColumnType : ColumnType<ByteArray>() {
    override val jdbcType: Int = Types.VARBINARY
    override fun sqlType(): String = "TINYBLOB"
    override fun toDb(value: ByteArray): Any = value
    override fun fromDb(value: Any?): ByteArray? = when (value) {
        is ByteArray -> value
        else -> null
    }
}

/**
 * MySQL MEDIUMBLOB column type (max 16MB).
 */
class MediumBlobColumnType : ColumnType<ByteArray>() {
    override val jdbcType: Int = Types.LONGVARBINARY
    override fun sqlType(): String = "MEDIUMBLOB"
    override fun toDb(value: ByteArray): Any = value
    override fun fromDb(value: Any?): ByteArray? = when (value) {
        is ByteArray -> value
        else -> null
    }
}

/**
 * MySQL LONGBLOB column type (max 4GB).
 */
class LongBlobColumnType : ColumnType<ByteArray>() {
    override val jdbcType: Int = Types.LONGVARBINARY
    override fun sqlType(): String = "LONGBLOB"
    override fun toDb(value: ByteArray): Any = value
    override fun fromDb(value: Any?): ByteArray? = when (value) {
        is ByteArray -> value
        else -> null
    }
}

/**
 * MySQL JSON column type.
 */
class JsonColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.LONGVARCHAR
    override fun sqlType(): String = "JSON"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * MySQL ENUM column type.
 */
class EnumColumnType(val values: List<String>) : ColumnType<String>() {
    override val jdbcType: Int = Types.VARCHAR
    override fun sqlType(): String = "ENUM(${values.joinToString(",") { "'$it'" }})"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/**
 * MySQL SET column type.
 */
class SetColumnType(val values: List<String>) : ColumnType<Set<String>>() {
    override val jdbcType: Int = Types.VARCHAR
    override fun sqlType(): String = "SET(${values.joinToString(",") { "'$it'" }})"
    override fun toDb(value: Set<String>): Any = value.joinToString(",")
    override fun fromDb(value: Any?): Set<String>? = value?.toString()?.split(",")?.toSet()
}

/**
 * MySQL YEAR column type.
 */
class YearColumnType : ColumnType<Int>() {
    override val jdbcType: Int = Types.INTEGER
    override fun sqlType(): String = "YEAR"
    override fun toDb(value: Int): Any = value
    override fun fromDb(value: Any?): Int? = when (value) {
        is Int -> value
        is Number -> value.toInt()
        else -> null
    }
}

/**
 * MySQL BIT column type.
 */
class BitColumnType(val length: Int = 1) : ColumnType<ByteArray>() {
    override val jdbcType: Int = Types.BIT
    override fun sqlType(): String = "BIT($length)"
    override fun toDb(value: ByteArray): Any = value
    override fun fromDb(value: Any?): ByteArray? = when (value) {
        is ByteArray -> value
        is Boolean -> if (value) byteArrayOf(1) else byteArrayOf(0)
        else -> null
    }
}

// ============== Table Extensions for MySQL Types ==============

/**
 * Define a TINYINT column.
 */
fun Table.tinyInt(name: String): Column<Byte> = registerCustomColumn(name, TinyIntColumnType())

/**
 * Define a MEDIUMINT column.
 */
fun Table.mediumInt(name: String): Column<Int> = registerCustomColumn(name, MediumIntColumnType())

/**
 * Define a TINYTEXT column.
 */
fun Table.tinyText(name: String): Column<String> = registerCustomColumn(name, TinyTextColumnType())

/**
 * Define a MEDIUMTEXT column.
 */
fun Table.mediumText(name: String): Column<String> = registerCustomColumn(name, MediumTextColumnType())

/**
 * Define a LONGTEXT column.
 */
fun Table.longText(name: String): Column<String> = registerCustomColumn(name, LongTextColumnType())

/**
 * Define a TINYBLOB column.
 */
fun Table.tinyBlob(name: String): Column<ByteArray> = registerCustomColumn(name, TinyBlobColumnType())

/**
 * Define a MEDIUMBLOB column.
 */
fun Table.mediumBlob(name: String): Column<ByteArray> = registerCustomColumn(name, MediumBlobColumnType())

/**
 * Define a LONGBLOB column.
 */
fun Table.longBlob(name: String): Column<ByteArray> = registerCustomColumn(name, LongBlobColumnType())

/**
 * Define a JSON column.
 */
fun Table.json(name: String): Column<String> = registerCustomColumn(name, JsonColumnType())

/**
 * Define an ENUM column.
 */
fun Table.enum(name: String, vararg values: String): Column<String> =
    registerCustomColumn(name, EnumColumnType(values.toList()))

/**
 * Define a SET column.
 */
fun Table.set(name: String, vararg values: String): Column<Set<String>> =
    registerCustomColumn(name, SetColumnType(values.toList()))

/**
 * Define a YEAR column.
 */
fun Table.year(name: String): Column<Int> = registerCustomColumn(name, YearColumnType())

/**
 * Define a BIT column.
 */
fun Table.bit(name: String, length: Int = 1): Column<ByteArray> = registerCustomColumn(name, BitColumnType(length))
