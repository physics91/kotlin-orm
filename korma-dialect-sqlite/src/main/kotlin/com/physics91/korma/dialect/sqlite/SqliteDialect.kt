package com.physics91.korma.dialect.sqlite

import com.physics91.korma.schema.*
import com.physics91.korma.sql.BaseSqlDialect

/**
 * SQLite database dialect implementation.
 *
 * SQLite is a self-contained, serverless, zero-configuration, transactional
 * SQL database engine. It's the most widely deployed database in the world.
 *
 * Features:
 * - Zero configuration
 * - Single file database
 * - Full ACID compliance
 * - JSON1 extension support
 * - Full-text search (FTS5)
 * - R-Tree indexes
 *
 * Limitations:
 * - Limited ALTER TABLE support
 * - No native BOOLEAN type (uses INTEGER 0/1)
 * - No native DATE/TIME types (uses TEXT, INTEGER, or REAL)
 * - RIGHT and FULL OUTER JOIN not supported
 * - Limited concurrent write access
 *
 * @see <a href="https://www.sqlite.org/">SQLite</a>
 */
object SqliteDialect : BaseSqlDialect() {

    override val name: String = "SQLite"

    // ============== Feature Support ==============

    override val supportsReturning: Boolean = true  // SQLite 3.35.0+
    override val supportsOnConflict: Boolean = true  // ON CONFLICT clause
    override val supportsILike: Boolean = false  // Use LIKE with COLLATE NOCASE
    override val supportsCTE: Boolean = true
    override val supportsWindowFunctions: Boolean = true  // SQLite 3.25.0+
    override val supportsLimitOffset: Boolean = true
    override val supportsBooleanType: Boolean = false  // Uses INTEGER 0/1

    /**
     * SQLite uses UPSERT syntax (ON CONFLICT DO UPDATE).
     */
    val supportsUpsert: Boolean = true  // SQLite 3.24.0+

    // ============== Identifier Quoting ==============

    override val identifierQuoteChar: Char = '"'

    // ============== Type Mapping ==============

    /**
     * SQLite uses dynamic typing with 5 storage classes:
     * NULL, INTEGER, REAL, TEXT, BLOB
     *
     * Type affinity is determined by declared type name.
     */
    override fun sqlTypeName(type: ColumnType<*>): String = when (type) {
        is IntColumnType -> "INTEGER"
        is LongColumnType -> "INTEGER"
        is ShortColumnType -> "INTEGER"
        is FloatColumnType -> "REAL"
        is DoubleColumnType -> "REAL"
        is BooleanColumnType -> "INTEGER"  // 0 = false, 1 = true
        is VarcharColumnType -> "TEXT"
        is CharColumnType -> "TEXT"
        is TextColumnType -> "TEXT"
        is DecimalColumnType -> "NUMERIC"  // SQLite NUMERIC affinity
        is TimestampColumnType -> "TEXT"  // ISO8601 format
        is DateColumnType -> "TEXT"  // YYYY-MM-DD format
        is TimeColumnType -> "TEXT"  // HH:MM:SS format
        is DateTimeColumnType -> "TEXT"  // ISO8601 format
        is BinaryColumnType -> "BLOB"
        is BlobColumnType -> "BLOB"
        is UUIDColumnType -> "TEXT"  // Store as string
        is NullableColumnType<*> -> sqlTypeName(type.delegate)
        else -> type.sqlType()
    }

    override fun autoIncrementType(baseType: ColumnType<*>): String = "INTEGER"

    // ============== DDL Customization ==============

    override fun columnDefinition(column: Column<*>): String {
        val parts = mutableListOf<String>()

        parts.add(quoteIdentifier(column.name))

        // Type
        if (column.isAutoIncrement && column.isPrimaryKey) {
            // SQLite ROWID alias - must be INTEGER PRIMARY KEY
            parts.add("INTEGER PRIMARY KEY AUTOINCREMENT")
        } else {
            parts.add(sqlTypeName(column.type))

            // PRIMARY KEY (non-autoincrement)
            if (column.isPrimaryKey && !column.isAutoIncrement) {
                parts.add("PRIMARY KEY")
            }

            // NOT NULL
            if (column.isNotNull && !column.nullable && !column.isPrimaryKey) {
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
        }

        return parts.joinToString(" ")
    }

    override fun createTableStatement(table: Table, ifNotExists: Boolean): String {
        val columns = table.columns.joinToString(",\n    ") { columnDefinition(it) }

        // Handle composite primary keys
        val primaryKeys = table.columns.filter { it.isPrimaryKey && !it.isAutoIncrement }
        val compositePkCount = primaryKeys.size
        val hasAutoIncrement = table.columns.any { it.isAutoIncrement && it.isPrimaryKey }

        val pkClause = if (compositePkCount > 1 && !hasAutoIncrement) {
            ",\n    PRIMARY KEY (${primaryKeys.joinToString(", ") { quoteIdentifier(it.name) }})"
        } else ""

        val ifNotExistsClause = if (ifNotExists) "IF NOT EXISTS " else ""
        return """
            |CREATE TABLE $ifNotExistsClause${quoteIdentifier(table.tableName)} (
            |    $columns$pkClause
            |)
        """.trimMargin()
    }

    // ============== SQLite-Specific Operations ==============

    /**
     * Generate UPSERT statement (ON CONFLICT DO UPDATE).
     * Requires SQLite 3.24.0+
     */
    fun upsert(
        table: Table,
        conflictColumns: List<Column<*>>,
        updateColumns: List<Column<*>>
    ): String {
        val conflictClause = conflictColumns.joinToString(", ") { quoteIdentifier(it.name) }
        val updateClause = if (updateColumns.isNotEmpty()) {
            val updates = updateColumns.joinToString(", ") { col ->
                "${quoteIdentifier(col.name)} = excluded.${quoteIdentifier(col.name)}"
            }
            "DO UPDATE SET $updates"
        } else {
            "DO NOTHING"
        }
        return "ON CONFLICT($conflictClause) $updateClause"
    }

    /**
     * Generate INSERT OR REPLACE statement.
     */
    fun insertOrReplace(table: Table): String = "INSERT OR REPLACE INTO ${quoteIdentifier(table.tableName)}"

    /**
     * Generate INSERT OR IGNORE statement.
     */
    fun insertOrIgnore(table: Table): String = "INSERT OR IGNORE INTO ${quoteIdentifier(table.tableName)}"

    /**
     * Generate CREATE TABLE IF NOT EXISTS.
     */
    fun createTableIfNotExists(table: Table): String = createTableStatement(table, ifNotExists = true)

    /**
     * Drop table if exists.
     */
    fun dropTableIfExists(table: Table): String = "DROP TABLE IF EXISTS ${quoteIdentifier(table.tableName)}"

    // ============== Utility ==============

    override fun currentTimestampExpression(): String = "datetime('now')"

    override fun currentDateExpression(): String = "date('now')"

    /**
     * Current time expression.
     */
    fun currentTimeExpression(): String = "time('now')"

    /**
     * Unix timestamp expression.
     */
    fun unixTimestamp(): String = "strftime('%s', 'now')"

    /**
     * Generate EXPLAIN QUERY PLAN for query analysis.
     */
    fun explainQueryPlan(sql: String): String = "EXPLAIN QUERY PLAN $sql"

    /**
     * Generate VACUUM command to rebuild database.
     */
    fun vacuum(): String = "VACUUM"

    /**
     * Generate ANALYZE command to update statistics.
     */
    fun analyze(table: Table? = null): String =
        if (table != null) "ANALYZE ${quoteIdentifier(table.tableName)}"
        else "ANALYZE"

    /**
     * Generate PRAGMA statement.
     */
    fun pragma(name: String, value: Any? = null): String =
        if (value != null) "PRAGMA $name = $value"
        else "PRAGMA $name"

    /**
     * Common pragmas for optimization.
     */
    object Pragmas {
        val journalModeWal = "PRAGMA journal_mode = WAL"
        val synchronousNormal = "PRAGMA synchronous = NORMAL"
        val cacheSize = { pages: Int -> "PRAGMA cache_size = $pages" }
        val mmapSize = { bytes: Long -> "PRAGMA mmap_size = $bytes" }
        val foreignKeysOn = "PRAGMA foreign_keys = ON"
        val foreignKeysOff = "PRAGMA foreign_keys = OFF"
        val busyTimeout = { ms: Int -> "PRAGMA busy_timeout = $ms" }
    }

    /**
     * SQLite connection URL format.
     * @param path Path to database file, or ":memory:" for in-memory database
     */
    fun connectionUrl(path: String): String = "jdbc:sqlite:$path"

    /**
     * In-memory database URL.
     */
    fun inMemoryUrl(): String = "jdbc:sqlite::memory:"

    /**
     * Shared in-memory database URL (accessible from multiple connections).
     */
    fun sharedMemoryUrl(name: String): String = "jdbc:sqlite:file:$name?mode=memory&cache=shared"
}

// ============== SQLite-Specific Features ==============

/**
 * SQLite date/time storage modes.
 */
enum class DateTimeMode {
    /** Store as TEXT in ISO8601 format (default) */
    TEXT,
    /** Store as INTEGER Unix timestamp */
    INTEGER,
    /** Store as REAL Julian day number */
    REAL
}

/**
 * SQLite conflict resolution strategies.
 */
enum class ConflictStrategy {
    /** Abort the current SQL statement with an error */
    ABORT,
    /** Fail the entire transaction */
    FAIL,
    /** Skip the conflicting row */
    IGNORE,
    /** Replace the existing row */
    REPLACE,
    /** Roll back the entire transaction */
    ROLLBACK
}

// ============== Table Extensions for SQLite ==============

/**
 * Define an INTEGER PRIMARY KEY AUTOINCREMENT column.
 * This creates a ROWID alias in SQLite.
 */
fun Table.rowId(name: String = "id"): Column<Long> =
    registerCustomColumn(name, LongColumnType).apply {
        primaryKey()
        autoIncrement()
    }
