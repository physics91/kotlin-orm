package com.physics91.korma.dsl

import com.physics91.korma.dsl.clauses.ReturningClauseSupport
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.Expression
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Builder for INSERT queries.
 *
 * Supports:
 * - Single row insert
 * - Batch insert (multiple rows)
 * - ON CONFLICT (upsert)
 * - RETURNING clause
 *
 * Example:
 * ```kotlin
 * val insert = InsertBuilder(Users)
 *     .set(Users.name, "John")
 *     .set(Users.email, "john@example.com")
 *     .set(Users.age, 25)
 *     .returning(Users.id)
 *     .build(dialect)
 * ```
 */
@QueryDsl
class InsertBuilder(
    override val table: Table
) : QueryBuilder, ReturningClauseSupport<InsertBuilder> {

    // ============== State ==============

    private val values = mutableMapOf<Column<*>, Any?>()
    private var onConflictColumns: List<Column<*>>? = null
    private var onConflictAction: OnConflictAction = OnConflictAction.NONE
    private var updateColumns: List<Column<*>>? = null
    override var returningColumns: List<Column<*>> = emptyList()

    // ============== Setting Values ==============

    /**
     * Set a column value.
     */
    fun <T> set(column: Column<T>, value: T): InsertBuilder {
        values[column] = value
        return this
    }

    /**
     * Set a column value from an expression.
     */
    fun <T> set(column: Column<T>, expression: Expression<T>): InsertBuilder {
        values[column] = expression
        return this
    }

    /**
     * Set multiple column values using a lambda.
     */
    inline fun values(block: InsertBuilder.() -> Unit): InsertBuilder {
        this.block()
        return this
    }

    /**
     * Operator syntax for setting values.
     * Usage: it[column] = value
     */
    operator fun <T> set(column: Column<T>, value: T?) {
        if (value != null) {
            values[column] = value
        } else if (column.type.nullable) {
            values[column] = null
        }
    }

    // ============== ON CONFLICT (Upsert) ==============

    /**
     * Handle conflict on specified columns.
     */
    fun onConflict(vararg columns: Column<*>): OnConflictBuilder {
        onConflictColumns = columns.toList()
        return OnConflictBuilder(this)
    }

    /**
     * Builder for ON CONFLICT clause.
     */
    @QueryDsl
    inner class OnConflictBuilder(
        private val parent: InsertBuilder
    ) {
        /**
         * Do nothing on conflict.
         */
        fun doNothing(): InsertBuilder {
            parent.onConflictAction = OnConflictAction.DO_NOTHING
            return parent
        }

        /**
         * Update specified columns on conflict.
         */
        fun doUpdate(vararg columns: Column<*>): InsertBuilder {
            parent.onConflictAction = OnConflictAction.DO_UPDATE
            parent.updateColumns = if (columns.isEmpty()) {
                // Update all non-conflict columns
                parent.values.keys.filter { it !in (parent.onConflictColumns ?: emptyList()) }
            } else {
                columns.toList()
            }
            return parent
        }

        /**
         * Update all columns on conflict.
         */
        fun doUpdateAll(): InsertBuilder {
            parent.onConflictAction = OnConflictAction.DO_UPDATE
            parent.updateColumns = parent.values.keys.filter {
                it !in (parent.onConflictColumns ?: emptyList())
            }
            return parent
        }
    }

    // ============== RETURNING ==============
    // Inherited from ReturningClauseSupport:
    // - returning(vararg columns), returning(columns: List)
    // - returningAll()

    // ============== Build ==============

    override fun build(dialect: SqlDialect): PreparedSql {
        if (values.isEmpty()) {
            throw IllegalStateException("No values specified for INSERT")
        }

        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        // INSERT INTO table (columns)
        sql.append("INSERT INTO ")
        sql.append(dialect.quoteIdentifier(table.tableName))
        sql.append(" (")
        sql.append(values.keys.joinToString(", ") { dialect.quoteIdentifier(it.name) })
        sql.append(")")

        // VALUES (?, ?, ...)
        sql.append(" VALUES (")
        sql.append(values.entries.joinToString(", ") { (column, value) ->
            when (value) {
                is Expression<*> -> value.toSql(dialect, params)
                else -> {
                    @Suppress("UNCHECKED_CAST")
                    val columnType = column.type as ColumnType<Any?>
                    val dbValue = if (value == null) null else columnType.toDb(value)
                    params.add(dbValue)
                    "?"
                }
            }
        })
        sql.append(")")

        // ON CONFLICT
        when (onConflictAction) {
            OnConflictAction.DO_NOTHING -> {
                val conflictColumns = onConflictColumns ?: throw IllegalStateException("Conflict columns required")
                sql.append(" ")
                sql.append(dialect.onConflictDoNothing(conflictColumns))
            }
            OnConflictAction.DO_UPDATE -> {
                val conflictColumns = onConflictColumns ?: throw IllegalStateException("Conflict columns required")
                val updateCols = updateColumns ?: throw IllegalStateException("Update columns required")
                sql.append(" ")
                sql.append(dialect.onConflictClause(conflictColumns, updateCols))
            }
            OnConflictAction.NONE -> { /* No conflict handling */ }
        }

        // RETURNING
        if (returningColumns.isNotEmpty() && dialect.supportsReturning) {
            sql.append(" ")
            sql.append(dialect.returningClause(returningColumns))
        }

        return PreparedSql(sql.toString(), params)
    }

    /**
     * Get the columns and values for batch insert.
     */
    internal fun getValues(): Map<Column<*>, Any?> = values.toMap()
}

/**
 * ON CONFLICT action types.
 */
enum class OnConflictAction {
    NONE,
    DO_NOTHING,
    DO_UPDATE
}

/**
 * Builder for batch INSERT.
 */
@QueryDsl
class BatchInsertBuilder(
    private val table: Table
) {
    private val rows = mutableListOf<Map<Column<*>, Any?>>()

    /**
     * Add a row.
     */
    fun addRow(block: InsertBuilder.() -> Unit): BatchInsertBuilder {
        val builder = InsertBuilder(table).apply(block)
        rows.add(builder.getValues())
        return this
    }

    /**
     * Add multiple rows from a collection.
     */
    fun <T> addRows(items: Collection<T>, mapper: InsertBuilder.(T) -> Unit): BatchInsertBuilder {
        items.forEach { item ->
            val builder = InsertBuilder(table).apply { mapper(item) }
            rows.add(builder.getValues())
        }
        return this
    }

    /**
     * Build batch INSERT.
     * Returns multiple PreparedSql if dialect doesn't support multi-row insert.
     */
    fun build(dialect: SqlDialect): List<PreparedSql> {
        if (rows.isEmpty()) {
            return emptyList()
        }

        val columns = rows.flatMap { it.keys }.distinct()

        return if (dialect.supportsMultiRowInsert) {
            listOf(buildMultiRowInsert(dialect, columns))
        } else {
            rows.map { buildSingleInsert(dialect, columns, it) }
        }
    }

    private fun buildMultiRowInsert(dialect: SqlDialect, columns: List<Column<*>>): PreparedSql {
        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        // INSERT INTO table (columns)
        sql.append("INSERT INTO ")
        sql.append(dialect.quoteIdentifier(table.tableName))
        sql.append(" (")
        sql.append(columns.joinToString(", ") { dialect.quoteIdentifier(it.name) })
        sql.append(") VALUES ")

        // Multiple rows
        val rowSqls = rows.map { row ->
            val rowParams = columns.map { column ->
                val value = row[column]
                if (value != null) {
                    @Suppress("UNCHECKED_CAST")
                    val columnType = column.type as ColumnType<Any?>
                    columnType.toDb(value)
                } else {
                    null
                }
            }
            params.addAll(rowParams)
            "(${columns.map { "?" }.joinToString(", ")})"
        }
        sql.append(rowSqls.joinToString(", "))

        return PreparedSql(sql.toString(), params)
    }

    private fun buildSingleInsert(
        dialect: SqlDialect,
        columns: List<Column<*>>,
        row: Map<Column<*>, Any?>
    ): PreparedSql {
        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        sql.append("INSERT INTO ")
        sql.append(dialect.quoteIdentifier(table.tableName))
        sql.append(" (")
        sql.append(columns.joinToString(", ") { dialect.quoteIdentifier(it.name) })
        sql.append(") VALUES (")

        sql.append(columns.joinToString(", ") { column ->
            val value = row[column]
            if (value != null) {
                @Suppress("UNCHECKED_CAST")
                val columnType = column.type as ColumnType<Any?>
                params.add(columnType.toDb(value))
            } else {
                params.add(null)
            }
            "?"
        })
        sql.append(")")

        return PreparedSql(sql.toString(), params)
    }
}

// ============== Convenience Functions ==============

/**
 * Start an INSERT query.
 */
fun insertInto(table: Table): InsertBuilder = InsertBuilder(table)

/**
 * Start a batch INSERT.
 */
fun batchInsertInto(table: Table): BatchInsertBuilder = BatchInsertBuilder(table)

/**
 * Insert with DSL syntax.
 */
inline fun insertInto(table: Table, block: InsertBuilder.() -> Unit): InsertBuilder =
    InsertBuilder(table).apply(block)

/**
 * Batch insert with items.
 */
inline fun <T> batchInsertInto(
    table: Table,
    items: Collection<T>,
    crossinline mapper: InsertBuilder.(T) -> Unit
): BatchInsertBuilder =
    BatchInsertBuilder(table).addRows(items) { mapper(it) }
