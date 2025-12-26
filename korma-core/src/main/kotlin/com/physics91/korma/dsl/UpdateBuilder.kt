package com.physics91.korma.dsl

import com.physics91.korma.dsl.clauses.ReturningClauseSupport
import com.physics91.korma.dsl.clauses.WhereClauseSupport
import com.physics91.korma.expression.Expression
import com.physics91.korma.expression.LiteralExpression
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Builder for UPDATE queries.
 *
 * Supports:
 * - SET column = value
 * - SET column = expression
 * - WHERE conditions
 * - RETURNING clause
 *
 * Example:
 * ```kotlin
 * val update = UpdateBuilder(Users)
 *     .set(Users.name, "Jane")
 *     .set(Users.age, Users.age + 1)
 *     .where { Users.id eq 1 }
 *     .returning(Users.id, Users.name)
 *     .build(dialect)
 * ```
 */
@QueryDsl
class UpdateBuilder(
    override val table: Table
) : QueryBuilder, WhereClauseSupport<UpdateBuilder>, ReturningClauseSupport<UpdateBuilder> {

    // Required override due to multiple interface inheritance
    override fun self(): UpdateBuilder = this

    // ============== State ==============

    private val updates = mutableMapOf<Column<*>, UpdateValue>()
    override var whereClause: Predicate? = null
    override var returningColumns: List<Column<*>> = emptyList()

    // ============== Setting Values ==============

    /**
     * Set a column to a literal value.
     */
    fun <T> set(column: Column<T>, value: T): UpdateBuilder {
        updates[column] = UpdateValue.Literal(value)
        return this
    }

    /**
     * Set a column to an expression (e.g., column + 1).
     */
    fun <T> set(column: Column<T>, expression: Expression<T>): UpdateBuilder {
        updates[column] = UpdateValue.Expr(expression)
        return this
    }

    /**
     * Set a column to NULL.
     */
    fun <T> setNull(column: Column<T?>): UpdateBuilder {
        updates[column] = UpdateValue.Null
        return this
    }

    /**
     * Increment a numeric column.
     */
    fun increment(column: Column<Int>, by: Int = 1): UpdateBuilder {
        return set(column, column + by)
    }

    /**
     * Increment a long column.
     */
    fun increment(column: Column<Long>, by: Long = 1): UpdateBuilder {
        return set(column, column + by)
    }

    /**
     * Decrement a numeric column.
     */
    fun decrement(column: Column<Int>, by: Int = 1): UpdateBuilder {
        return set(column, column - by)
    }

    /**
     * Operator syntax for setting values.
     */
    operator fun <T> set(column: Column<T>, value: T?) {
        if (value == null) {
            updates[column] = UpdateValue.Null
        } else {
            updates[column] = UpdateValue.Literal(value)
        }
    }

    /**
     * Set multiple values using a lambda.
     */
    inline fun values(block: UpdateBuilder.() -> Unit): UpdateBuilder {
        this.block()
        return this
    }

    // ============== WHERE Clause ==============
    // Inherited from WhereClauseSupport:
    // - where(condition), where(builder)
    // - andWhere(condition), orWhere(condition)
    // - whereIfNotNull(value, predicateFactory)

    // ============== RETURNING ==============
    // Inherited from ReturningClauseSupport:
    // - returning(vararg columns), returning(columns: List)
    // - returningAll()

    // ============== Build ==============

    override fun build(dialect: SqlDialect): PreparedSql {
        if (updates.isEmpty()) {
            throw IllegalStateException("No updates specified")
        }

        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        // UPDATE table
        sql.append("UPDATE ")
        sql.append(dialect.quoteIdentifier(table.tableName))

        // SET column = value, ...
        sql.append(" SET ")
        sql.append(updates.entries.joinToString(", ") { (column, updateValue) ->
            val columnName = dialect.quoteIdentifier(column.name)
            val valueSql = when (updateValue) {
                is UpdateValue.Literal<*> -> {
                    @Suppress("UNCHECKED_CAST")
                    val columnType = column.type as ColumnType<Any?>
                    val dbValue = if (updateValue.value == null) null else columnType.toDb(updateValue.value)
                    params.add(dbValue)
                    "?"
                }
                is UpdateValue.Expr<*> -> {
                    updateValue.expression.toSql(dialect, params)
                }
                UpdateValue.Null -> "NULL"
            }
            "$columnName = $valueSql"
        })

        // WHERE
        if (whereClause != null) {
            sql.append(" WHERE ")
            sql.append(whereClause!!.toSql(dialect, params))
        }

        // RETURNING
        if (returningColumns.isNotEmpty() && dialect.supportsReturning) {
            sql.append(" ")
            sql.append(dialect.returningClause(returningColumns))
        }

        return PreparedSql(sql.toString(), params)
    }
}

/**
 * Represents an update value (literal, expression, or null).
 */
sealed class UpdateValue {
    data class Literal<T>(val value: T) : UpdateValue()
    data class Expr<T>(val expression: Expression<T>) : UpdateValue()
    data object Null : UpdateValue()
}

// ============== Convenience Functions ==============

/**
 * Start an UPDATE query.
 */
fun update(table: Table): UpdateBuilder = UpdateBuilder(table)

/**
 * Update with DSL syntax.
 */
inline fun update(table: Table, block: UpdateBuilder.() -> Unit): UpdateBuilder =
    UpdateBuilder(table).apply(block)
