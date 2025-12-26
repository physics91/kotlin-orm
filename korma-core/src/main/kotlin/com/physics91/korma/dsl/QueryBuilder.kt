package com.physics91.korma.dsl

import com.physics91.korma.expression.*
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * DSL marker for query builders.
 */
@DslMarker
annotation class QueryDsl

/**
 * Base interface for all query builders.
 */
interface QueryBuilder {
    /**
     * Build the prepared SQL statement.
     */
    fun build(dialect: SqlDialect): PreparedSql
}

/**
 * Represents a JOIN clause.
 */
data class JoinClause(
    val type: JoinType,
    val table: Table,
    val condition: Predicate,
    val alias: String? = null
)

/**
 * JOIN types.
 */
enum class JoinType(val sql: String) {
    INNER("INNER JOIN"),
    LEFT("LEFT JOIN"),
    RIGHT("RIGHT JOIN"),
    FULL("FULL OUTER JOIN"),
    CROSS("CROSS JOIN")
}

/**
 * Source for FROM clause - can be a table or subquery.
 */
sealed interface FromSource {
    fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String
}

/**
 * Table as FROM source.
 */
class TableSource(
    val table: Table,
    val alias: String? = null
) : FromSource {
    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val tableName = dialect.quoteIdentifier(table.tableName)
        return if (alias != null) {
            "$tableName AS ${dialect.quoteIdentifier(alias)}"
        } else {
            tableName
        }
    }
}

/**
 * Subquery as FROM source.
 */
class SubquerySource(
    val subquery: SelectBuilder,
    val alias: String
) : FromSource {
    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val subquerySql = subquery.build(dialect)
        params.addAll(subquerySql.params)
        return "(${subquerySql.sql}) AS ${dialect.quoteIdentifier(alias)}"
    }
}

// ============== Column to Expression Conversion ==============

/**
 * Convert a Column to ColumnExpression for use in queries.
 */
fun <T> Column<T>.toExpression(): ColumnExpression<T> = ColumnExpression(this)

/**
 * Alias for column in SELECT.
 */
infix fun <T> Column<T>.alias(name: String): AliasExpression<T> =
    AliasExpression(ColumnExpression(this), name)

// ============== Operator Extensions for Columns ==============

// Comparison operators
infix fun <T> Column<T>.eq(value: T): ComparisonPredicate<T> =
    ColumnExpression(this).eq(value)

infix fun <T> Column<T>.eq(other: Column<T>): ComparisonPredicate<T> =
    ColumnExpression(this).eq(ColumnExpression(other))

infix fun <T> Column<T>.neq(value: T): ComparisonPredicate<T> =
    ColumnExpression(this).neq(value)

infix fun <T> Column<T>.lt(value: T): ComparisonPredicate<T> =
    ColumnExpression(this).lt(value)

infix fun <T> Column<T>.lte(value: T): ComparisonPredicate<T> =
    ColumnExpression(this).lte(value)

infix fun <T> Column<T>.gt(value: T): ComparisonPredicate<T> =
    ColumnExpression(this).gt(value)

infix fun <T> Column<T>.gte(value: T): ComparisonPredicate<T> =
    ColumnExpression(this).gte(value)

// Null checks
fun <T> Column<T>.isNull(): NullPredicate<T> =
    ColumnExpression(this).isNull()

fun <T> Column<T>.isNotNull(): NullPredicate<T> =
    ColumnExpression(this).isNotNull()

// IN operators
infix fun <T> Column<T>.inList(values: Collection<T>): InPredicate<T> =
    ColumnExpression(this).inList(values)

infix fun <T> Column<T>.notInList(values: Collection<T>): InPredicate<T> =
    ColumnExpression(this).notInList(values)

// BETWEEN
fun <T> Column<T>.between(from: T, to: T): BetweenPredicate<T> =
    ColumnExpression(this).between(from, to)

// Ordering
fun <T> Column<T>.asc(): OrderByExpression =
    ColumnExpression(this).asc()

fun <T> Column<T>.desc(): OrderByExpression =
    ColumnExpression(this).desc()

// String operations (for String columns)
infix fun Column<String>.like(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, false, false)

infix fun Column<String>.notLike(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, true, false)

infix fun Column<String>.ilike(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, false, true)

// ============== Arithmetic Operators for Columns ==============

operator fun Column<Int>.plus(value: Int): BinaryExpression<Int> =
    ColumnExpression(this) + value

operator fun Column<Long>.plus(value: Long): BinaryExpression<Long> =
    ColumnExpression(this) + value

operator fun Column<Int>.minus(value: Int): BinaryExpression<Int> =
    ColumnExpression(this) - value

operator fun Column<Long>.minus(value: Long): BinaryExpression<Long> =
    ColumnExpression(this) - value
