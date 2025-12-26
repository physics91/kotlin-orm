package com.physics91.korma.expression

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.IntColumnType
import com.physics91.korma.schema.LongColumnType

/**
 * SQL aggregate and scalar functions.
 */
object SqlFunctions {

    // ============== Aggregate Functions ==============

    /** COUNT(*) - count all rows */
    fun count(): FunctionExpression<Long> =
        FunctionExpression("COUNT", listOf(AllColumnsExpression), LongColumnType)

    /** COUNT(column) - count non-null values */
    fun <T> count(column: Column<T>): FunctionExpression<Long> =
        FunctionExpression("COUNT", listOf(ColumnExpression(column)), LongColumnType)

    /** COUNT(expression) */
    fun <T> count(expression: Expression<T>): FunctionExpression<Long> =
        FunctionExpression("COUNT", listOf(expression), LongColumnType)

    /** COUNT(DISTINCT column) - count distinct non-null values */
    fun <T> countDistinct(column: Column<T>): FunctionExpression<Long> =
        FunctionExpression("COUNT", listOf(DistinctExpression(ColumnExpression(column))), LongColumnType)

    /** SUM(column) */
    fun <T : Number> sum(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("SUM", listOf(ColumnExpression(column)), column.type)

    /** SUM(expression) */
    fun <T : Number> sum(expression: Expression<T>): FunctionExpression<T> =
        FunctionExpression("SUM", listOf(expression), expression.columnType)

    /** AVG(column) - returns Double */
    fun <T : Number> avg(column: Column<T>): FunctionExpression<Double> =
        FunctionExpression("AVG", listOf(ColumnExpression(column)), null)

    /** MIN(column) */
    fun <T : Comparable<T>> min(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("MIN", listOf(ColumnExpression(column)), column.type)

    /** MAX(column) */
    fun <T : Comparable<T>> max(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("MAX", listOf(ColumnExpression(column)), column.type)

    // ============== String Functions ==============

    /** UPPER(column) */
    fun upper(column: Column<String>): FunctionExpression<String> =
        FunctionExpression("UPPER", listOf(ColumnExpression(column)), column.type)

    /** LOWER(column) */
    fun lower(column: Column<String>): FunctionExpression<String> =
        FunctionExpression("LOWER", listOf(ColumnExpression(column)), column.type)

    /** TRIM(column) */
    fun trim(column: Column<String>): FunctionExpression<String> =
        FunctionExpression("TRIM", listOf(ColumnExpression(column)), column.type)

    /** LTRIM(column) */
    fun ltrim(column: Column<String>): FunctionExpression<String> =
        FunctionExpression("LTRIM", listOf(ColumnExpression(column)), column.type)

    /** RTRIM(column) */
    fun rtrim(column: Column<String>): FunctionExpression<String> =
        FunctionExpression("RTRIM", listOf(ColumnExpression(column)), column.type)

    /** LENGTH(column) */
    fun length(column: Column<String>): FunctionExpression<Int> =
        FunctionExpression("LENGTH", listOf(ColumnExpression(column)), IntColumnType)

    /** CONCAT(expressions...) */
    fun concat(vararg expressions: Expression<String>): FunctionExpression<String> =
        FunctionExpression("CONCAT", expressions.toList(), null)

    /** SUBSTRING(column, start, length) */
    fun substring(column: Column<String>, start: Int, length: Int): FunctionExpression<String> =
        FunctionExpression(
            "SUBSTRING",
            listOf(
                ColumnExpression(column),
                LiteralExpression(start, IntColumnType),
                LiteralExpression(length, IntColumnType)
            ),
            column.type
        )

    /** REPLACE(column, from, to) */
    fun replace(column: Column<String>, from: String, to: String): FunctionExpression<String> =
        FunctionExpression(
            "REPLACE",
            listOf(
                ColumnExpression(column),
                LiteralExpression(from, null),
                LiteralExpression(to, null)
            ),
            column.type
        )

    // ============== Numeric Functions ==============

    /** ABS(column) */
    fun <T : Number> abs(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("ABS", listOf(ColumnExpression(column)), column.type)

    /** ROUND(column) */
    fun <T : Number> round(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("ROUND", listOf(ColumnExpression(column)), column.type)

    /** ROUND(column, decimals) */
    fun <T : Number> round(column: Column<T>, decimals: Int): FunctionExpression<T> =
        FunctionExpression(
            "ROUND",
            listOf(ColumnExpression(column), LiteralExpression(decimals, IntColumnType)),
            column.type
        )

    /** FLOOR(column) */
    fun <T : Number> floor(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("FLOOR", listOf(ColumnExpression(column)), column.type)

    /** CEIL(column) */
    fun <T : Number> ceil(column: Column<T>): FunctionExpression<T> =
        FunctionExpression("CEIL", listOf(ColumnExpression(column)), column.type)

    // ============== Date/Time Functions ==============

    /** CURRENT_TIMESTAMP */
    fun currentTimestamp(): RawExpression<kotlinx.datetime.Instant> =
        RawExpression("CURRENT_TIMESTAMP", null)

    /** CURRENT_DATE */
    fun currentDate(): RawExpression<kotlinx.datetime.LocalDate> =
        RawExpression("CURRENT_DATE", null)

    /** CURRENT_TIME */
    fun currentTime(): RawExpression<kotlinx.datetime.LocalTime> =
        RawExpression("CURRENT_TIME", null)

    // ============== Null Handling Functions ==============

    /** COALESCE(expressions...) - returns first non-null */
    fun <T> coalesce(vararg expressions: Expression<T>): CoalesceExpression<T> =
        CoalesceExpression(expressions.toList(), expressions.firstOrNull()?.columnType)

    /** COALESCE(column, default) */
    fun <T> coalesce(column: Column<T>, default: T): CoalesceExpression<T> =
        CoalesceExpression(
            listOf(ColumnExpression(column), LiteralExpression(default, column.type)),
            column.type
        )

    /** NULLIF(expr1, expr2) - returns null if equal */
    fun <T> nullIf(expr1: Expression<T>, expr2: Expression<T>): FunctionExpression<T?> =
        FunctionExpression("NULLIF", listOf(expr1, expr2), null)
}

/**
 * Expression for SELECT * or COUNT(*)
 */
object AllColumnsExpression : Expression<Any> {
    override val columnType = null

    override fun toSql(dialect: com.physics91.korma.sql.SqlDialect, params: MutableList<Any?>): String = "*"

    override fun toString(): String = "*"
}

/**
 * DISTINCT expression wrapper.
 */
class DistinctExpression<T>(
    val expression: Expression<T>
) : Expression<T> {
    override val columnType = expression.columnType

    override fun toSql(dialect: com.physics91.korma.sql.SqlDialect, params: MutableList<Any?>): String {
        return "DISTINCT ${expression.toSql(dialect, params)}"
    }

    override fun toString(): String = "DISTINCT $expression"
}

// ============== Convenience Extensions ==============

/** COUNT(*) shorthand */
fun count() = SqlFunctions.count()

/** COUNT(column) shorthand */
fun <T> count(column: Column<T>) = SqlFunctions.count(column)

/** COUNT(DISTINCT column) shorthand */
fun <T> countDistinct(column: Column<T>) = SqlFunctions.countDistinct(column)

/** SUM(column) shorthand */
fun <T : Number> sum(column: Column<T>) = SqlFunctions.sum(column)

/** AVG(column) shorthand */
fun <T : Number> avg(column: Column<T>) = SqlFunctions.avg(column)

/** MIN(column) shorthand */
fun <T : Comparable<T>> min(column: Column<T>) = SqlFunctions.min(column)

/** MAX(column) shorthand */
fun <T : Comparable<T>> max(column: Column<T>) = SqlFunctions.max(column)

/** COALESCE shorthand */
fun <T> coalesce(column: Column<T>, default: T) = SqlFunctions.coalesce(column, default)

/** CURRENT_TIMESTAMP shorthand */
fun now() = SqlFunctions.currentTimestamp()
