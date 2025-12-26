package com.physics91.korma.expression

import com.physics91.korma.dsl.asc
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.LongColumnType
import com.physics91.korma.sql.SqlDialect

/**
 * Window function expression.
 *
 * Usage:
 * ```kotlin
 * select(
 *     Users.name,
 *     rowNumber().over {
 *         partitionBy(Users.department)
 *         orderBy(Users.salary.desc())
 *     }
 * )
 * ```
 */
class WindowFunctionExpression<T>(
    val functionName: String,
    val arguments: List<Expression<*>>,
    override val columnType: ColumnType<T>?,
    val windowSpec: WindowSpecification? = null
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val args = if (arguments.isEmpty()) "" else arguments.joinToString(", ") { it.toSql(dialect, params) }
        val windowPart = windowSpec?.toSql(dialect, params)?.let { " OVER ($it)" } ?: ""
        return "$functionName($args)$windowPart"
    }

    /**
     * Add OVER clause with window specification.
     */
    fun over(spec: WindowSpecification): WindowFunctionExpression<T> =
        WindowFunctionExpression(functionName, arguments, columnType, spec)

    /**
     * Add OVER clause with DSL builder.
     */
    fun over(builder: WindowSpecificationBuilder.() -> Unit): WindowFunctionExpression<T> {
        val spec = WindowSpecificationBuilder().apply(builder).build()
        return WindowFunctionExpression(functionName, arguments, columnType, spec)
    }

    /**
     * Add simple OVER () clause (entire result set as partition).
     */
    fun over(): WindowFunctionExpression<T> =
        WindowFunctionExpression(functionName, arguments, columnType, WindowSpecification())

    override fun toString(): String = "$functionName(${arguments.joinToString(", ")})"
}

/**
 * Window specification (PARTITION BY, ORDER BY, frame clause).
 */
data class WindowSpecification(
    val partitionBy: List<Expression<*>> = emptyList(),
    val orderBy: List<OrderByExpression> = emptyList(),
    val frameClause: FrameClause? = null
) {
    fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val parts = mutableListOf<String>()

        if (partitionBy.isNotEmpty()) {
            parts.add("PARTITION BY ${partitionBy.joinToString(", ") { it.toSql(dialect, params) }}")
        }

        if (orderBy.isNotEmpty()) {
            parts.add("ORDER BY ${orderBy.joinToString(", ") { it.toSql(dialect, params) }}")
        }

        frameClause?.let {
            parts.add(it.toSql())
        }

        return parts.joinToString(" ")
    }
}

/**
 * Builder for window specifications.
 */
class WindowSpecificationBuilder {
    private val partitionByColumns = mutableListOf<Expression<*>>()
    private val orderByExpressions = mutableListOf<OrderByExpression>()
    private var frameClause: FrameClause? = null

    /** Add PARTITION BY columns. */
    fun partitionBy(vararg columns: Column<*>) {
        partitionByColumns.addAll(columns.map { ColumnExpression(it) })
    }

    /** Add PARTITION BY expressions. */
    fun partitionBy(vararg expressions: Expression<*>) {
        partitionByColumns.addAll(expressions)
    }

    /** Add ORDER BY expressions. */
    fun orderBy(vararg orders: OrderByExpression) {
        orderByExpressions.addAll(orders)
    }

    /** Add ORDER BY column ascending. */
    fun orderBy(column: Column<*>) {
        orderByExpressions.add(column.asc())
    }

    /** Set ROWS frame. */
    fun rows(start: FrameBound, end: FrameBound? = null) {
        frameClause = FrameClause(FrameType.ROWS, start, end)
    }

    /** Set RANGE frame. */
    fun range(start: FrameBound, end: FrameBound? = null) {
        frameClause = FrameClause(FrameType.RANGE, start, end)
    }

    /** Set GROUPS frame. */
    fun groups(start: FrameBound, end: FrameBound? = null) {
        frameClause = FrameClause(FrameType.GROUPS, start, end)
    }

    fun build(): WindowSpecification = WindowSpecification(
        partitionBy = partitionByColumns.toList(),
        orderBy = orderByExpressions.toList(),
        frameClause = frameClause
    )
}

/**
 * Frame clause for window functions.
 */
data class FrameClause(
    val type: FrameType,
    val start: FrameBound,
    val end: FrameBound? = null
) {
    fun toSql(): String {
        val endPart = end?.let { " AND ${it.toSql()}" } ?: ""
        val betweenPart = if (end != null) "BETWEEN " else ""
        return "${type.sql} $betweenPart${start.toSql()}$endPart"
    }
}

/**
 * Frame type.
 */
enum class FrameType(val sql: String) {
    ROWS("ROWS"),
    RANGE("RANGE"),
    GROUPS("GROUPS")
}

/**
 * Frame bound for window frame specification.
 */
sealed class FrameBound {
    abstract fun toSql(): String

    /** UNBOUNDED PRECEDING */
    object UnboundedPreceding : FrameBound() {
        override fun toSql(): String = "UNBOUNDED PRECEDING"
    }

    /** UNBOUNDED FOLLOWING */
    object UnboundedFollowing : FrameBound() {
        override fun toSql(): String = "UNBOUNDED FOLLOWING"
    }

    /** CURRENT ROW */
    object CurrentRow : FrameBound() {
        override fun toSql(): String = "CURRENT ROW"
    }

    /** N PRECEDING */
    data class Preceding(val offset: Int) : FrameBound() {
        override fun toSql(): String = "$offset PRECEDING"
    }

    /** N FOLLOWING */
    data class Following(val offset: Int) : FrameBound() {
        override fun toSql(): String = "$offset FOLLOWING"
    }
}

// Frame bound convenience functions
val unboundedPreceding: FrameBound = FrameBound.UnboundedPreceding
val unboundedFollowing: FrameBound = FrameBound.UnboundedFollowing
val currentRow: FrameBound = FrameBound.CurrentRow
fun preceding(offset: Int): FrameBound = FrameBound.Preceding(offset)
fun following(offset: Int): FrameBound = FrameBound.Following(offset)

// ============== Window Functions ==============

/**
 * ROW_NUMBER() - Assigns a unique sequential integer to each row within the partition.
 */
fun rowNumber(): WindowFunctionExpression<Long> =
    WindowFunctionExpression("ROW_NUMBER", emptyList(), LongColumnType)

/**
 * RANK() - Assigns rank with gaps for ties.
 */
fun rank(): WindowFunctionExpression<Long> =
    WindowFunctionExpression("RANK", emptyList(), LongColumnType)

/**
 * DENSE_RANK() - Assigns rank without gaps for ties.
 */
fun denseRank(): WindowFunctionExpression<Long> =
    WindowFunctionExpression("DENSE_RANK", emptyList(), LongColumnType)

/**
 * NTILE(n) - Divides the partition into n groups.
 */
fun ntile(n: Int): WindowFunctionExpression<Long> =
    WindowFunctionExpression("NTILE", listOf(LiteralExpression(n, null)), LongColumnType)

/**
 * PERCENT_RANK() - Relative rank (0 to 1).
 */
fun percentRank(): WindowFunctionExpression<Double> =
    WindowFunctionExpression("PERCENT_RANK", emptyList(), null)

/**
 * CUME_DIST() - Cumulative distribution (0 to 1).
 */
fun cumeDist(): WindowFunctionExpression<Double> =
    WindowFunctionExpression("CUME_DIST", emptyList(), null)

/**
 * LAG(column, offset, default) - Access value from previous row.
 */
fun <T> lag(column: Column<T>, offset: Int = 1, default: T? = null): WindowFunctionExpression<T?> {
    val args = mutableListOf<Expression<*>>(ColumnExpression(column), LiteralExpression(offset, null))
    if (default != null) {
        args.add(LiteralExpression(default, column.type))
    }
    return WindowFunctionExpression("LAG", args, null)
}

/**
 * LAG(expression, offset, default) - Access value from previous row.
 */
fun <T> lag(expression: Expression<T>, offset: Int = 1, default: T? = null): WindowFunctionExpression<T?> {
    val args = mutableListOf<Expression<*>>(expression, LiteralExpression(offset, null))
    if (default != null) {
        args.add(LiteralExpression(default, expression.columnType))
    }
    return WindowFunctionExpression("LAG", args, null)
}

/**
 * LEAD(column, offset, default) - Access value from following row.
 */
fun <T> lead(column: Column<T>, offset: Int = 1, default: T? = null): WindowFunctionExpression<T?> {
    val args = mutableListOf<Expression<*>>(ColumnExpression(column), LiteralExpression(offset, null))
    if (default != null) {
        args.add(LiteralExpression(default, column.type))
    }
    return WindowFunctionExpression("LEAD", args, null)
}

/**
 * LEAD(expression, offset, default) - Access value from following row.
 */
fun <T> lead(expression: Expression<T>, offset: Int = 1, default: T? = null): WindowFunctionExpression<T?> {
    val args = mutableListOf<Expression<*>>(expression, LiteralExpression(offset, null))
    if (default != null) {
        args.add(LiteralExpression(default, expression.columnType))
    }
    return WindowFunctionExpression("LEAD", args, null)
}

/**
 * FIRST_VALUE(column) - First value in the window frame.
 */
fun <T> firstValue(column: Column<T>): WindowFunctionExpression<T> =
    WindowFunctionExpression("FIRST_VALUE", listOf(ColumnExpression(column)), column.type)

/**
 * LAST_VALUE(column) - Last value in the window frame.
 */
fun <T> lastValue(column: Column<T>): WindowFunctionExpression<T> =
    WindowFunctionExpression("LAST_VALUE", listOf(ColumnExpression(column)), column.type)

/**
 * NTH_VALUE(column, n) - Nth value in the window frame.
 */
fun <T> nthValue(column: Column<T>, n: Int): WindowFunctionExpression<T> =
    WindowFunctionExpression("NTH_VALUE", listOf(ColumnExpression(column), LiteralExpression(n, null)), column.type)

// ============== Aggregate Functions as Window Functions ==============

/**
 * SUM() as window function.
 */
fun <T : Number> sumOver(column: Column<T>): WindowFunctionExpression<T> =
    WindowFunctionExpression("SUM", listOf(ColumnExpression(column)), column.type)

/**
 * AVG() as window function.
 */
fun <T : Number> avgOver(column: Column<T>): WindowFunctionExpression<Double> =
    WindowFunctionExpression("AVG", listOf(ColumnExpression(column)), null)

/**
 * COUNT() as window function.
 */
fun countOver(): WindowFunctionExpression<Long> =
    WindowFunctionExpression("COUNT", listOf(AllColumnsExpression), LongColumnType)

/**
 * COUNT(column) as window function.
 */
fun <T> countOver(column: Column<T>): WindowFunctionExpression<Long> =
    WindowFunctionExpression("COUNT", listOf(ColumnExpression(column)), LongColumnType)

/**
 * MIN() as window function.
 */
fun <T : Comparable<T>> minOver(column: Column<T>): WindowFunctionExpression<T> =
    WindowFunctionExpression("MIN", listOf(ColumnExpression(column)), column.type)

/**
 * MAX() as window function.
 */
fun <T : Comparable<T>> maxOver(column: Column<T>): WindowFunctionExpression<T> =
    WindowFunctionExpression("MAX", listOf(ColumnExpression(column)), column.type)
