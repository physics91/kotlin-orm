package com.physics91.korma.expression

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.sql.SqlDialect

/**
 * Base interface for all SQL expressions.
 *
 * Expressions represent any SQL construct that produces a value:
 * - Columns
 * - Literals
 * - Function calls
 * - Arithmetic operations
 * - Subqueries
 *
 * @param T The Kotlin type of the value this expression produces
 */
interface Expression<T> {
    /** The column type for parameter binding and result mapping */
    val columnType: ColumnType<T>?

    /** Generate SQL for this expression */
    fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String

    /** Create an alias for this expression */
    infix fun alias(name: String): AliasExpression<T> = AliasExpression(this, name)
}

/**
 * Expression representing a database column.
 */
open class ColumnExpression<T>(
    val column: Column<T>
) : Expression<T> {

    override val columnType: ColumnType<T> = column.type

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return dialect.quoteIdentifier(column.table.tableName) +
                "." +
                dialect.quoteIdentifier(column.name)
    }

    // ============== Comparison Operators ==============

    infix fun eq(value: T): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.EQ, LiteralExpression(value, columnType))

    infix fun eq(other: Expression<T>): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.EQ, other)

    infix fun neq(value: T): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.NEQ, LiteralExpression(value, columnType))

    infix fun neq(other: Expression<T>): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.NEQ, other)

    infix fun lt(value: T): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.LT, LiteralExpression(value, columnType))

    infix fun lt(other: Expression<T>): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.LT, other)

    infix fun lte(value: T): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.LTE, LiteralExpression(value, columnType))

    infix fun lte(other: Expression<T>): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.LTE, other)

    infix fun gt(value: T): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.GT, LiteralExpression(value, columnType))

    infix fun gt(other: Expression<T>): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.GT, other)

    infix fun gte(value: T): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.GTE, LiteralExpression(value, columnType))

    infix fun gte(other: Expression<T>): ComparisonPredicate<T> =
        ComparisonPredicate(this, ComparisonOp.GTE, other)

    // ============== Null Checks ==============

    fun isNull(): NullPredicate<T> = NullPredicate(this, true)
    fun isNotNull(): NullPredicate<T> = NullPredicate(this, false)

    // ============== IN Operators ==============

    infix fun inList(values: Collection<T>): InPredicate<T> = InPredicate(this, values.toList(), false)
    infix fun notInList(values: Collection<T>): InPredicate<T> = InPredicate(this, values.toList(), true)

    // ============== BETWEEN Operator ==============

    fun between(from: T, to: T): BetweenPredicate<T> = BetweenPredicate(this, from, to, false)
    fun notBetween(from: T, to: T): BetweenPredicate<T> = BetweenPredicate(this, from, to, true)

    // ============== Ordering ==============

    fun asc(): OrderByExpression = OrderByExpression(this, SortDirection.ASC)
    fun desc(): OrderByExpression = OrderByExpression(this, SortDirection.DESC)
    fun ascNullsFirst(): OrderByExpression = OrderByExpression(this, SortDirection.ASC, NullsOrder.FIRST)
    fun ascNullsLast(): OrderByExpression = OrderByExpression(this, SortDirection.ASC, NullsOrder.LAST)
    fun descNullsFirst(): OrderByExpression = OrderByExpression(this, SortDirection.DESC, NullsOrder.FIRST)
    fun descNullsLast(): OrderByExpression = OrderByExpression(this, SortDirection.DESC, NullsOrder.LAST)

    override fun toString(): String = column.qualifiedName
}

/**
 * Expression for string columns with additional string operations.
 */
class StringColumnExpression(
    column: Column<String>
) : ColumnExpression<String>(column) {

    infix fun like(pattern: String): LikePredicate =
        LikePredicate(this, pattern, false, false)

    infix fun notLike(pattern: String): LikePredicate =
        LikePredicate(this, pattern, true, false)

    infix fun ilike(pattern: String): LikePredicate =
        LikePredicate(this, pattern, false, true)

    infix fun notIlike(pattern: String): LikePredicate =
        LikePredicate(this, pattern, true, true)

    fun startsWith(prefix: String): LikePredicate = like("$prefix%")
    fun endsWith(suffix: String): LikePredicate = like("%$suffix")
    fun contains(substring: String): LikePredicate = like("%$substring%")

    // String functions
    fun upper(): FunctionExpression<String> =
        FunctionExpression("UPPER", listOf(this), column.type)

    fun lower(): FunctionExpression<String> =
        FunctionExpression("LOWER", listOf(this), column.type)

    fun trim(): FunctionExpression<String> =
        FunctionExpression("TRIM", listOf(this), column.type)

    fun length(): FunctionExpression<Int> =
        FunctionExpression("LENGTH", listOf(this), null)
}

/**
 * Expression representing a literal value.
 */
class LiteralExpression<T>(
    val value: T,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        if (value == null) {
            return "NULL"
        }
        params.add(columnType?.toDb(value) ?: value)
        return "?"
    }

    override fun toString(): String = value.toString()
}

/**
 * Expression with an alias (AS clause).
 */
class AliasExpression<T>(
    val expression: Expression<T>,
    val alias: String
) : Expression<T> {

    override val columnType: ColumnType<T>? = expression.columnType

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "${expression.toSql(dialect, params)} AS ${dialect.quoteIdentifier(alias)}"
    }

    override fun toString(): String = "$expression AS $alias"
}

/**
 * Binary arithmetic expression.
 */
class BinaryExpression<T>(
    val left: Expression<*>,
    val operator: ArithmeticOp,
    val right: Expression<*>,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "(${left.toSql(dialect, params)} ${operator.sql} ${right.toSql(dialect, params)})"
    }

    override fun toString(): String = "($left ${operator.sql} $right)"
}

/**
 * SQL function call expression.
 */
class FunctionExpression<T>(
    val functionName: String,
    val arguments: List<Expression<*>>,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val args = arguments.joinToString(", ") { it.toSql(dialect, params) }
        return "$functionName($args)"
    }

    override fun toString(): String = "$functionName(${arguments.joinToString(", ")})"
}

/**
 * CASE WHEN expression.
 */
class CaseExpression<T>(
    val conditions: List<Pair<Predicate, Expression<T>>>,
    val elseValue: Expression<T>?,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val whenClauses = conditions.joinToString(" ") { (condition, result) ->
            "WHEN ${condition.toSql(dialect, params)} THEN ${result.toSql(dialect, params)}"
        }
        val elseClause = elseValue?.let { " ELSE ${it.toSql(dialect, params)}" } ?: ""
        return "CASE $whenClauses$elseClause END"
    }
}

/**
 * Coalesce expression - returns first non-null value.
 */
class CoalesceExpression<T>(
    val expressions: List<Expression<T>>,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val args = expressions.joinToString(", ") { it.toSql(dialect, params) }
        return "COALESCE($args)"
    }
}

/**
 * Raw SQL expression for advanced use cases.
 */
class RawExpression<T>(
    val sql: String,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String = sql
}

/**
 * Arithmetic operators.
 */
enum class ArithmeticOp(val sql: String) {
    PLUS("+"),
    MINUS("-"),
    TIMES("*"),
    DIV("/"),
    MOD("%")
}

/**
 * Sort direction for ORDER BY.
 */
enum class SortDirection(val sql: String) {
    ASC("ASC"),
    DESC("DESC")
}

/**
 * Nulls ordering for ORDER BY.
 */
enum class NullsOrder(val sql: String) {
    FIRST("NULLS FIRST"),
    LAST("NULLS LAST")
}

/**
 * ORDER BY expression.
 */
class OrderByExpression(
    val expression: Expression<*>,
    val direction: SortDirection,
    val nullsOrder: NullsOrder? = null
) {
    fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val nullsPart = nullsOrder?.let { " ${it.sql}" } ?: ""
        return "${expression.toSql(dialect, params)} ${direction.sql}$nullsPart"
    }
}

// ============== Arithmetic Operator Extensions ==============

operator fun <T : Number> Expression<T>.plus(other: Expression<T>): BinaryExpression<T> =
    BinaryExpression(this, ArithmeticOp.PLUS, other, this.columnType)

operator fun <T : Number> Expression<T>.minus(other: Expression<T>): BinaryExpression<T> =
    BinaryExpression(this, ArithmeticOp.MINUS, other, this.columnType)

operator fun <T : Number> Expression<T>.times(other: Expression<T>): BinaryExpression<T> =
    BinaryExpression(this, ArithmeticOp.TIMES, other, this.columnType)

operator fun <T : Number> Expression<T>.div(other: Expression<T>): BinaryExpression<T> =
    BinaryExpression(this, ArithmeticOp.DIV, other, this.columnType)

operator fun <T : Number> Expression<T>.rem(other: Expression<T>): BinaryExpression<T> =
    BinaryExpression(this, ArithmeticOp.MOD, other, this.columnType)

// Arithmetic with literal values
operator fun Expression<Int>.plus(value: Int): BinaryExpression<Int> =
    BinaryExpression(this, ArithmeticOp.PLUS, LiteralExpression(value, columnType), columnType)

operator fun Expression<Long>.plus(value: Long): BinaryExpression<Long> =
    BinaryExpression(this, ArithmeticOp.PLUS, LiteralExpression(value, columnType), columnType)

operator fun Expression<Int>.minus(value: Int): BinaryExpression<Int> =
    BinaryExpression(this, ArithmeticOp.MINUS, LiteralExpression(value, columnType), columnType)

operator fun Expression<Long>.minus(value: Long): BinaryExpression<Long> =
    BinaryExpression(this, ArithmeticOp.MINUS, LiteralExpression(value, columnType), columnType)
