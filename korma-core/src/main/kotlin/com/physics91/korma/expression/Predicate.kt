package com.physics91.korma.expression

import com.physics91.korma.schema.ColumnType
import com.physics91.korma.sql.SqlDialect

/**
 * Base interface for SQL predicates (boolean conditions).
 *
 * Predicates are used in WHERE, HAVING, ON (JOIN) clauses.
 */
sealed interface Predicate : Expression<Boolean> {
    override val columnType: ColumnType<Boolean>? get() = null

    /** Combine with AND */
    infix fun and(other: Predicate): Predicate = AndPredicate(listOf(this, other))

    /** Combine with OR */
    infix fun or(other: Predicate): Predicate = OrPredicate(listOf(this, other))

    /** Negate this predicate */
    operator fun not(): Predicate = NotPredicate(this)
}

/**
 * Comparison operators for predicates.
 */
enum class ComparisonOp(val sql: String) {
    EQ("="),
    NEQ("<>"),
    LT("<"),
    LTE("<="),
    GT(">"),
    GTE(">=")
}

/**
 * Comparison predicate (e.g., column = value).
 */
class ComparisonPredicate<T>(
    val left: Expression<T>,
    val operator: ComparisonOp,
    val right: Expression<T>
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "${left.toSql(dialect, params)} ${operator.sql} ${right.toSql(dialect, params)}"
    }

    override fun toString(): String = "$left ${operator.sql} $right"
}

/**
 * IS NULL / IS NOT NULL predicate.
 */
class NullPredicate<T>(
    val expression: Expression<T>,
    val isNull: Boolean
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val nullCheck = if (isNull) "IS NULL" else "IS NOT NULL"
        return "${expression.toSql(dialect, params)} $nullCheck"
    }

    override fun toString(): String = "$expression ${if (isNull) "IS NULL" else "IS NOT NULL"}"
}

/**
 * IN predicate (e.g., column IN (1, 2, 3)).
 */
class InPredicate<T>(
    val expression: Expression<T>,
    val values: List<T>,
    val negated: Boolean = false
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        if (values.isEmpty()) {
            // Empty IN clause is always false, NOT IN is always true
            return if (negated) "1 = 1" else "1 = 0"
        }

        val expressionSql = expression.toSql(dialect, params)
        val placeholders = values.joinToString(", ") {
            val columnType = expression.columnType
            params.add(columnType?.toDb(it) ?: it)
            "?"
        }

        val operator = if (negated) "NOT IN" else "IN"
        return "$expressionSql $operator ($placeholders)"
    }

    override fun toString(): String {
        val operator = if (negated) "NOT IN" else "IN"
        return "$expression $operator (${values.joinToString(", ")})"
    }
}

/**
 * BETWEEN predicate.
 */
class BetweenPredicate<T>(
    val expression: Expression<T>,
    val from: T,
    val to: T,
    val negated: Boolean = false
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val expressionSql = expression.toSql(dialect, params)
        val columnType = expression.columnType
        params.add(columnType?.toDb(from) ?: from)
        params.add(columnType?.toDb(to) ?: to)

        val operator = if (negated) "NOT BETWEEN" else "BETWEEN"
        return "$expressionSql $operator ? AND ?"
    }

    override fun toString(): String {
        val operator = if (negated) "NOT BETWEEN" else "BETWEEN"
        return "$expression $operator $from AND $to"
    }
}

/**
 * LIKE predicate for pattern matching.
 */
class LikePredicate(
    val expression: Expression<String>,
    val pattern: String,
    val negated: Boolean = false,
    val caseInsensitive: Boolean = false
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val expressionSql = expression.toSql(dialect, params)

        val operator = when {
            negated && caseInsensitive -> "NOT ILIKE"
            negated -> "NOT LIKE"
            caseInsensitive -> "ILIKE"
            else -> "LIKE"
        }

        // If dialect doesn't support ILIKE, use LOWER()
        return if (caseInsensitive && !dialect.supportsILike) {
            val notPart = if (negated) "NOT " else ""
            params.add(pattern)
            "LOWER($expressionSql) ${notPart}LIKE LOWER(?)"
        } else {
            params.add(pattern)
            "$expressionSql $operator ?"
        }
    }

    override fun toString(): String {
        val operator = when {
            negated && caseInsensitive -> "NOT ILIKE"
            negated -> "NOT LIKE"
            caseInsensitive -> "ILIKE"
            else -> "LIKE"
        }
        return "$expression $operator '$pattern'"
    }
}

/**
 * AND predicate combining multiple conditions.
 */
class AndPredicate(
    val conditions: List<Predicate>
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        if (conditions.isEmpty()) return "1 = 1"
        if (conditions.size == 1) return conditions.first().toSql(dialect, params)

        return conditions.joinToString(" AND ") { condition ->
            "(${condition.toSql(dialect, params)})"
        }
    }

    override infix fun and(other: Predicate): Predicate =
        AndPredicate(conditions + other)

    override fun toString(): String = conditions.joinToString(" AND ") { "($it)" }
}

/**
 * OR predicate combining multiple conditions.
 */
class OrPredicate(
    val conditions: List<Predicate>
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        if (conditions.isEmpty()) return "1 = 0"
        if (conditions.size == 1) return conditions.first().toSql(dialect, params)

        return conditions.joinToString(" OR ") { condition ->
            "(${condition.toSql(dialect, params)})"
        }
    }

    override infix fun or(other: Predicate): Predicate =
        OrPredicate(conditions + other)

    override fun toString(): String = conditions.joinToString(" OR ") { "($it)" }
}

/**
 * NOT predicate.
 */
class NotPredicate(
    val predicate: Predicate
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "NOT (${predicate.toSql(dialect, params)})"
    }

    override operator fun not(): Predicate = predicate

    override fun toString(): String = "NOT ($predicate)"
}

/**
 * EXISTS predicate for subquery.
 */
class ExistsPredicate(
    val subquerySql: String,
    val subqueryParams: List<Any?>,
    val negated: Boolean = false
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        params.addAll(subqueryParams)
        val operator = if (negated) "NOT EXISTS" else "EXISTS"
        return "$operator ($subquerySql)"
    }
}

/**
 * Raw SQL predicate for advanced use cases.
 */
class RawPredicate(
    val sql: String,
    val rawParams: List<Any?> = emptyList()
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        params.addAll(rawParams)
        return sql
    }

    override fun toString(): String = sql
}

/**
 * Always true predicate.
 */
object TruePredicate : Predicate {
    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String = "1 = 1"
    override fun toString(): String = "TRUE"
}

/**
 * Always false predicate.
 */
object FalsePredicate : Predicate {
    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String = "1 = 0"
    override fun toString(): String = "FALSE"
}

// ============== Predicate Builder DSL ==============

/**
 * DSL marker for predicate building.
 */
@DslMarker
annotation class PredicateDsl

/**
 * Predicate builder for DSL-style WHERE clauses.
 */
@PredicateDsl
class PredicateBuilder {
    private val conditions = mutableListOf<Predicate>()

    /** Add a condition */
    fun condition(predicate: Predicate) {
        conditions.add(predicate)
    }

    /** Add a condition if the value is not null */
    fun <T> conditionIfNotNull(value: T?, predicateFactory: (T) -> Predicate) {
        if (value != null) {
            conditions.add(predicateFactory(value))
        }
    }

    /** Build the combined predicate */
    fun build(): Predicate = when {
        conditions.isEmpty() -> TruePredicate
        conditions.size == 1 -> conditions.first()
        else -> AndPredicate(conditions)
    }
}

/**
 * Build a predicate using DSL.
 */
inline fun buildPredicate(block: PredicateBuilder.() -> Unit): Predicate {
    return PredicateBuilder().apply(block).build()
}

// ============== Extension Functions for Columns ==============

/**
 * Create a string column expression with additional string operations.
 */
fun com.physics91.korma.schema.Column<String>.asStringExpression(): StringColumnExpression =
    StringColumnExpression(this)

/**
 * LIKE operator extension for Column<String>.
 */
infix fun com.physics91.korma.schema.Column<String>.like(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, false, false)

/**
 * ILIKE operator extension for Column<String>.
 */
infix fun com.physics91.korma.schema.Column<String>.ilike(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, false, true)
