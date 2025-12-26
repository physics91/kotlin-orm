package com.physics91.korma.expression

import com.physics91.korma.dsl.SelectBuilder
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.sql.SqlDialect

/**
 * Scalar subquery expression that returns a single value.
 *
 * Usage:
 * ```kotlin
 * select(Users.name, scalar(from(Orders).select(count()).where { Orders.userId eq Users.id }))
 * ```
 */
class ScalarSubqueryExpression<T>(
    val subquery: SelectBuilder,
    override val columnType: ColumnType<T>?
) : Expression<T> {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val subSql = subquery.build(dialect)
        params.addAll(subSql.params)
        return "(${subSql.sql})"
    }

    override fun toString(): String = "(subquery)"
}

/**
 * IN predicate with subquery.
 *
 * Usage:
 * ```kotlin
 * where { Users.id inSubquery from(ActiveUsers).select(ActiveUsers.userId) }
 * ```
 */
class InSubqueryPredicate<T>(
    val expression: Expression<T>,
    val subquery: SelectBuilder,
    val negated: Boolean = false
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val subSql = subquery.build(dialect)
        params.addAll(subSql.params)
        val operator = if (negated) "NOT IN" else "IN"
        return "${expression.toSql(dialect, params)} $operator (${subSql.sql})"
    }

    override fun toString(): String {
        val operator = if (negated) "NOT IN" else "IN"
        return "$expression $operator (subquery)"
    }
}

/**
 * EXISTS predicate with SelectBuilder subquery.
 *
 * Usage:
 * ```kotlin
 * where { exists(from(Orders).where { Orders.userId eq Users.id }) }
 * ```
 */
class ExistsSubqueryPredicate(
    val subquery: SelectBuilder,
    val negated: Boolean = false
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val subSql = subquery.build(dialect)
        params.addAll(subSql.params)
        val operator = if (negated) "NOT EXISTS" else "EXISTS"
        return "$operator (${subSql.sql})"
    }

    override fun toString(): String {
        val operator = if (negated) "NOT EXISTS" else "EXISTS"
        return "$operator (subquery)"
    }
}

/**
 * Comparison predicate with subquery (e.g., column = (SELECT ...))
 */
class SubqueryComparisonPredicate<T>(
    val expression: Expression<T>,
    val operator: ComparisonOp,
    val subquery: SelectBuilder
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val subSql = subquery.build(dialect)
        params.addAll(subSql.params)
        return "${expression.toSql(dialect, params)} ${operator.sql} (${subSql.sql})"
    }

    override fun toString(): String = "$expression ${operator.sql} (subquery)"
}

/**
 * ANY/ALL predicate with subquery.
 *
 * Usage:
 * ```kotlin
 * where { Users.age gt any(from(Thresholds).select(Thresholds.minAge)) }
 * ```
 */
class QuantifiedSubqueryPredicate<T>(
    val expression: Expression<T>,
    val operator: ComparisonOp,
    val quantifier: Quantifier,
    val subquery: SelectBuilder
) : Predicate {

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        val subSql = subquery.build(dialect)
        params.addAll(subSql.params)
        return "${expression.toSql(dialect, params)} ${operator.sql} ${quantifier.sql} (${subSql.sql})"
    }

    override fun toString(): String = "$expression ${operator.sql} ${quantifier.sql} (subquery)"
}

/**
 * Quantifiers for subquery predicates.
 */
enum class Quantifier(val sql: String) {
    ANY("ANY"),
    ALL("ALL"),
    SOME("SOME") // Synonym for ANY
}

// ============== Extension Functions ==============

/**
 * Create a scalar subquery expression.
 */
fun <T> scalar(subquery: SelectBuilder, columnType: ColumnType<T>? = null): ScalarSubqueryExpression<T> =
    ScalarSubqueryExpression(subquery, columnType)

/**
 * IN subquery predicate extension.
 */
infix fun <T> Expression<T>.inSubquery(subquery: SelectBuilder): InSubqueryPredicate<T> =
    InSubqueryPredicate(this, subquery, false)

/**
 * NOT IN subquery predicate extension.
 */
infix fun <T> Expression<T>.notInSubquery(subquery: SelectBuilder): InSubqueryPredicate<T> =
    InSubqueryPredicate(this, subquery, true)

/**
 * IN subquery predicate for columns.
 */
infix fun <T> Column<T>.inSubquery(subquery: SelectBuilder): InSubqueryPredicate<T> =
    InSubqueryPredicate(ColumnExpression(this), subquery, false)

/**
 * NOT IN subquery predicate for columns.
 */
infix fun <T> Column<T>.notInSubquery(subquery: SelectBuilder): InSubqueryPredicate<T> =
    InSubqueryPredicate(ColumnExpression(this), subquery, true)

/**
 * EXISTS predicate function.
 */
fun exists(subquery: SelectBuilder): ExistsSubqueryPredicate =
    ExistsSubqueryPredicate(subquery, false)

/**
 * NOT EXISTS predicate function.
 */
fun notExists(subquery: SelectBuilder): ExistsSubqueryPredicate =
    ExistsSubqueryPredicate(subquery, true)

/**
 * Comparison with subquery extensions.
 */
infix fun <T> Expression<T>.eqSubquery(subquery: SelectBuilder): SubqueryComparisonPredicate<T> =
    SubqueryComparisonPredicate(this, ComparisonOp.EQ, subquery)

infix fun <T> Expression<T>.neqSubquery(subquery: SelectBuilder): SubqueryComparisonPredicate<T> =
    SubqueryComparisonPredicate(this, ComparisonOp.NEQ, subquery)

infix fun <T> Expression<T>.ltSubquery(subquery: SelectBuilder): SubqueryComparisonPredicate<T> =
    SubqueryComparisonPredicate(this, ComparisonOp.LT, subquery)

infix fun <T> Expression<T>.lteSubquery(subquery: SelectBuilder): SubqueryComparisonPredicate<T> =
    SubqueryComparisonPredicate(this, ComparisonOp.LTE, subquery)

infix fun <T> Expression<T>.gtSubquery(subquery: SelectBuilder): SubqueryComparisonPredicate<T> =
    SubqueryComparisonPredicate(this, ComparisonOp.GT, subquery)

infix fun <T> Expression<T>.gteSubquery(subquery: SelectBuilder): SubqueryComparisonPredicate<T> =
    SubqueryComparisonPredicate(this, ComparisonOp.GTE, subquery)

/**
 * ANY/ALL/SOME wrapper classes for subquery quantifiers.
 */
class AnySubquery(val subquery: SelectBuilder)
class AllSubquery(val subquery: SelectBuilder)

fun any(subquery: SelectBuilder) = AnySubquery(subquery)
fun all(subquery: SelectBuilder) = AllSubquery(subquery)

/**
 * Comparison with ANY subquery.
 */
infix fun <T> Expression<T>.gt(any: AnySubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.GT, Quantifier.ANY, any.subquery)

infix fun <T> Expression<T>.gte(any: AnySubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.GTE, Quantifier.ANY, any.subquery)

infix fun <T> Expression<T>.lt(any: AnySubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.LT, Quantifier.ANY, any.subquery)

infix fun <T> Expression<T>.lte(any: AnySubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.LTE, Quantifier.ANY, any.subquery)

infix fun <T> Expression<T>.eq(any: AnySubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.EQ, Quantifier.ANY, any.subquery)

/**
 * Comparison with ALL subquery.
 */
infix fun <T> Expression<T>.gt(all: AllSubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.GT, Quantifier.ALL, all.subquery)

infix fun <T> Expression<T>.gte(all: AllSubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.GTE, Quantifier.ALL, all.subquery)

infix fun <T> Expression<T>.lt(all: AllSubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.LT, Quantifier.ALL, all.subquery)

infix fun <T> Expression<T>.lte(all: AllSubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.LTE, Quantifier.ALL, all.subquery)

infix fun <T> Expression<T>.eq(all: AllSubquery): QuantifiedSubqueryPredicate<T> =
    QuantifiedSubqueryPredicate(this, ComparisonOp.EQ, Quantifier.ALL, all.subquery)
