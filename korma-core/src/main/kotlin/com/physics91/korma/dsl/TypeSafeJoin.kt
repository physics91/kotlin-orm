package com.physics91.korma.dsl

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.ComparisonPredicate
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table

/**
 * Type-safe join DSL for Korma.
 *
 * Provides compile-time type checking for join conditions, preventing
 * type mismatches like comparing an Int column with a String column.
 *
 * Usage:
 * ```kotlin
 * // Type-safe column comparison - compiler enforces matching types
 * SelectBuilder(Users)
 *     .selectAll()
 *     .typedJoin(Orders).on(Users.id, Orders.userId)  // Both must be same type
 *
 * // Multiple conditions with type-safe builder
 * SelectBuilder(Users)
 *     .selectAll()
 *     .typedJoin(Orders)
 *         .on(Users.id, Orders.userId)
 *         .and(Users.tenantId, Orders.tenantId)
 *
 * // With lambda for complex conditions
 * SelectBuilder(Users)
 *     .selectAll()
 *     .typedJoin(Orders) { join ->
 *         join.on(Users.id, Orders.userId)
 *             .and(Users.tenantId, Orders.tenantId)
 *     }
 * ```
 */

/**
 * DSL marker to prevent nested DSL context leakage.
 */
@DslMarker
annotation class JoinDsl

/**
 * Intermediate builder for type-safe join construction.
 *
 * @param L Type parameter for the left table (from SelectBuilder)
 * @param R Type parameter for the right table (join target)
 */
@JoinDsl
class TypedJoinBuilder<L : Table, R : Table>(
    private val selectBuilder: SelectBuilder,
    private val targetTable: R,
    private val joinType: JoinType,
    private val alias: String? = null
) {
    private var condition: Predicate? = null

    /**
     * Specifies the join condition using two columns of the same type.
     * Compile-time type safety ensures both columns have compatible types.
     *
     * @param T The type of both columns - must match for compilation
     * @param leftColumn Column from the left/source table
     * @param rightColumn Column from the right/target table
     */
    fun <T> on(leftColumn: Column<T>, rightColumn: Column<T>): TypedJoinBuilder<L, R> {
        condition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
        return this
    }

    /**
     * Adds an additional AND condition to the join.
     * Type-safe: both columns must have the same type.
     *
     * @param T The type of both columns
     * @param leftColumn Column from the left table
     * @param rightColumn Column from the right table
     */
    fun <T> and(leftColumn: Column<T>, rightColumn: Column<T>): TypedJoinBuilder<L, R> {
        val newCondition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
        condition = condition?.let { it and newCondition } ?: newCondition
        return this
    }

    /**
     * Adds an additional OR condition to the join.
     * Type-safe: both columns must have the same type.
     *
     * @param T The type of both columns
     * @param leftColumn Column from the left table
     * @param rightColumn Column from the right table
     */
    fun <T> or(leftColumn: Column<T>, rightColumn: Column<T>): TypedJoinBuilder<L, R> {
        val newCondition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
        condition = condition?.let { it or newCondition } ?: newCondition
        return this
    }

    /**
     * Allows using a custom predicate for advanced join conditions.
     * Use when the type-safe operators are not sufficient.
     *
     * Note: This bypasses type safety - use sparingly.
     */
    fun onCondition(predicate: Predicate): TypedJoinBuilder<L, R> {
        condition = predicate
        return this
    }

    /**
     * Adds an additional AND condition using a custom predicate.
     */
    fun andCondition(predicate: Predicate): TypedJoinBuilder<L, R> {
        condition = condition?.let { it and predicate } ?: predicate
        return this
    }

    /**
     * Finishes the join and returns to the SelectBuilder for chaining.
     */
    fun done(): SelectBuilder {
        addJoinToBuilder()
        return selectBuilder
    }

    /**
     * Internal: adds the constructed join to the SelectBuilder.
     */
    internal fun addJoinToBuilder(): SelectBuilder {
        val finalCondition = condition
            ?: throw IllegalStateException("Join condition must be specified using on()")
        return selectBuilder.addTypedJoin(joinType, targetTable, finalCondition, alias)
    }

    /**
     * Helper to allow method chaining back to SelectBuilder after join.
     * Implicitly adds the join when using SelectBuilder methods.
     */
    fun select(vararg columns: Column<*>): SelectBuilder {
        addJoinToBuilder()
        return selectBuilder.select(*columns)
    }

    fun selectAll(): SelectBuilder {
        addJoinToBuilder()
        return selectBuilder.selectAll()
    }

    fun where(condition: Predicate): SelectBuilder {
        addJoinToBuilder()
        return selectBuilder.where(condition)
    }

    fun where(builder: () -> Predicate): SelectBuilder {
        addJoinToBuilder()
        return selectBuilder.where(builder)
    }
}

/**
 * Type-safe equality comparison between two column expressions of the same type.
 */
infix fun <T> ColumnExpression<T>.eq(other: ColumnExpression<T>): ComparisonPredicate<T> =
    this.eq(other as com.physics91.korma.expression.Expression<T>)

// ============== SelectBuilder Extensions for Type-Safe Joins ==============

/**
 * Starts a type-safe INNER JOIN.
 *
 * @param R The type of the target table
 * @param table The table to join
 * @param alias Optional alias for the joined table
 */
fun <R : Table> SelectBuilder.typedJoin(table: R, alias: String? = null): TypedJoinBuilder<Table, R> =
    TypedJoinBuilder(this, table, JoinType.INNER, alias)

/**
 * Starts a type-safe INNER JOIN with a builder block.
 */
fun <R : Table> SelectBuilder.typedJoin(
    table: R,
    alias: String? = null,
    block: (TypedJoinBuilder<Table, R>) -> TypedJoinBuilder<Table, R>
): SelectBuilder {
    val builder = TypedJoinBuilder<Table, R>(this, table, JoinType.INNER, alias)
    block(builder)
    return builder.done()
}

/**
 * Starts a type-safe LEFT JOIN.
 */
fun <R : Table> SelectBuilder.typedLeftJoin(table: R, alias: String? = null): TypedJoinBuilder<Table, R> =
    TypedJoinBuilder(this, table, JoinType.LEFT, alias)

/**
 * Starts a type-safe LEFT JOIN with a builder block.
 */
fun <R : Table> SelectBuilder.typedLeftJoin(
    table: R,
    alias: String? = null,
    block: (TypedJoinBuilder<Table, R>) -> TypedJoinBuilder<Table, R>
): SelectBuilder {
    val builder = TypedJoinBuilder<Table, R>(this, table, JoinType.LEFT, alias)
    block(builder)
    return builder.done()
}

/**
 * Starts a type-safe RIGHT JOIN.
 */
fun <R : Table> SelectBuilder.typedRightJoin(table: R, alias: String? = null): TypedJoinBuilder<Table, R> =
    TypedJoinBuilder(this, table, JoinType.RIGHT, alias)

/**
 * Starts a type-safe RIGHT JOIN with a builder block.
 */
fun <R : Table> SelectBuilder.typedRightJoin(
    table: R,
    alias: String? = null,
    block: (TypedJoinBuilder<Table, R>) -> TypedJoinBuilder<Table, R>
): SelectBuilder {
    val builder = TypedJoinBuilder<Table, R>(this, table, JoinType.RIGHT, alias)
    block(builder)
    return builder.done()
}

/**
 * Starts a type-safe FULL OUTER JOIN.
 */
fun <R : Table> SelectBuilder.typedFullJoin(table: R, alias: String? = null): TypedJoinBuilder<Table, R> =
    TypedJoinBuilder(this, table, JoinType.FULL, alias)

/**
 * Starts a type-safe FULL OUTER JOIN with a builder block.
 */
fun <R : Table> SelectBuilder.typedFullJoin(
    table: R,
    alias: String? = null,
    block: (TypedJoinBuilder<Table, R>) -> TypedJoinBuilder<Table, R>
): SelectBuilder {
    val builder = TypedJoinBuilder<Table, R>(this, table, JoinType.FULL, alias)
    block(builder)
    return builder.done()
}

// ============== Quick Type-Safe Join with Direct Column Comparison ==============

/**
 * Quick type-safe INNER JOIN with column comparison.
 * Most concise syntax for simple equality joins.
 *
 * Usage:
 * ```kotlin
 * Users.query {
 *     selectAll()
 *     joinOn(Orders, Users.id, Orders.userId)
 * }
 * ```
 */
fun <T, R : Table> SelectBuilder.joinOn(
    table: R,
    leftColumn: Column<T>,
    rightColumn: Column<T>,
    alias: String? = null
): SelectBuilder {
    val condition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
    return addTypedJoin(JoinType.INNER, table, condition, alias)
}

/**
 * Quick type-safe LEFT JOIN with column comparison.
 */
fun <T, R : Table> SelectBuilder.leftJoinOn(
    table: R,
    leftColumn: Column<T>,
    rightColumn: Column<T>,
    alias: String? = null
): SelectBuilder {
    val condition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
    return addTypedJoin(JoinType.LEFT, table, condition, alias)
}

/**
 * Quick type-safe RIGHT JOIN with column comparison.
 */
fun <T, R : Table> SelectBuilder.rightJoinOn(
    table: R,
    leftColumn: Column<T>,
    rightColumn: Column<T>,
    alias: String? = null
): SelectBuilder {
    val condition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
    return addTypedJoin(JoinType.RIGHT, table, condition, alias)
}

/**
 * Quick type-safe FULL OUTER JOIN with column comparison.
 */
fun <T, R : Table> SelectBuilder.fullJoinOn(
    table: R,
    leftColumn: Column<T>,
    rightColumn: Column<T>,
    alias: String? = null
): SelectBuilder {
    val condition = ColumnExpression(leftColumn) eq ColumnExpression(rightColumn)
    return addTypedJoin(JoinType.FULL, table, condition, alias)
}
