package com.physics91.korma.dsl.clauses

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table

/**
 * Mixin interface for query builders that support RETURNING clauses.
 *
 * This interface eliminates code duplication across InsertBuilder, UpdateBuilder, and DeleteBuilder
 * by providing a single source of truth for RETURNING clause handling.
 *
 * Note: RETURNING clause support varies by database dialect. Check dialect.supportsReturning
 * before using these features.
 *
 * @param T The concrete builder type for fluent API chaining
 */
interface ReturningClauseSupport<T : ReturningClauseSupport<T>> {

    /**
     * The columns to return after the operation.
     * Empty list means no RETURNING clause.
     */
    var returningColumns: List<Column<*>>

    /**
     * The table this builder operates on.
     * Used to get all columns for returningAll().
     */
    val table: Table

    /**
     * Returns this builder instance as the concrete type T.
     * Used for fluent API chaining.
     */
    @Suppress("UNCHECKED_CAST")
    fun self(): T = this as T

    /**
     * Add RETURNING clause with specific columns.
     *
     * Example:
     * ```kotlin
     * builder.returning(Users.id, Users.createdAt)
     * ```
     *
     * @param columns The columns to return
     * @return This builder for chaining
     */
    fun returning(vararg columns: Column<*>): T {
        returningColumns = columns.toList()
        return self()
    }

    /**
     * Add RETURNING clause with a list of columns.
     *
     * Example:
     * ```kotlin
     * val cols = listOf(Users.id, Users.name)
     * builder.returning(cols)
     * ```
     *
     * @param columns The list of columns to return
     * @return This builder for chaining
     */
    fun returning(columns: List<Column<*>>): T {
        returningColumns = columns
        return self()
    }

    /**
     * Return all columns from the table.
     *
     * Example:
     * ```kotlin
     * builder.returningAll()
     * ```
     *
     * @return This builder for chaining
     */
    fun returningAll(): T {
        returningColumns = table.columns
        return self()
    }

    /**
     * Check if this builder has any returning columns set.
     */
    fun hasReturning(): Boolean = returningColumns.isNotEmpty()
}
