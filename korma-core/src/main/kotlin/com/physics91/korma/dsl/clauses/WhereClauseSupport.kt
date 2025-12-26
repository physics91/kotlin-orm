package com.physics91.korma.dsl.clauses

import com.physics91.korma.expression.Predicate

/**
 * Mixin interface for query builders that support WHERE clauses.
 *
 * This interface eliminates code duplication across SelectBuilder, UpdateBuilder, and DeleteBuilder
 * by providing a single source of truth for WHERE clause handling.
 *
 * Follows Interface Segregation Principle (ISP) - builders only implement what they need.
 *
 * @param T The concrete builder type for fluent API chaining
 */
interface WhereClauseSupport<T : WhereClauseSupport<T>> {

    /**
     * The current WHERE clause predicate.
     * May be null if no WHERE condition has been set.
     */
    var whereClause: Predicate?

    /**
     * Returns this builder instance as the concrete type T.
     * Used for fluent API chaining.
     */
    @Suppress("UNCHECKED_CAST")
    fun self(): T = this as T

    /**
     * Set the WHERE condition.
     *
     * Example:
     * ```kotlin
     * builder.where(Users.age gt 18)
     * ```
     */
    fun where(condition: Predicate): T {
        whereClause = condition
        return self()
    }

    /**
     * Set the WHERE condition with a builder lambda.
     *
     * Example:
     * ```kotlin
     * builder.where { (Users.age gt 18) and (Users.active eq true) }
     * ```
     */
    fun where(builder: () -> Predicate): T {
        whereClause = builder()
        return self()
    }

    /**
     * Add an AND condition to the existing WHERE clause.
     * If no WHERE clause exists, this becomes the initial condition.
     *
     * Example:
     * ```kotlin
     * builder
     *     .where(Users.age gt 18)
     *     .andWhere(Users.active eq true)
     * ```
     */
    fun andWhere(condition: Predicate): T {
        whereClause = whereClause?.and(condition) ?: condition
        return self()
    }

    /**
     * Add an OR condition to the existing WHERE clause.
     * If no WHERE clause exists, this becomes the initial condition.
     *
     * Example:
     * ```kotlin
     * builder
     *     .where(Users.status eq "admin")
     *     .orWhere(Users.status eq "moderator")
     * ```
     */
    fun orWhere(condition: Predicate): T {
        whereClause = whereClause?.or(condition) ?: condition
        return self()
    }

    /**
     * Add a condition only if the value is not null.
     * Useful for optional filters.
     *
     * Example:
     * ```kotlin
     * builder.whereIfNotNull(searchTerm) { term -> Users.name like "%$term%" }
     * ```
     */
    fun <V> whereIfNotNull(value: V?, predicateFactory: (V) -> Predicate): T {
        if (value != null) {
            andWhere(predicateFactory(value))
        }
        return self()
    }

    /**
     * Add an AND condition with a builder lambda.
     *
     * Example:
     * ```kotlin
     * builder.andWhere { Users.email like "%@example.com" }
     * ```
     */
    fun andWhere(builder: () -> Predicate): T {
        return andWhere(builder())
    }

    /**
     * Add an OR condition with a builder lambda.
     *
     * Example:
     * ```kotlin
     * builder.orWhere { Users.status eq "pending" }
     * ```
     */
    fun orWhere(builder: () -> Predicate): T {
        return orWhere(builder())
    }
}
