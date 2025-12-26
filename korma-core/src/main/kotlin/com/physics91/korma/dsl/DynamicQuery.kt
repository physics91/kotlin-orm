package com.physics91.korma.dsl

import com.physics91.korma.dsl.clauses.WhereClauseSupport
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column

/**
 * Dynamic Query Builder for Korma.
 *
 * Provides an ergonomic DSL for building queries with optional/conditional filters.
 * Conditions are only added when their corresponding values are non-null.
 *
 * Usage:
 * ```kotlin
 * // Basic dynamic WHERE
 * Users.selectAll()
 *     .dynamicWhere {
 *         name?.let { +Users.name.contains(it) }
 *         age?.let { +Users.age.gte(it) }
 *         status?.let { +Users.status.eq(it) }
 *     }
 *
 * // With explicit AND/OR grouping
 * Users.selectAll()
 *     .dynamicWhere {
 *         +Users.active.eq(true)
 *         searchTerm?.let { term ->
 *             or {
 *                 +Users.name.contains(term)
 *                 +Users.email.contains(term)
 *             }
 *         }
 *     }
 *
 * // Filter object pattern
 * data class UserFilter(
 *     val name: String? = null,
 *     val minAge: Int? = null,
 *     val active: Boolean? = null
 * )
 *
 * fun findUsers(filter: UserFilter) =
 *     Users.selectAll()
 *         .applyFilter(filter) { f ->
 *             f.name?.let { +Users.name.contains(it) }
 *             f.minAge?.let { +Users.age.gte(it) }
 *             f.active?.let { +Users.active.eq(it) }
 *         }
 * ```
 */

/**
 * DSL marker for dynamic query scope.
 */
@DslMarker
annotation class DynamicQueryDsl

/**
 * Scope for building dynamic WHERE conditions.
 *
 * Conditions added with the unary plus operator (+) are combined with AND by default.
 * Use `or { }` blocks to group conditions with OR logic.
 */
@DynamicQueryDsl
class DynamicWhereScope {
    private val conditions = mutableListOf<Predicate>()
    private var combineWithOr = false

    /**
     * Adds a condition to the dynamic WHERE clause.
     * Conditions are combined with AND by default.
     */
    operator fun Predicate.unaryPlus() {
        conditions.add(this)
    }

    /**
     * Adds a condition only if the value is not null.
     * More explicit alternative to let { + }.
     *
     * Usage:
     * ```kotlin
     * ifNotNull(searchTerm) { Users.name.contains(it) }
     * ```
     */
    fun <T> ifNotNull(value: T?, predicateFactory: (T) -> Predicate) {
        if (value != null) {
            conditions.add(predicateFactory(value))
        }
    }

    /**
     * Adds a condition only if the boolean flag is true.
     *
     * Usage:
     * ```kotlin
     * ifTrue(includeInactive) { Users.active.eq(false) }
     * ```
     */
    fun ifTrue(condition: Boolean, predicateFactory: () -> Predicate) {
        if (condition) {
            conditions.add(predicateFactory())
        }
    }

    /**
     * Adds a condition only if the boolean flag is false.
     */
    fun ifFalse(condition: Boolean, predicateFactory: () -> Predicate) {
        if (!condition) {
            conditions.add(predicateFactory())
        }
    }

    /**
     * Adds a condition only if the collection is not empty.
     *
     * Usage:
     * ```kotlin
     * ifNotEmpty(statusList) { Users.status.inList(it) }
     * ```
     */
    fun <T> ifNotEmpty(collection: Collection<T>?, predicateFactory: (Collection<T>) -> Predicate) {
        if (!collection.isNullOrEmpty()) {
            conditions.add(predicateFactory(collection))
        }
    }

    /**
     * Adds a condition only if the string is not blank.
     *
     * Usage:
     * ```kotlin
     * ifNotBlank(searchTerm) { Users.name.contains(it) }
     * ```
     */
    fun ifNotBlank(value: String?, predicateFactory: (String) -> Predicate) {
        if (!value.isNullOrBlank()) {
            conditions.add(predicateFactory(value))
        }
    }

    /**
     * Groups conditions with OR logic.
     * All conditions inside the or block are combined with OR.
     *
     * Usage:
     * ```kotlin
     * or {
     *     +Users.status.eq("admin")
     *     +Users.status.eq("moderator")
     * }
     * ```
     */
    fun or(block: DynamicOrScope.() -> Unit) {
        val orScope = DynamicOrScope()
        orScope.block()
        orScope.build()?.let { conditions.add(it) }
    }

    /**
     * Groups conditions with AND logic explicitly.
     * Useful for grouping within an OR context.
     *
     * Usage:
     * ```kotlin
     * or {
     *     and {
     *         +Users.role.eq("user")
     *         +Users.verified.eq(true)
     *     }
     *     +Users.role.eq("admin")
     * }
     * ```
     */
    fun and(block: DynamicAndScope.() -> Unit) {
        val andScope = DynamicAndScope()
        andScope.block()
        andScope.build()?.let { conditions.add(it) }
    }

    /**
     * Builds the combined predicate from all added conditions.
     * Returns null if no conditions were added.
     */
    internal fun build(): Predicate? {
        if (conditions.isEmpty()) return null
        return conditions.reduce { acc, predicate -> acc and predicate }
    }
}

/**
 * Scope for building OR-combined conditions.
 */
@DynamicQueryDsl
class DynamicOrScope {
    private val conditions = mutableListOf<Predicate>()

    /**
     * Adds a condition to be combined with OR.
     */
    operator fun Predicate.unaryPlus() {
        conditions.add(this)
    }

    /**
     * Adds a condition only if the value is not null.
     */
    fun <T> ifNotNull(value: T?, predicateFactory: (T) -> Predicate) {
        if (value != null) {
            conditions.add(predicateFactory(value))
        }
    }

    /**
     * Groups conditions with AND logic within OR.
     */
    fun and(block: DynamicAndScope.() -> Unit) {
        val andScope = DynamicAndScope()
        andScope.block()
        andScope.build()?.let { conditions.add(it) }
    }

    /**
     * Builds the combined predicate with OR logic.
     */
    internal fun build(): Predicate? {
        if (conditions.isEmpty()) return null
        return conditions.reduce { acc, predicate -> acc or predicate }
    }
}

/**
 * Scope for building AND-combined conditions (explicit grouping).
 */
@DynamicQueryDsl
class DynamicAndScope {
    private val conditions = mutableListOf<Predicate>()

    /**
     * Adds a condition to be combined with AND.
     */
    operator fun Predicate.unaryPlus() {
        conditions.add(this)
    }

    /**
     * Adds a condition only if the value is not null.
     */
    fun <T> ifNotNull(value: T?, predicateFactory: (T) -> Predicate) {
        if (value != null) {
            conditions.add(predicateFactory(value))
        }
    }

    /**
     * Builds the combined predicate with AND logic.
     */
    internal fun build(): Predicate? {
        if (conditions.isEmpty()) return null
        return conditions.reduce { acc, predicate -> acc and predicate }
    }
}

// ============== SelectBuilder Extensions ==============

/**
 * Adds dynamic WHERE conditions to the query.
 *
 * Conditions added within the block are combined with AND by default.
 * Only non-null conditions are included.
 *
 * Usage:
 * ```kotlin
 * Users.selectAll()
 *     .dynamicWhere {
 *         searchName?.let { +Users.name.contains(it) }
 *         minAge?.let { +Users.age.gte(it) }
 *     }
 * ```
 */
fun SelectBuilder.dynamicWhere(block: DynamicWhereScope.() -> Unit): SelectBuilder {
    val scope = DynamicWhereScope()
    scope.block()
    scope.build()?.let { where(it) }
    return this
}

/**
 * Adds dynamic AND conditions to an existing WHERE clause.
 */
fun SelectBuilder.dynamicAndWhere(block: DynamicWhereScope.() -> Unit): SelectBuilder {
    val scope = DynamicWhereScope()
    scope.block()
    scope.build()?.let { andWhere(it) }
    return this
}

/**
 * Adds dynamic OR conditions to an existing WHERE clause.
 */
fun SelectBuilder.dynamicOrWhere(block: DynamicWhereScope.() -> Unit): SelectBuilder {
    val scope = DynamicWhereScope()
    scope.block()
    scope.build()?.let { orWhere(it) }
    return this
}

/**
 * Applies a filter object to the query.
 * Convenient pattern for search/filter DTOs.
 *
 * Usage:
 * ```kotlin
 * data class UserSearchFilter(
 *     val name: String? = null,
 *     val email: String? = null,
 *     val active: Boolean? = null
 * )
 *
 * fun searchUsers(filter: UserSearchFilter) =
 *     Users.selectAll()
 *         .applyFilter(filter) { f ->
 *             f.name?.let { +Users.name.contains(it) }
 *             f.email?.let { +Users.email.contains(it) }
 *             f.active?.let { +Users.active.eq(it) }
 *         }
 * ```
 */
fun <F> SelectBuilder.applyFilter(filter: F, block: DynamicWhereScope.(F) -> Unit): SelectBuilder {
    val scope = DynamicWhereScope()
    scope.block(filter)
    scope.build()?.let { where(it) }
    return this
}

// ============== Generic WhereClauseSupport Extensions ==============

/**
 * Generic dynamic WHERE for any builder supporting WHERE clauses.
 */
fun <T : WhereClauseSupport<T>> WhereClauseSupport<T>.dynamicWhere(block: DynamicWhereScope.() -> Unit): T {
    val scope = DynamicWhereScope()
    scope.block()
    scope.build()?.let { where(it) }
    return self()
}

/**
 * Generic filter application for any builder supporting WHERE clauses.
 */
fun <T : WhereClauseSupport<T>, F> WhereClauseSupport<T>.applyFilter(
    filter: F,
    block: DynamicWhereScope.(F) -> Unit
): T {
    val scope = DynamicWhereScope()
    scope.block(filter)
    scope.build()?.let { where(it) }
    return self()
}

// ============== Convenience Column Extensions for Dynamic Queries ==============

/**
 * Creates an equality predicate for use in dynamic queries.
 * Null-safe: returns null if value is null.
 *
 * Usage in dynamic query:
 * ```kotlin
 * dynamicWhere {
 *     Users.name.eqOrNull(searchName)?.let { +it }
 * }
 * ```
 */
fun <T> Column<T>.eqOrNull(value: T?): Predicate? =
    value?.let { this eq it }

/**
 * Creates a contains predicate for String columns.
 * Null-safe: returns null if value is null or blank.
 */
fun Column<String>.containsOrNull(value: String?): Predicate? =
    value?.takeIf { it.isNotBlank() }?.let { this like "%$it%" }

/**
 * Creates a starts-with predicate for String columns.
 * Null-safe: returns null if value is null or blank.
 */
fun Column<String>.startsWithOrNull(value: String?): Predicate? =
    value?.takeIf { it.isNotBlank() }?.let { this like "$it%" }

/**
 * Creates an ends-with predicate for String columns.
 * Null-safe: returns null if value is null or blank.
 */
fun Column<String>.endsWithOrNull(value: String?): Predicate? =
    value?.takeIf { it.isNotBlank() }?.let { this like "%$it" }

/**
 * Creates an IN predicate for collections.
 * Null-safe: returns null if collection is null or empty.
 */
fun <T> Column<T>.inListOrNull(values: Collection<T>?): Predicate? =
    values?.takeIf { it.isNotEmpty() }?.let { this inList it }

/**
 * Creates a >= predicate.
 * Null-safe: returns null if value is null.
 */
fun <T : Comparable<T>> Column<T>.gteOrNull(value: T?): Predicate? =
    value?.let { this gte it }

/**
 * Creates a <= predicate.
 * Null-safe: returns null if value is null.
 */
fun <T : Comparable<T>> Column<T>.lteOrNull(value: T?): Predicate? =
    value?.let { this lte it }

/**
 * Creates a BETWEEN predicate.
 * Null-safe: returns null if either bound is null.
 */
fun <T : Comparable<T>> Column<T>.betweenOrNull(from: T?, to: T?): Predicate? =
    if (from != null && to != null) this.between(from, to) else null
