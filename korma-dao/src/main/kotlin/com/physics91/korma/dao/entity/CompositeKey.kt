package com.physics91.korma.dao.entity

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.CompositeKey
import com.physics91.korma.schema.Table

/**
 * Extension function to create a WHERE predicate matching all composite key columns.
 *
 * Usage:
 * ```kotlin
 * object UserRoles : CompositeKeyTable("user_roles") {
 *     val userId = long("user_id").references(Users.id)
 *     val roleId = long("role_id").references(Roles.id)
 *
 *     override val compositeKey = primaryKey(userId, roleId)
 * }
 *
 * class UserRole : CompositeKeyEntity() {
 *     var userId: Long = 0L
 *     var roleId: Long = 0L
 *
 *     override fun getCompositeKey() = mapOf(
 *         UserRoles.userId to userId,
 *         UserRoles.roleId to roleId
 *     )
 *
 *     override val entityTable get() = UserRoles
 * }
 * ```
 */
fun CompositeKey.wherePredicate(values: Map<Column<*>, Any?>): Predicate {
    require(values.keys.containsAll(columns)) {
        "Values must include all composite key columns"
    }

    var predicate: Predicate? = null
    for (column in columns) {
        @Suppress("UNCHECKED_CAST")
        val columnExpr = ColumnExpression(column as Column<Any?>)
        val value = values[column]
        val eq = columnExpr eq value

        predicate = if (predicate == null) eq else predicate.and(eq)
    }

    return predicate!!
}

/**
 * Table with a composite primary key.
 */
abstract class CompositeKeyTable(name: String) : Table(name) {
    /**
     * The composite primary key definition.
     * Must be overridden to define the composite key columns.
     */
    abstract override val compositeKey: CompositeKey

    /**
     * Whether this table has auto-generated key values.
     * For composite keys, this is typically false.
     */
    open val autoGenerateKey: Boolean = false
}

/**
 * Base class for entities with composite keys.
 *
 * Composite key entities don't have a single `id` property.
 * Instead, they implement `getCompositeKey()` to return all key values.
 */
abstract class CompositeKeyEntity : CompositeEntity {
    /**
     * Whether this entity is new (not yet persisted).
     */
    open val isNew: Boolean = true

    /**
     * The entity table this entity belongs to.
     */
    abstract val entityTable: CompositeKeyTable

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as CompositeKeyEntity

        val thisKey = getCompositeKey()
        val otherKey = other.getCompositeKey()

        // Both must have non-null values for all keys
        if (thisKey.values.any { it == null } || otherKey.values.any { it == null }) {
            return false
        }

        return thisKey == otherKey
    }

    override fun hashCode(): Int {
        val key = getCompositeKey()
        return if (key.values.any { it == null }) {
            System.identityHashCode(this)
        } else {
            key.hashCode()
        }
    }

    override fun toString(): String {
        val keyStr = getCompositeKey().entries.joinToString(", ") { "${it.key.name}=${it.value}" }
        return "${this::class.simpleName}($keyStr)"
    }
}

/**
 * EntityClass for composite key entities.
 */
abstract class CompositeKeyEntityClass<E : CompositeKeyEntity>(
    val table: CompositeKeyTable
) {
    /**
     * Create a new entity instance.
     */
    abstract fun createEntity(): E

    /**
     * Create a new entity with initialization.
     */
    fun new(init: E.() -> Unit = {}): E {
        val entity = createEntity()
        entity.init()
        return entity
    }

    /**
     * Find entity by composite key values.
     */
    fun findByKey(vararg keyValues: Pair<Column<*>, Any?>): CompositeKeyQuery<E> {
        return CompositeKeyQuery(table, this).where(keyValues.toMap())
    }

    /**
     * Find all entities matching the predicate.
     */
    fun find(predicate: () -> Predicate): CompositeKeyQuery<E> {
        return CompositeKeyQuery(table, this).whereClause(predicate())
    }

    /**
     * Find all entities.
     */
    fun all(): CompositeKeyQuery<E> {
        return CompositeKeyQuery(table, this)
    }
}

/**
 * Query builder for composite key entities.
 */
class CompositeKeyQuery<E : CompositeKeyEntity>(
    private val table: CompositeKeyTable,
    private val entityClass: CompositeKeyEntityClass<E>
) {
    private val builder = com.physics91.korma.dsl.SelectBuilder(table)

    /**
     * Add WHERE condition by composite key values.
     */
    fun where(keyValues: Map<Column<*>, Any?>): CompositeKeyQuery<E> {
        builder.where(table.compositeKey.wherePredicate(keyValues))
        return this
    }

    /**
     * Add custom WHERE clause.
     */
    fun whereClause(predicate: Predicate): CompositeKeyQuery<E> {
        builder.where(predicate)
        return this
    }

    /**
     * Add AND condition.
     */
    fun andWhere(predicate: Predicate): CompositeKeyQuery<E> {
        builder.andWhere(predicate)
        return this
    }

    /**
     * Order by column.
     */
    fun orderBy(column: Column<*>): CompositeKeyQuery<E> {
        builder.orderBy(column)
        return this
    }

    /**
     * Limit results.
     */
    fun limit(count: Int): CompositeKeyQuery<E> {
        builder.limit(count)
        return this
    }

    /**
     * Offset results.
     */
    fun offset(count: Long): CompositeKeyQuery<E> {
        builder.offset(count)
        return this
    }

    /**
     * Get the underlying SelectBuilder.
     */
    fun toSelectBuilder() = builder

    /**
     * Get the entity class for result mapping.
     */
    fun getEntityClass() = entityClass

    /**
     * Get the table.
     */
    fun getTable() = table
}
