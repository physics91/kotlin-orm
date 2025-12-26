package com.physics91.korma.dao.entity

import com.physics91.korma.dsl.SelectBuilder
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column

/**
 * Companion object base class for Entity types.
 *
 * Provides CRUD operations and query building for entities.
 *
 * Example:
 * ```kotlin
 * class User(id: Long = 0L) : LongEntity(id) {
 *     var name: String = ""
 *     var email: String = ""
 *
 *     companion object : LongEntityClass<User>(Users)
 * }
 *
 * object Users : LongEntityTable<User>("users") {
 *     val name = varchar("name", 100)
 *     val email = varchar("email", 255)
 *
 *     override fun createEntity() = User()
 *     override fun entityToMap(entity: User) = mapOf(
 *         name to entity.name,
 *         email to entity.email
 *     )
 *     override fun mapToEntity(row: Map<Column<*>, Any?>, entity: User) {
 *         entity.name = row[name] as String
 *         entity.email = row[email] as String
 *     }
 * }
 * ```
 *
 * @param ID The type of the primary key
 * @param E The entity type
 */
abstract class EntityClass<ID : Comparable<ID>, E : Entity<ID>>(
    val table: EntityTable<ID, E>
) {

    /**
     * Create a new entity instance (not persisted).
     */
    fun new(init: E.() -> Unit = {}): E {
        val entity = table.createEntity()
        entity.init()
        return entity
    }

    /**
     * Find entity by primary key.
     * Returns null if not found.
     */
    fun findById(id: ID): EntityQuery<ID, E> {
        return EntityQuery(table).where { ColumnExpression(table.id) eq id }
    }

    /**
     * Find all entities matching the predicate.
     */
    fun find(predicate: () -> Predicate): EntityQuery<ID, E> {
        return EntityQuery(table).where(predicate)
    }

    /**
     * Find all entities.
     */
    fun all(): EntityQuery<ID, E> {
        return EntityQuery(table)
    }

    /**
     * Count all entities.
     */
    fun count(): CountQuery<ID, E> {
        return CountQuery(table)
    }

    /**
     * Count entities matching the predicate.
     */
    fun count(predicate: () -> Predicate): CountQuery<ID, E> {
        return CountQuery(table).where(predicate)
    }

    /**
     * Check if any entity exists matching the predicate.
     */
    fun exists(predicate: () -> Predicate): ExistsQuery<ID, E> {
        return ExistsQuery(table).where(predicate)
    }

    /**
     * Start a query builder for this entity.
     */
    fun query(): SelectBuilder {
        return SelectBuilder(table)
    }
}

/**
 * EntityClass for Long ID entities.
 */
abstract class LongEntityClass<E : LongEntity>(
    table: LongEntityTable<E>
) : EntityClass<Long, E>(table)

/**
 * EntityClass for Int ID entities.
 */
abstract class IntEntityClass<E : IntEntity>(
    table: IntEntityTable<E>
) : EntityClass<Int, E>(table)

/**
 * EntityClass for UUID entities.
 */
abstract class UUIDEntityClass<E : UUIDEntity>(
    table: UUIDEntityTable<E>
) : EntityClass<java.util.UUID, E>(table)

/**
 * EntityClass for String ID entities.
 */
abstract class StringEntityClass<E : StringEntity>(
    table: StringEntityTable<E>
) : EntityClass<String, E>(table)

/**
 * Query builder for entities.
 */
class EntityQuery<ID : Comparable<ID>, E : Entity<ID>>(
    private val table: EntityTable<ID, E>
) {
    private val builder = SelectBuilder(table)
    private var orderByApplied = false

    /**
     * Add a WHERE condition.
     */
    fun where(predicate: () -> Predicate): EntityQuery<ID, E> {
        builder.where(predicate)
        return this
    }

    /**
     * Add an AND condition to existing WHERE.
     */
    fun andWhere(predicate: () -> Predicate): EntityQuery<ID, E> {
        builder.andWhere(predicate())
        return this
    }

    /**
     * Add an OR condition to existing WHERE.
     */
    fun orWhere(predicate: () -> Predicate): EntityQuery<ID, E> {
        builder.orWhere(predicate())
        return this
    }

    /**
     * Order by a column ascending.
     */
    fun orderBy(column: Column<*>): EntityQuery<ID, E> {
        builder.orderBy(column)
        orderByApplied = true
        return this
    }

    /**
     * Set the LIMIT.
     */
    fun limit(count: Int): EntityQuery<ID, E> {
        builder.limit(count)
        return this
    }

    /**
     * Set the OFFSET.
     */
    fun offset(count: Long): EntityQuery<ID, E> {
        builder.offset(count)
        return this
    }

    /**
     * Paginate results.
     */
    fun paginate(page: Int, pageSize: Int): EntityQuery<ID, E> {
        builder.paginate(page, pageSize)
        return this
    }

    /**
     * Add FOR UPDATE lock.
     */
    fun forUpdate(): EntityQuery<ID, E> {
        builder.forUpdate()
        return this
    }

    /**
     * Get the underlying SelectBuilder.
     * Used by the executor to build and execute the query.
     */
    fun toSelectBuilder(): SelectBuilder = builder

    /**
     * Get the entity table.
     */
    fun getTable(): EntityTable<ID, E> = table
}

/**
 * Query for counting entities.
 */
class CountQuery<ID : Comparable<ID>, E : Entity<ID>>(
    private val table: EntityTable<ID, E>
) {
    private var whereClause: Predicate? = null

    /**
     * Add a WHERE condition.
     */
    fun where(predicate: () -> Predicate): CountQuery<ID, E> {
        whereClause = predicate()
        return this
    }

    /**
     * Get the WHERE predicate.
     */
    fun getWhere(): Predicate? = whereClause

    /**
     * Get the entity table.
     */
    fun getTable(): EntityTable<ID, E> = table
}

/**
 * Query for checking existence.
 */
class ExistsQuery<ID : Comparable<ID>, E : Entity<ID>>(
    private val table: EntityTable<ID, E>
) {
    private var whereClause: Predicate? = null

    /**
     * Add a WHERE condition.
     */
    fun where(predicate: () -> Predicate): ExistsQuery<ID, E> {
        whereClause = predicate()
        return this
    }

    /**
     * Get the WHERE predicate.
     */
    fun getWhere(): Predicate? = whereClause

    /**
     * Get the entity table.
     */
    fun getTable(): EntityTable<ID, E> = table
}
