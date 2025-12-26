package com.physics91.korma.dao.entity

import com.physics91.korma.dsl.DeleteBuilder
import com.physics91.korma.dsl.InsertBuilder
import com.physics91.korma.dsl.UpdateBuilder
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.count
import com.physics91.korma.schema.Column
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Entity store provides operations for persisting entities.
 *
 * This is the main interface between the DAO layer and the database layer.
 *
 * Example:
 * ```kotlin
 * val store = EntityStore(dialect)
 *
 * // Create
 * val user = User.new { name = "John"; email = "john@example.com" }
 * store.persist(user)
 *
 * // Read
 * val users = store.execute(User.find { Users.name eq "John" })
 *
 * // Update
 * user.name = "Jane"
 * store.update(user)
 *
 * // Delete
 * store.delete(user)
 * ```
 */
class EntityStore(
    private val dialect: SqlDialect
) {

    /**
     * Persist a new entity.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> persist(entity: E): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val table = entity.entityTable as EntityTable<ID, E>
        val values = table.entityToMap(entity)

        val builder = InsertBuilder(table)
        for ((column, value) in values) {
            @Suppress("UNCHECKED_CAST")
            builder.set(column as Column<Any?>, value)
        }

        return builder.build(dialect)
    }

    /**
     * Persist a new entity with RETURNING clause (for auto-generated IDs).
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> persistReturning(
        entity: E,
        vararg columns: Column<*>
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val table = entity.entityTable as EntityTable<ID, E>
        val values = table.entityToMap(entity)

        val builder = InsertBuilder(table)
        for ((column, value) in values) {
            @Suppress("UNCHECKED_CAST")
            builder.set(column as Column<Any?>, value)
        }

        return builder.returning(*columns).build(dialect)
    }

    /**
     * Update an existing entity.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> update(entity: E): PreparedSql {
        require(!entity.isNew) { "Cannot update a new entity. Use persist() first." }

        @Suppress("UNCHECKED_CAST")
        val table = entity.entityTable as EntityTable<ID, E>
        val values = table.entityToMap(entity)

        val builder = UpdateBuilder(table)
        for ((column, value) in values) {
            @Suppress("UNCHECKED_CAST")
            builder.set(column as Column<Any?>, value)
        }

        builder.where { ColumnExpression(table.id) eq entity.id }

        return builder.build(dialect)
    }

    /**
     * Delete an entity.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> delete(entity: E): PreparedSql {
        require(!entity.isNew) { "Cannot delete a new entity." }

        @Suppress("UNCHECKED_CAST")
        val table = entity.entityTable as EntityTable<ID, E>

        val builder = DeleteBuilder(table)
        builder.where { ColumnExpression(table.id) eq entity.id }

        return builder.build(dialect)
    }

    /**
     * Delete entity by ID.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> deleteById(
        table: EntityTable<ID, E>,
        id: ID
    ): PreparedSql {
        val builder = DeleteBuilder(table)
        builder.where { ColumnExpression(table.id) eq id }

        return builder.build(dialect)
    }

    /**
     * Build a SELECT query from EntityQuery.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> buildSelect(
        query: EntityQuery<ID, E>
    ): PreparedSql {
        return query.toSelectBuilder().selectAll().build(dialect)
    }

    /**
     * Build a COUNT query.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> buildCount(
        query: CountQuery<ID, E>
    ): PreparedSql {
        val table = query.getTable()
        val builder = com.physics91.korma.dsl.SelectBuilder(table)
            .select(count())

        val where = query.getWhere()
        if (where != null) {
            builder.where(where)
        }

        return builder.build(dialect)
    }

    /**
     * Build an EXISTS query.
     * Returns PreparedSql for execution.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> buildExists(
        query: ExistsQuery<ID, E>
    ): PreparedSql {
        val table = query.getTable()
        val builder = com.physics91.korma.dsl.SelectBuilder(table)
            .select(ColumnExpression(table.id))
            .limit(1)

        val where = query.getWhere()
        if (where != null) {
            builder.where(where)
        }

        return builder.build(dialect)
    }

    /**
     * Create an entity from a result row.
     */
    fun <ID : Comparable<ID>, E : Entity<ID>> createFromRow(
        table: EntityTable<ID, E>,
        row: Map<Column<*>, Any?>
    ): E {
        val entity = table.createEntity()

        // Set the ID
        @Suppress("UNCHECKED_CAST")
        entity.id = row[table.id] as ID

        // Map other properties
        table.mapToEntity(row, entity)

        // Mark UUID entities as persisted
        if (entity is UUIDEntity) {
            entity.markPersisted()
        }

        return entity
    }
}
