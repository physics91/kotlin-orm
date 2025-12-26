package com.physics91.korma.dao.entity

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table

/**
 * A table that is associated with an Entity type.
 *
 * @param ID The type of the primary key
 * @param E The entity type
 * @param name The table name
 */
abstract class EntityTable<ID : Comparable<ID>, E : Entity<ID>>(
    name: String
) : Table(name) {

    /**
     * The primary key column for this entity table.
     */
    abstract val id: Column<ID>

    /**
     * Create a new entity instance from a row.
     */
    abstract fun createEntity(): E

    /**
     * Map entity properties to column values for insert/update.
     */
    abstract fun entityToMap(entity: E): Map<Column<*>, Any?>

    /**
     * Map column values from a row to entity properties.
     */
    abstract fun mapToEntity(row: Map<Column<*>, Any?>, entity: E)
}

/**
 * EntityTable with Long primary key.
 */
abstract class LongEntityTable<E : LongEntity>(
    name: String
) : EntityTable<Long, E>(name) {

    override val id: Column<Long> = long("id").primaryKey().autoIncrement()
}

/**
 * EntityTable with Int primary key.
 */
abstract class IntEntityTable<E : IntEntity>(
    name: String
) : EntityTable<Int, E>(name) {

    override val id: Column<Int> = integer("id").primaryKey().autoIncrement()
}

/**
 * EntityTable with UUID primary key.
 */
abstract class UUIDEntityTable<E : UUIDEntity>(
    name: String
) : EntityTable<java.util.UUID, E>(name) {

    override val id: Column<java.util.UUID> = uuid("id").primaryKey()
}

/**
 * EntityTable with String primary key.
 */
abstract class StringEntityTable<E : StringEntity>(
    name: String,
    idLength: Int = 255
) : EntityTable<String, E>(name) {

    override val id: Column<String> = varchar("id", idLength).primaryKey()
}
