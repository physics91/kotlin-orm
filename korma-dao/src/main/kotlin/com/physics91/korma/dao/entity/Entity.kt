package com.physics91.korma.dao.entity

import com.physics91.korma.schema.Column
import java.util.UUID

/**
 * Base interface for all entities.
 *
 * An entity represents a database row with a primary key.
 *
 * @param ID The type of the primary key
 */
interface Entity<ID : Comparable<ID>> {
    /**
     * The entity's primary key value.
     * May be null for new (not yet persisted) entities.
     */
    var id: ID

    /**
     * Whether this entity is new (not yet persisted).
     */
    val isNew: Boolean
        get() = false

    /**
     * The entity table this entity belongs to.
     */
    val entityTable: EntityTable<ID, *>
}

/**
 * Base class for entities with Long primary keys.
 */
abstract class LongEntity(id: Long = 0L) : Entity<Long> {
    override var id: Long = id

    override val isNew: Boolean
        get() = id == 0L

    abstract override val entityTable: EntityTable<Long, *>

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as LongEntity

        // Both are new (unpersisted): use reference equality
        if (isNew && other.isNew) return this === other
        // Both are persisted: use ID equality
        if (!isNew && !other.isNew) return id == other.id
        // One new, one persisted: never equal
        return false
    }

    override fun hashCode(): Int = if (isNew) System.identityHashCode(this) else id.hashCode()

    override fun toString(): String = "${this::class.simpleName}(id=$id)"
}

/**
 * Base class for entities with Int primary keys.
 */
abstract class IntEntity(id: Int = 0) : Entity<Int> {
    override var id: Int = id

    override val isNew: Boolean
        get() = id == 0

    abstract override val entityTable: EntityTable<Int, *>

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as IntEntity

        // Both are new (unpersisted): use reference equality
        if (isNew && other.isNew) return this === other
        // Both are persisted: use ID equality
        if (!isNew && !other.isNew) return id == other.id
        // One new, one persisted: never equal
        return false
    }

    override fun hashCode(): Int = if (isNew) System.identityHashCode(this) else id.hashCode()

    override fun toString(): String = "${this::class.simpleName}(id=$id)"
}

/**
 * Base class for entities with UUID primary keys.
 */
abstract class UUIDEntity(id: UUID = UUID.randomUUID()) : Entity<UUID> {
    override var id: UUID = id

    private var _isNew: Boolean = true

    override val isNew: Boolean
        get() = _isNew

    abstract override val entityTable: EntityTable<UUID, *>

    internal fun markPersisted() {
        _isNew = false
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as UUIDEntity
        // UUIDEntity always uses ID-based equality since UUID is unique at creation time
        // Unlike Long/Int/String IDs, UUID doesn't have a "not yet assigned" state
        return id == other.id
    }

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String = "${this::class.simpleName}(id=$id)"
}

/**
 * Base class for entities with String primary keys.
 */
abstract class StringEntity(id: String = "") : Entity<String> {
    override var id: String = id

    override val isNew: Boolean
        get() = id.isEmpty()

    abstract override val entityTable: EntityTable<String, *>

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as StringEntity

        // Both are new (unpersisted): use reference equality
        if (isNew && other.isNew) return this === other
        // Both are persisted: use ID equality
        if (!isNew && !other.isNew) return id == other.id
        // One new, one persisted: never equal
        return false
    }

    override fun hashCode(): Int = if (isNew) System.identityHashCode(this) else id.hashCode()

    override fun toString(): String = "${this::class.simpleName}(id=$id)"
}

/**
 * Marker interface for entities with composite keys.
 */
interface CompositeEntity {
    /**
     * Get the composite key as a map of column to value.
     */
    fun getCompositeKey(): Map<Column<*>, Any?>
}
