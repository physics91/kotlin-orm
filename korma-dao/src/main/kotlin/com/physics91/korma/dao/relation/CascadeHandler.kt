package com.physics91.korma.dao.relation

import com.physics91.korma.dao.entity.Entity
import com.physics91.korma.dao.entity.EntityStore
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Cascade operation types.
 */
enum class CascadeOperation {
    PERSIST,
    MERGE,
    REMOVE,
    REFRESH
}

/**
 * Handler for cascade operations on related entities.
 *
 * Cascade operations propagate changes from parent entities to related entities:
 * - PERSIST: When persisting a parent, also persist new related entities
 * - MERGE: When updating a parent, also update related entities
 * - REMOVE: When deleting a parent, also delete related entities
 * - REFRESH: When refreshing a parent, also refresh related entities
 */
class CascadeHandler(
    private val dialect: SqlDialect,
    private val entityStore: EntityStore
) {

    /**
     * Process cascade operations for a parent entity.
     *
     * @param operation The cascade operation to perform
     * @param relation The relation being cascaded
     * @param relatedEntities The related entities to cascade to
     * @return List of PreparedSql for execution
     */
    fun <SOURCE : Entity<*>, TARGET : Entity<*>> processCascade(
        operation: CascadeOperation,
        relation: Relation<SOURCE, TARGET>,
        relatedEntities: List<TARGET>
    ): List<PreparedSql> {
        // Check if this operation should cascade
        if (!shouldCascade(operation, relation.cascadeTypes)) {
            return emptyList()
        }

        return when (operation) {
            CascadeOperation.PERSIST -> cascadePersist(relatedEntities)
            CascadeOperation.MERGE -> cascadeMerge(relatedEntities)
            CascadeOperation.REMOVE -> cascadeRemove(relatedEntities)
            CascadeOperation.REFRESH -> emptyList() // Refresh is handled differently
        }
    }

    /**
     * Check if the given operation should cascade based on cascade types.
     */
    private fun shouldCascade(
        operation: CascadeOperation,
        cascadeTypes: Set<CascadeType>
    ): Boolean {
        if (CascadeType.ALL in cascadeTypes) return true

        return when (operation) {
            CascadeOperation.PERSIST -> CascadeType.PERSIST in cascadeTypes
            CascadeOperation.MERGE -> CascadeType.MERGE in cascadeTypes
            CascadeOperation.REMOVE -> CascadeType.REMOVE in cascadeTypes
            CascadeOperation.REFRESH -> CascadeType.REFRESH in cascadeTypes
        }
    }

    /**
     * Cascade persist operation to related entities.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <TARGET : Entity<*>> cascadePersist(
        entities: List<TARGET>
    ): List<PreparedSql> {
        return entities
            .filter { it.isNew }
            .map { entity ->
                entityStore.persist(entity as Entity<Comparable<Any>>)
            }
    }

    /**
     * Cascade merge/update operation to related entities.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <TARGET : Entity<*>> cascadeMerge(
        entities: List<TARGET>
    ): List<PreparedSql> {
        return entities
            .filter { !it.isNew }
            .map { entity ->
                entityStore.update(entity as Entity<Comparable<Any>>)
            }
    }

    /**
     * Cascade remove/delete operation to related entities.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <TARGET : Entity<*>> cascadeRemove(
        entities: List<TARGET>
    ): List<PreparedSql> {
        return entities
            .filter { !it.isNew }
            .map { entity ->
                entityStore.delete(entity as Entity<Comparable<Any>>)
            }
    }
}

/**
 * Builder for cascade configuration.
 */
class CascadeBuilder {
    private val types = mutableSetOf<CascadeType>()

    /**
     * Add PERSIST cascade.
     */
    fun persist(): CascadeBuilder {
        types.add(CascadeType.PERSIST)
        return this
    }

    /**
     * Add MERGE cascade.
     */
    fun merge(): CascadeBuilder {
        types.add(CascadeType.MERGE)
        return this
    }

    /**
     * Add REMOVE cascade.
     */
    fun remove(): CascadeBuilder {
        types.add(CascadeType.REMOVE)
        return this
    }

    /**
     * Add REFRESH cascade.
     */
    fun refresh(): CascadeBuilder {
        types.add(CascadeType.REFRESH)
        return this
    }

    /**
     * Add ALL cascades.
     */
    fun all(): CascadeBuilder {
        types.add(CascadeType.ALL)
        return this
    }

    /**
     * Build the cascade type set.
     */
    fun build(): Set<CascadeType> = types.toSet()
}

/**
 * Create cascade configuration.
 */
fun cascade(init: CascadeBuilder.() -> Unit): Set<CascadeType> {
    val builder = CascadeBuilder()
    builder.init()
    return builder.build()
}

/**
 * Orphan removal handler.
 * Handles entities that are removed from a collection relationship.
 */
class OrphanRemovalHandler(
    private val dialect: SqlDialect,
    private val entityStore: EntityStore
) {
    /**
     * Track orphans when a collection is modified.
     *
     * @param originalCollection The original collection before modification
     * @param currentCollection The current collection after modification
     * @return List of PreparedSql to delete orphans
     */
    @Suppress("UNCHECKED_CAST")
    fun <TARGET : Entity<*>> handleOrphans(
        originalCollection: List<TARGET>,
        currentCollection: List<TARGET>
    ): List<PreparedSql> {
        val currentIds = currentCollection.mapNotNull { it.id }.toSet()
        val orphans = originalCollection.filter { it.id !in currentIds && !it.isNew }

        return orphans.map { entity ->
            entityStore.delete(entity as Entity<Comparable<Any>>)
        }
    }
}

/**
 * Unit of work for tracking entity changes and cascade operations.
 */
class UnitOfWork(
    private val dialect: SqlDialect
) {
    private val newEntities = mutableListOf<Entity<*>>()
    private val dirtyEntities = mutableListOf<Entity<*>>()
    private val removedEntities = mutableListOf<Entity<*>>()

    /**
     * Register a new entity to be persisted.
     */
    fun registerNew(entity: Entity<*>) {
        if (entity !in newEntities) {
            newEntities.add(entity)
        }
    }

    /**
     * Register an entity as dirty (needs update).
     */
    fun registerDirty(entity: Entity<*>) {
        if (entity !in dirtyEntities && !entity.isNew) {
            dirtyEntities.add(entity)
        }
    }

    /**
     * Register an entity for removal.
     */
    fun registerRemoved(entity: Entity<*>) {
        if (!entity.isNew && entity !in removedEntities) {
            removedEntities.add(entity)
        }
    }

    /**
     * Get all new entities.
     */
    fun getNewEntities(): List<Entity<*>> = newEntities.toList()

    /**
     * Get all dirty entities.
     */
    fun getDirtyEntities(): List<Entity<*>> = dirtyEntities.toList()

    /**
     * Get all removed entities.
     */
    fun getRemovedEntities(): List<Entity<*>> = removedEntities.toList()

    /**
     * Clear all tracked entities.
     */
    fun clear() {
        newEntities.clear()
        dirtyEntities.clear()
        removedEntities.clear()
    }

    /**
     * Check if any changes are tracked.
     */
    fun hasChanges(): Boolean =
        newEntities.isNotEmpty() || dirtyEntities.isNotEmpty() || removedEntities.isNotEmpty()
}
