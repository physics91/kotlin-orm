package com.physics91.korma.dao.relation

import com.physics91.korma.dao.entity.Entity
import com.physics91.korma.dao.entity.EntityTable
import com.physics91.korma.schema.Column

/**
 * Fetch strategy for loading related entities.
 */
enum class FetchType {
    /**
     * Load related entities lazily (on first access).
     */
    LAZY,

    /**
     * Load related entities immediately with the parent.
     */
    EAGER
}

/**
 * Cascade operations for related entities.
 */
enum class CascadeType {
    /**
     * Cascade persist operations.
     */
    PERSIST,

    /**
     * Cascade merge/update operations.
     */
    MERGE,

    /**
     * Cascade remove/delete operations.
     */
    REMOVE,

    /**
     * Cascade refresh operations.
     */
    REFRESH,

    /**
     * All cascade operations.
     */
    ALL
}

/**
 * Base interface for entity relationships.
 *
 * @param SOURCE The source entity type
 * @param TARGET The target entity type
 */
sealed interface Relation<SOURCE : Entity<*>, TARGET : Entity<*>> {
    /**
     * The source entity table.
     */
    val sourceTable: EntityTable<*, SOURCE>

    /**
     * The target entity table.
     */
    val targetTable: EntityTable<*, TARGET>

    /**
     * Fetch strategy.
     */
    val fetchType: FetchType

    /**
     * Cascade operations.
     */
    val cascadeTypes: Set<CascadeType>
}

/**
 * One-to-One relationship.
 *
 * Example:
 * ```kotlin
 * // User has one Profile
 * val profile by oneToOne(
 *     target = Profiles,
 *     foreignKey = Profiles.userId
 * )
 * ```
 */
class OneToOne<SOURCE_ID : Comparable<SOURCE_ID>, SOURCE : Entity<SOURCE_ID>,
        TARGET_ID : Comparable<TARGET_ID>, TARGET : Entity<TARGET_ID>>(
    override val sourceTable: EntityTable<SOURCE_ID, SOURCE>,
    override val targetTable: EntityTable<TARGET_ID, TARGET>,
    val foreignKey: Column<SOURCE_ID>,
    override val fetchType: FetchType = FetchType.LAZY,
    override val cascadeTypes: Set<CascadeType> = emptySet()
) : Relation<SOURCE, TARGET>

/**
 * One-to-Many relationship.
 *
 * Example:
 * ```kotlin
 * // User has many Posts
 * val posts by oneToMany(
 *     target = Posts,
 *     foreignKey = Posts.authorId
 * )
 * ```
 */
class OneToMany<SOURCE_ID : Comparable<SOURCE_ID>, SOURCE : Entity<SOURCE_ID>,
        TARGET_ID : Comparable<TARGET_ID>, TARGET : Entity<TARGET_ID>>(
    override val sourceTable: EntityTable<SOURCE_ID, SOURCE>,
    override val targetTable: EntityTable<TARGET_ID, TARGET>,
    val foreignKey: Column<SOURCE_ID>,
    override val fetchType: FetchType = FetchType.LAZY,
    override val cascadeTypes: Set<CascadeType> = emptySet()
) : Relation<SOURCE, TARGET>

/**
 * Many-to-One relationship.
 *
 * Example:
 * ```kotlin
 * // Post belongs to User (author)
 * val author by manyToOne(
 *     target = Users,
 *     foreignKey = Posts.authorId
 * )
 * ```
 */
class ManyToOne<SOURCE_ID : Comparable<SOURCE_ID>, SOURCE : Entity<SOURCE_ID>,
        TARGET_ID : Comparable<TARGET_ID>, TARGET : Entity<TARGET_ID>>(
    override val sourceTable: EntityTable<SOURCE_ID, SOURCE>,
    override val targetTable: EntityTable<TARGET_ID, TARGET>,
    val foreignKey: Column<TARGET_ID>,
    override val fetchType: FetchType = FetchType.LAZY,
    override val cascadeTypes: Set<CascadeType> = emptySet()
) : Relation<SOURCE, TARGET>

/**
 * Many-to-Many relationship through a join table.
 *
 * Example:
 * ```kotlin
 * // User has many Roles through UserRoles join table
 * val roles by manyToMany(
 *     target = Roles,
 *     joinTable = UserRoles,
 *     sourceKey = UserRoles.userId,
 *     targetKey = UserRoles.roleId
 * )
 * ```
 */
class ManyToMany<SOURCE_ID : Comparable<SOURCE_ID>, SOURCE : Entity<SOURCE_ID>,
        TARGET_ID : Comparable<TARGET_ID>, TARGET : Entity<TARGET_ID>>(
    override val sourceTable: EntityTable<SOURCE_ID, SOURCE>,
    override val targetTable: EntityTable<TARGET_ID, TARGET>,
    val joinTable: EntityTable<*, *>,
    val sourceKey: Column<SOURCE_ID>,
    val targetKey: Column<TARGET_ID>,
    override val fetchType: FetchType = FetchType.LAZY,
    override val cascadeTypes: Set<CascadeType> = emptySet()
) : Relation<SOURCE, TARGET>
