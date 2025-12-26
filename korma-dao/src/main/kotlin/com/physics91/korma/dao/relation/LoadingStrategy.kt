package com.physics91.korma.dao.relation

import com.physics91.korma.dao.entity.Entity
import com.physics91.korma.dao.entity.EntityTable
import com.physics91.korma.dsl.SelectBuilder
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.schema.Column
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Loading strategy for fetching related entities.
 */
sealed interface LoadingStrategy {
    /**
     * Build queries for loading related entities.
     */
    fun <E : Entity<*>> buildQueries(
        relation: Relation<*, E>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): List<PreparedSql>
}

/**
 * Lazy loading strategy.
 * Loads related entities one by one when accessed.
 */
object LazyLoading : LoadingStrategy {
    override fun <E : Entity<*>> buildQueries(
        relation: Relation<*, E>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): List<PreparedSql> {
        // Lazy loading builds queries on-demand, one at a time
        return parentIds.map { parentId ->
            buildSingleQuery(relation, parentId, dialect)
        }
    }

    private fun <E : Entity<*>> buildSingleQuery(
        relation: Relation<*, E>,
        parentId: Any,
        dialect: SqlDialect
    ): PreparedSql {
        return when (relation) {
            is OneToOne<*, *, *, *> -> buildOneToOneQuery(relation, parentId, dialect)
            is OneToMany<*, *, *, *> -> buildOneToManyQuery(relation, parentId, dialect)
            is ManyToOne<*, *, *, *> -> buildManyToOneQuery(relation, parentId, dialect)
            is ManyToMany<*, *, *, *> -> buildManyToManyQuery(relation, parentId, dialect)
        }
    }

    private fun buildOneToOneQuery(
        relation: OneToOne<*, *, *, *>,
        parentId: Any,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val fkExpr = ColumnExpression(relation.foreignKey as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { fkExpr eq parentId }
            .limit(1)
            .build(dialect)
    }

    private fun buildOneToManyQuery(
        relation: OneToMany<*, *, *, *>,
        parentId: Any,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val fkExpr = ColumnExpression(relation.foreignKey as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { fkExpr eq parentId }
            .build(dialect)
    }

    private fun buildManyToOneQuery(
        relation: ManyToOne<*, *, *, *>,
        parentId: Any,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val idExpr = ColumnExpression(relation.targetTable.id as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { idExpr eq parentId }
            .limit(1)
            .build(dialect)
    }

    private fun buildManyToManyQuery(
        relation: ManyToMany<*, *, *, *>,
        parentId: Any,
        dialect: SqlDialect
    ): PreparedSql {
        // Join through the join table
        @Suppress("UNCHECKED_CAST")
        val targetIdExpr = ColumnExpression(relation.targetTable.id as Column<Any?>)
        @Suppress("UNCHECKED_CAST")
        val sourceKeyExpr = ColumnExpression(relation.sourceKey as Column<Any?>)
        @Suppress("UNCHECKED_CAST")
        val targetKeyExpr = ColumnExpression(relation.targetKey as Column<Any?>)

        return SelectBuilder(relation.targetTable)
            .selectAll()
            .join(relation.joinTable, on = { targetIdExpr eq targetKeyExpr })
            .where { sourceKeyExpr eq parentId }
            .build(dialect)
    }
}

/**
 * Eager loading strategy.
 * Loads all related entities upfront using IN clause.
 */
object EagerLoading : LoadingStrategy {
    override fun <E : Entity<*>> buildQueries(
        relation: Relation<*, E>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): List<PreparedSql> {
        // Eager loading fetches everything in one query
        if (parentIds.isEmpty()) return emptyList()

        val query = buildBatchQuery(relation, parentIds, dialect)
        return listOf(query)
    }

    private fun buildBatchQuery(
        relation: Relation<*, *>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        return when (relation) {
            is OneToOne<*, *, *, *> -> buildEagerOneToOneQuery(relation, parentIds, dialect)
            is OneToMany<*, *, *, *> -> buildEagerOneToManyQuery(relation, parentIds, dialect)
            is ManyToOne<*, *, *, *> -> buildEagerManyToOneQuery(relation, parentIds, dialect)
            is ManyToMany<*, *, *, *> -> buildEagerManyToManyQuery(relation, parentIds, dialect)
        }
    }

    private fun buildEagerOneToOneQuery(
        relation: OneToOne<*, *, *, *>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val fkExpr = ColumnExpression(relation.foreignKey as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { fkExpr.inList(parentIds) }
            .build(dialect)
    }

    private fun buildEagerOneToManyQuery(
        relation: OneToMany<*, *, *, *>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val fkExpr = ColumnExpression(relation.foreignKey as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { fkExpr.inList(parentIds) }
            .build(dialect)
    }

    private fun buildEagerManyToOneQuery(
        relation: ManyToOne<*, *, *, *>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val idExpr = ColumnExpression(relation.targetTable.id as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { idExpr.inList(parentIds) }
            .build(dialect)
    }

    private fun buildEagerManyToManyQuery(
        relation: ManyToMany<*, *, *, *>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val targetIdExpr = ColumnExpression(relation.targetTable.id as Column<Any?>)
        @Suppress("UNCHECKED_CAST")
        val sourceKeyExpr = ColumnExpression(relation.sourceKey as Column<Any?>)
        @Suppress("UNCHECKED_CAST")
        val targetKeyExpr = ColumnExpression(relation.targetKey as Column<Any?>)

        return SelectBuilder(relation.targetTable)
            .selectAll()
            .join(relation.joinTable, on = { targetIdExpr eq targetKeyExpr })
            .where { sourceKeyExpr.inList(parentIds) }
            .build(dialect)
    }
}

/**
 * Batch loading strategy.
 * Loads related entities in batches to avoid N+1 queries.
 *
 * @param batchSize The maximum number of parent IDs per batch
 */
class BatchLoading(
    private val batchSize: Int = DEFAULT_BATCH_SIZE
) : LoadingStrategy {

    companion object {
        const val DEFAULT_BATCH_SIZE = 100
    }

    override fun <E : Entity<*>> buildQueries(
        relation: Relation<*, E>,
        parentIds: List<Any>,
        dialect: SqlDialect
    ): List<PreparedSql> {
        // Split into batches and create one query per batch
        return parentIds.chunked(batchSize).map { batch ->
            buildBatchQuery(relation, batch, dialect)
        }
    }

    private fun buildBatchQuery(
        relation: Relation<*, *>,
        batchIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        return when (relation) {
            is OneToOne<*, *, *, *> -> buildBatchOneToOneQuery(relation, batchIds, dialect)
            is OneToMany<*, *, *, *> -> buildBatchOneToManyQuery(relation, batchIds, dialect)
            is ManyToOne<*, *, *, *> -> buildBatchManyToOneQuery(relation, batchIds, dialect)
            is ManyToMany<*, *, *, *> -> buildBatchManyToManyQuery(relation, batchIds, dialect)
        }
    }

    private fun buildBatchOneToOneQuery(
        relation: OneToOne<*, *, *, *>,
        batchIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val fkExpr = ColumnExpression(relation.foreignKey as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { fkExpr.inList(batchIds) }
            .build(dialect)
    }

    private fun buildBatchOneToManyQuery(
        relation: OneToMany<*, *, *, *>,
        batchIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val fkExpr = ColumnExpression(relation.foreignKey as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { fkExpr.inList(batchIds) }
            .build(dialect)
    }

    private fun buildBatchManyToOneQuery(
        relation: ManyToOne<*, *, *, *>,
        batchIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val idExpr = ColumnExpression(relation.targetTable.id as Column<Any?>)
        return SelectBuilder(relation.targetTable)
            .selectAll()
            .where { idExpr.inList(batchIds) }
            .build(dialect)
    }

    private fun buildBatchManyToManyQuery(
        relation: ManyToMany<*, *, *, *>,
        batchIds: List<Any>,
        dialect: SqlDialect
    ): PreparedSql {
        @Suppress("UNCHECKED_CAST")
        val targetIdExpr = ColumnExpression(relation.targetTable.id as Column<Any?>)
        @Suppress("UNCHECKED_CAST")
        val sourceKeyExpr = ColumnExpression(relation.sourceKey as Column<Any?>)
        @Suppress("UNCHECKED_CAST")
        val targetKeyExpr = ColumnExpression(relation.targetKey as Column<Any?>)

        return SelectBuilder(relation.targetTable)
            .selectAll()
            .join(relation.joinTable, on = { targetIdExpr eq targetKeyExpr })
            .where { sourceKeyExpr.inList(batchIds) }
            .build(dialect)
    }
}

/**
 * Entity fetcher for handling relation loading.
 */
class EntityFetcher(
    private val dialect: SqlDialect,
    private val defaultStrategy: LoadingStrategy = LazyLoading
) {
    /**
     * Prepare queries for loading a relation.
     */
    fun <E : Entity<*>> prepareQueries(
        relation: Relation<*, E>,
        parentIds: List<Any>,
        strategy: LoadingStrategy = defaultStrategy
    ): List<PreparedSql> {
        return strategy.buildQueries(relation, parentIds, dialect)
    }

    /**
     * Get the strategy based on fetch type.
     */
    fun getStrategy(fetchType: FetchType): LoadingStrategy {
        return when (fetchType) {
            FetchType.LAZY -> LazyLoading
            FetchType.EAGER -> EagerLoading
        }
    }
}
