package com.physics91.korma.dao.relation

import com.physics91.korma.dao.entity.*
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.sql.BaseSqlDialect
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/**
 * Tests for loading strategies.
 */
class LoadingStrategyTest {

    private val dialect = object : BaseSqlDialect() {
        override val name = "test"
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    // ============== Test Entity Implementations ==============

    object Users : LongEntityTable<User>("users") {
        val name = varchar("name", 100)

        override fun createEntity() = User()
        override fun entityToMap(entity: User): Map<Column<*>, Any?> = mapOf(name to entity.name)
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: User) {
            entity.name = row[name] as String
        }
    }

    class User(id: Long = 0L) : LongEntity(id) {
        var name: String = ""
        override val entityTable get() = Users
    }

    object Posts : LongEntityTable<Post>("posts") {
        val authorId = long("author_id")
        val title = varchar("title", 200)

        override fun createEntity() = Post()
        override fun entityToMap(entity: Post): Map<Column<*>, Any?> = mapOf(
            authorId to entity.authorId,
            title to entity.title
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: Post) {
            entity.authorId = row[authorId] as Long
            entity.title = row[title] as String
        }
    }

    class Post(id: Long = 0L) : LongEntity(id) {
        var authorId: Long = 0L
        var title: String = ""
        override val entityTable get() = Posts
    }

    // ============== LazyLoading Tests ==============

    @Test
    fun `LazyLoading builds one query per parent`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = LazyLoading.buildQueries(relation, listOf(1L, 2L, 3L), dialect)

        assertEquals(3, queries.size)
    }

    @Test
    fun `LazyLoading generates correct SQL for OneToMany`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = LazyLoading.buildQueries(relation, listOf(1L), dialect)

        assertEquals(1, queries.size)
        assertTrue(queries[0].sql.contains("SELECT"))
        assertTrue(queries[0].sql.contains("FROM"))
        assertTrue(queries[0].sql.contains("\"posts\""))
        assertTrue(queries[0].sql.contains("WHERE"))
        assertEquals(listOf<Any?>(1L), queries[0].params)
    }

    @Test
    fun `LazyLoading returns empty list for empty parent ids`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = LazyLoading.buildQueries(relation, emptyList(), dialect)

        assertTrue(queries.isEmpty())
    }

    // ============== EagerLoading Tests ==============

    @Test
    fun `EagerLoading builds single query for all parents`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = EagerLoading.buildQueries(relation, listOf(1L, 2L, 3L), dialect)

        assertEquals(1, queries.size)
    }

    @Test
    fun `EagerLoading uses IN clause`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = EagerLoading.buildQueries(relation, listOf(1L, 2L), dialect)

        assertEquals(1, queries.size)
        assertTrue(queries[0].sql.contains("IN"))
        assertEquals(2, queries[0].params.size)
    }

    @Test
    fun `EagerLoading returns empty list for empty parent ids`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = EagerLoading.buildQueries(relation, emptyList(), dialect)

        assertTrue(queries.isEmpty())
    }

    // ============== BatchLoading Tests ==============

    @Test
    fun `BatchLoading splits into batches`() {
        val batchLoading = BatchLoading(batchSize = 2)
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = batchLoading.buildQueries(relation, listOf(1L, 2L, 3L, 4L, 5L), dialect)

        assertEquals(3, queries.size) // 5 ids with batch size 2 = 3 batches
    }

    @Test
    fun `BatchLoading with default batch size`() {
        val batchLoading = BatchLoading()
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        // 150 parent IDs with default batch size of 100
        val parentIds = (1L..150L).toList()
        val queries = batchLoading.buildQueries(relation, parentIds, dialect)

        assertEquals(2, queries.size)
    }

    @Test
    fun `BatchLoading each batch uses IN clause`() {
        val batchLoading = BatchLoading(batchSize = 3)
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = batchLoading.buildQueries(relation, listOf(1L, 2L, 3L), dialect)

        assertEquals(1, queries.size)
        assertTrue(queries[0].sql.contains("IN"))
        assertEquals(3, queries[0].params.size)
    }

    // ============== EntityFetcher Tests ==============

    @Test
    fun `EntityFetcher uses default strategy`() {
        val fetcher = EntityFetcher(dialect)
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        // Default is LazyLoading, should produce one query per parent
        val queries = fetcher.prepareQueries(relation, listOf(1L, 2L))

        assertEquals(2, queries.size)
    }

    @Test
    fun `EntityFetcher can use custom strategy`() {
        val fetcher = EntityFetcher(dialect)
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        val queries = fetcher.prepareQueries(relation, listOf(1L, 2L), EagerLoading)

        assertEquals(1, queries.size)
    }

    @Test
    fun `EntityFetcher getStrategy returns correct strategy`() {
        val fetcher = EntityFetcher(dialect)

        assertEquals(LazyLoading, fetcher.getStrategy(FetchType.LAZY))
        assertEquals(EagerLoading, fetcher.getStrategy(FetchType.EAGER))
    }
}
