package com.physics91.korma.dsl.clauses

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.Predicate
import com.physics91.korma.expression.eq
import com.physics91.korma.fixtures.TestUsers
import com.physics91.korma.fixtures.TestDialect
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class WhereClauseSupportTest {

    private val dialect = TestDialect

    // Simple implementation for testing
    class TestWhereBuilder : WhereClauseSupport<TestWhereBuilder> {
        override var whereClause: Predicate? = null
    }

    @Test
    fun `where with predicate sets whereClause`() {
        val builder = TestWhereBuilder()
        val predicate = ColumnExpression(TestUsers.id) eq 1L

        builder.where(predicate)

        assertNotNull(builder.whereClause)
        val params = mutableListOf<Any?>()
        val sql = builder.whereClause!!.toSql(dialect, params)
        assertEquals("\"test_users\".\"id\" = ?", sql)
        assertEquals(listOf<Any?>(1L), params)
    }

    @Test
    fun `where with lambda sets whereClause`() {
        val builder = TestWhereBuilder()

        builder.where { ColumnExpression(TestUsers.name) eq "John" }

        assertNotNull(builder.whereClause)
    }

    @Test
    fun `andWhere combines with AND when whereClause exists`() {
        val builder = TestWhereBuilder()

        builder.where(ColumnExpression(TestUsers.id) eq 1L)
        builder.andWhere(ColumnExpression(TestUsers.name) eq "John")

        assertNotNull(builder.whereClause)
        val params = mutableListOf<Any?>()
        val sql = builder.whereClause!!.toSql(dialect, params)
        assertEquals("(\"test_users\".\"id\" = ?) AND (\"test_users\".\"name\" = ?)", sql)
    }

    @Test
    fun `andWhere sets whereClause when none exists`() {
        val builder = TestWhereBuilder()

        builder.andWhere(ColumnExpression(TestUsers.name) eq "John")

        assertNotNull(builder.whereClause)
    }

    @Test
    fun `orWhere combines with OR when whereClause exists`() {
        val builder = TestWhereBuilder()

        builder.where(ColumnExpression(TestUsers.id) eq 1L)
        builder.orWhere(ColumnExpression(TestUsers.id) eq 2L)

        assertNotNull(builder.whereClause)
        val params = mutableListOf<Any?>()
        val sql = builder.whereClause!!.toSql(dialect, params)
        assertEquals("(\"test_users\".\"id\" = ?) OR (\"test_users\".\"id\" = ?)", sql)
    }

    @Test
    fun `orWhere sets whereClause when none exists`() {
        val builder = TestWhereBuilder()

        builder.orWhere(ColumnExpression(TestUsers.id) eq 1L)

        assertNotNull(builder.whereClause)
    }

    @Test
    fun `whereIfNotNull adds predicate when value is not null`() {
        val builder = TestWhereBuilder()
        val name: String? = "John"

        builder.whereIfNotNull(name) { ColumnExpression(TestUsers.name) eq it }

        assertNotNull(builder.whereClause)
    }

    @Test
    fun `whereIfNotNull does nothing when value is null`() {
        val builder = TestWhereBuilder()
        val name: String? = null

        builder.whereIfNotNull(name) { ColumnExpression(TestUsers.name) eq it }

        assertNull(builder.whereClause)
    }

    @Test
    fun `whereIfNotNull chains with existing clause`() {
        val builder = TestWhereBuilder()
        val age: Int? = 25

        builder.where(ColumnExpression(TestUsers.id) eq 1L)
        builder.whereIfNotNull(age) { ColumnExpression(TestUsers.age) eq it }

        assertNotNull(builder.whereClause)
        val params = mutableListOf<Any?>()
        val sql = builder.whereClause!!.toSql(dialect, params)
        assertEquals("(\"test_users\".\"id\" = ?) AND (\"test_users\".\"age\" = ?)", sql)
    }

    @Test
    fun `self returns builder instance`() {
        val builder = TestWhereBuilder()

        assertEquals(builder, builder.self())
    }

    @Test
    fun `chained where calls return same builder`() {
        val builder = TestWhereBuilder()

        val result = builder
            .where(ColumnExpression(TestUsers.id) eq 1L)
            .andWhere(ColumnExpression(TestUsers.name) eq "John")

        assertEquals(builder, result)
    }
}
