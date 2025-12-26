package com.physics91.korma.expression

import com.physics91.korma.fixtures.TestDialect
import com.physics91.korma.fixtures.TestUsers
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PredicateBuilderTest {

    private val dialect = TestDialect

    @Test
    fun `build returns TruePredicate when no conditions added`() {
        val builder = PredicateBuilder()
        val result = builder.build()

        assertEquals(TruePredicate, result)
    }

    @Test
    fun `build returns single condition when one condition added`() {
        val condition = ColumnExpression(TestUsers.id) eq 1L

        val builder = PredicateBuilder()
        builder.condition(condition)
        val result = builder.build()

        val params = mutableListOf<Any?>()
        assertEquals("\"test_users\".\"id\" = ?", result.toSql(dialect, params))
        assertEquals(listOf<Any?>(1L), params)
    }

    @Test
    fun `build returns AndPredicate when multiple conditions added`() {
        val condition1 = ColumnExpression(TestUsers.id) eq 1L
        val condition2 = ColumnExpression(TestUsers.name) eq "John"

        val builder = PredicateBuilder()
        builder.condition(condition1)
        builder.condition(condition2)
        val result = builder.build()

        assertTrue(result is AndPredicate)
        val params = mutableListOf<Any?>()
        val sql = result.toSql(dialect, params)
        assertTrue(sql.contains("\"test_users\".\"id\" = ?"))
        assertTrue(sql.contains("\"test_users\".\"name\" = ?"))
        assertTrue(sql.contains(" AND "))
    }

    @Test
    fun `conditionIfNotNull adds condition when value is not null`() {
        val builder = PredicateBuilder()
        val name: String? = "John"

        builder.conditionIfNotNull(name) { ColumnExpression(TestUsers.name) eq it }
        val result = builder.build()

        val params = mutableListOf<Any?>()
        assertEquals("\"test_users\".\"name\" = ?", result.toSql(dialect, params))
        assertEquals(listOf<Any?>("John"), params)
    }

    @Test
    fun `conditionIfNotNull does nothing when value is null`() {
        val builder = PredicateBuilder()
        val name: String? = null

        builder.conditionIfNotNull(name) { ColumnExpression(TestUsers.name) eq it }
        val result = builder.build()

        assertEquals(TruePredicate, result)
    }

    @Test
    fun `buildPredicate DSL function works correctly`() {
        val result = buildPredicate {
            condition(ColumnExpression(TestUsers.id) eq 1L)
            condition(ColumnExpression(TestUsers.active) eq true)
        }

        assertTrue(result is AndPredicate)
    }

    @Test
    fun `buildPredicate with empty block returns TruePredicate`() {
        val result = buildPredicate { }

        assertEquals(TruePredicate, result)
    }

    @Test
    fun `mixed conditions and conditionIfNotNull`() {
        val builder = PredicateBuilder()
        val name: String? = "John"
        val age: Int? = null

        builder.condition(ColumnExpression(TestUsers.id) eq 1L)
        builder.conditionIfNotNull(name) { ColumnExpression(TestUsers.name) eq it }
        builder.conditionIfNotNull(age) { ColumnExpression(TestUsers.age) eq it }

        val result = builder.build()

        assertTrue(result is AndPredicate)
        val andPredicate = result as AndPredicate
        assertEquals(2, andPredicate.conditions.size)
    }
}
