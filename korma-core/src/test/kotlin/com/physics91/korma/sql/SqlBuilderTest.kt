package com.physics91.korma.sql

import com.physics91.korma.fixtures.TestDialect
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqlBuilderTest {

    private val dialect = TestDialect

    @Test
    fun `append adds raw SQL`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT * FROM users")
        assertEquals("SELECT * FROM users", builder.toString())
    }

    @Test
    fun `append returns builder for chaining`() {
        val builder = SqlBuilder(dialect)
        val result = builder.append("SELECT")
        assertEquals(builder, result)
    }

    @Test
    fun `appendWithSpace adds space when needed`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT")
        builder.appendWithSpace("*")
        assertEquals("SELECT *", builder.toString())
    }

    @Test
    fun `appendWithSpace does not add space when already present`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT ")
        builder.appendWithSpace("*")
        assertEquals("SELECT *", builder.toString())
    }

    @Test
    fun `appendWithSpace does not add space after newline`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT\n")
        builder.appendWithSpace("*")
        assertEquals("SELECT\n*", builder.toString())
    }

    @Test
    fun `appendWithSpace does not add space on empty builder`() {
        val builder = SqlBuilder(dialect)
        builder.appendWithSpace("SELECT")
        assertEquals("SELECT", builder.toString())
    }

    @Test
    fun `newLine adds newline character`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT *")
        builder.newLine()
        builder.append("FROM users")
        assertEquals("SELECT *\nFROM users", builder.toString())
    }

    @Test
    fun `appendParameter adds placeholder and stores value`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT * FROM users WHERE id = ")
        builder.appendParameter(42L)

        val result = builder.build()
        assertEquals("SELECT * FROM users WHERE id = ?", result.sql)
        assertEquals(listOf<Any?>(42L), result.params)
    }

    @Test
    fun `appendParameters adds multiple placeholders`() {
        val builder = SqlBuilder(dialect)
        builder.append("INSERT INTO users (id, name) VALUES (")
        builder.appendParameters(listOf(1L, "John"))
        builder.append(")")

        val result = builder.build()
        assertEquals("INSERT INTO users (id, name) VALUES (?, ?)", result.sql)
        assertEquals(listOf<Any?>(1L, "John"), result.params)
    }

    @Test
    fun `appendIdentifier quotes the identifier`() {
        val builder = SqlBuilder(dialect)
        builder.appendIdentifier("users")
        assertEquals("\"users\"", builder.toString())
    }

    @Test
    fun `appendQualifiedColumn adds table and column`() {
        val builder = SqlBuilder(dialect)
        builder.appendQualifiedColumn("users", "name")
        assertEquals("\"users\".\"name\"", builder.toString())
    }

    @Test
    fun `length returns current SQL length`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT")
        assertEquals(6, builder.length)
    }

    @Test
    fun `isEmpty returns true for empty builder`() {
        val builder = SqlBuilder(dialect)
        assertTrue(builder.isEmpty)
    }

    @Test
    fun `isEmpty returns false for non-empty builder`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT")
        assertFalse(builder.isEmpty)
    }

    @Test
    fun `build returns PreparedSql with sql and params`() {
        val builder = SqlBuilder(dialect)
        builder.append("SELECT * FROM users WHERE id = ")
        builder.appendParameter(1L)
        builder.append(" AND name = ")
        builder.appendParameter("John")

        val result = builder.build()
        assertEquals("SELECT * FROM users WHERE id = ? AND name = ?", result.sql)
        assertEquals(listOf<Any?>(1L, "John"), result.params)
    }

    @Test
    fun `buildSql extension function works correctly`() {
        val result = buildSql(dialect) {
            append("SELECT * FROM ")
            appendIdentifier("users")
            append(" WHERE id = ")
            appendParameter(1L)
        }

        assertEquals("SELECT * FROM \"users\" WHERE id = ?", result.sql)
        assertEquals(listOf<Any?>(1L), result.params)
    }

    @Test
    fun `chained operations work correctly`() {
        val result = SqlBuilder(dialect)
            .append("SELECT ")
            .appendIdentifier("id")
            .append(", ")
            .appendIdentifier("name")
            .append(" FROM ")
            .appendIdentifier("users")
            .build()

        assertEquals("SELECT \"id\", \"name\" FROM \"users\"", result.sql)
    }
}
