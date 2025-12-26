package com.physics91.korma.sql

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PreparedSqlTest {

    @Test
    fun `parameterCount returns correct count`() {
        val sql = PreparedSql("SELECT * FROM users WHERE id = ? AND name = ?", listOf(1L, "John"))
        assertEquals(2, sql.parameterCount)
    }

    @Test
    fun `parameterCount returns zero for no params`() {
        val sql = PreparedSql("SELECT 1", emptyList())
        assertEquals(0, sql.parameterCount)
    }

    @Test
    fun `toString without params returns raw SQL`() {
        val sql = PreparedSql("SELECT * FROM users", emptyList())
        assertEquals("SELECT * FROM users", sql.toString())
    }

    @Test
    fun `toString replaces placeholders with string values`() {
        val sql = PreparedSql("SELECT * FROM users WHERE name = ?", listOf("John"))
        assertEquals("SELECT * FROM users WHERE name = 'John'", sql.toString())
    }

    @Test
    fun `toString replaces placeholders with number values`() {
        val sql = PreparedSql("SELECT * FROM users WHERE id = ?", listOf(42L))
        assertEquals("SELECT * FROM users WHERE id = 42", sql.toString())
    }

    @Test
    fun `toString replaces placeholders with boolean true`() {
        val sql = PreparedSql("SELECT * FROM users WHERE active = ?", listOf(true))
        assertEquals("SELECT * FROM users WHERE active = TRUE", sql.toString())
    }

    @Test
    fun `toString replaces placeholders with boolean false`() {
        val sql = PreparedSql("SELECT * FROM users WHERE active = ?", listOf(false))
        assertEquals("SELECT * FROM users WHERE active = FALSE", sql.toString())
    }

    @Test
    fun `toString replaces placeholders with null`() {
        val sql = PreparedSql("SELECT * FROM users WHERE name = ?", listOf(null))
        assertEquals("SELECT * FROM users WHERE name = NULL", sql.toString())
    }

    @Test
    fun `toString handles multiple placeholders`() {
        val sql = PreparedSql(
            "SELECT * FROM users WHERE id = ? AND name = ? AND active = ?",
            listOf(1L, "John", true)
        )
        assertEquals("SELECT * FROM users WHERE id = 1 AND name = 'John' AND active = TRUE", sql.toString())
    }

    @Test
    fun `toString handles other types as quoted strings`() {
        data class CustomType(val value: String) {
            override fun toString() = value
        }
        val sql = PreparedSql("SELECT * FROM users WHERE data = ?", listOf(CustomType("custom")))
        assertEquals("SELECT * FROM users WHERE data = 'custom'", sql.toString())
    }

    @Test
    fun `EMPTY companion returns empty sql`() {
        assertEquals("", PreparedSql.EMPTY.sql)
        assertEquals(0, PreparedSql.EMPTY.parameterCount)
    }

    @Test
    fun `of companion creates sql without params`() {
        val sql = PreparedSql.of("SELECT 1")
        assertEquals("SELECT 1", sql.sql)
        assertEquals(0, sql.parameterCount)
    }
}
