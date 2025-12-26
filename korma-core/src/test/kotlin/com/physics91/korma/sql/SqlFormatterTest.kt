package com.physics91.korma.sql

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SqlFormatterTest {

    @Test
    fun `format normalizes whitespace`() {
        val sql = "SELECT   *  FROM    users"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users", formatted)
    }

    @Test
    fun `format adds newline before FROM`() {
        val sql = "SELECT * FROM users"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users", formatted)
    }

    @Test
    fun `format adds newline before WHERE`() {
        val sql = "SELECT * FROM users WHERE id = 1"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  WHERE id = 1", formatted)
    }

    @Test
    fun `format adds newline before AND`() {
        val sql = "SELECT * FROM users WHERE id = 1 AND name = 'test'"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  WHERE id = 1\n    AND name = 'test'", formatted)
    }

    @Test
    fun `format adds newline before OR`() {
        val sql = "SELECT * FROM users WHERE id = 1 OR name = 'test'"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  WHERE id = 1\n    OR name = 'test'", formatted)
    }

    @Test
    fun `format adds newline before ORDER BY`() {
        val sql = "SELECT * FROM users ORDER BY name"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  ORDER BY name", formatted)
    }

    @Test
    fun `format adds newline before GROUP BY`() {
        val sql = "SELECT count(*) FROM users GROUP BY status"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT count(*)\n  FROM users\n  GROUP BY status", formatted)
    }

    @Test
    fun `format adds newline before HAVING`() {
        val sql = "SELECT count(*) FROM users GROUP BY status HAVING count(*) > 1"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT count(*)\n  FROM users\n  GROUP BY status\n  HAVING count(*) > 1", formatted)
    }

    @Test
    fun `format adds newline before JOIN types`() {
        val sql = "SELECT * FROM users JOIN posts ON users.id = posts.user_id"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  JOIN posts ON users.id = posts.user_id", formatted)
    }

    @Test
    fun `format adds newline before LEFT JOIN`() {
        val sql = "SELECT * FROM users LEFT JOIN posts ON users.id = posts.user_id"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  LEFT JOIN posts ON users.id = posts.user_id", formatted)
    }

    @Test
    fun `format adds newline before RIGHT JOIN`() {
        val sql = "SELECT * FROM users RIGHT JOIN posts ON users.id = posts.user_id"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  RIGHT JOIN posts ON users.id = posts.user_id", formatted)
    }

    @Test
    fun `format adds newline before INNER JOIN`() {
        val sql = "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  INNER JOIN posts ON users.id = posts.user_id", formatted)
    }

    @Test
    fun `format adds newline before FULL JOIN`() {
        val sql = "SELECT * FROM users FULL JOIN posts ON users.id = posts.user_id"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  FULL JOIN posts ON users.id = posts.user_id", formatted)
    }

    @Test
    fun `format adds newline before CROSS JOIN`() {
        val sql = "SELECT * FROM users CROSS JOIN posts"
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users\n  CROSS JOIN posts", formatted)
    }

    @Test
    fun `format trims result`() {
        val sql = "  SELECT * FROM users  "
        val formatted = SqlFormatter.format(sql)
        assertEquals("SELECT *\n  FROM users", formatted)
    }

    @Test
    fun `format handles complex query`() {
        val sql = "SELECT u.id, u.name, count(p.id) FROM users u LEFT JOIN posts p ON u.id = p.user_id WHERE u.status = 'active' AND u.age > 18 GROUP BY u.id, u.name HAVING count(p.id) > 0 ORDER BY u.name"
        val formatted = SqlFormatter.format(sql)
        val expected = """SELECT u.id, u.name, count(p.id)
  FROM users u
  LEFT JOIN posts p ON u.id = p.user_id
  WHERE u.status = 'active'
    AND u.age > 18
  GROUP BY u.id, u.name
  HAVING count(p.id) > 0
  ORDER BY u.name"""
        assertEquals(expected, formatted)
    }
}
