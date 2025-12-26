package com.physics91.korma.expression

import com.physics91.korma.fixtures.TestDialect
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SpecialPredicatesTest {

    private val dialect = TestDialect

    // ============== TruePredicate Tests ==============

    @Test
    fun `TruePredicate toSql returns 1 = 1`() {
        val params = mutableListOf<Any?>()
        val sql = TruePredicate.toSql(dialect, params)

        assertEquals("1 = 1", sql)
        assertTrue(params.isEmpty())
    }

    @Test
    fun `TruePredicate toString returns TRUE`() {
        assertEquals("TRUE", TruePredicate.toString())
    }

    // ============== FalsePredicate Tests ==============

    @Test
    fun `FalsePredicate toSql returns 1 = 0`() {
        val params = mutableListOf<Any?>()
        val sql = FalsePredicate.toSql(dialect, params)

        assertEquals("1 = 0", sql)
        assertTrue(params.isEmpty())
    }

    @Test
    fun `FalsePredicate toString returns FALSE`() {
        assertEquals("FALSE", FalsePredicate.toString())
    }

    // ============== RawPredicate Tests ==============

    @Test
    fun `RawPredicate toSql returns raw SQL`() {
        val predicate = RawPredicate("custom_function(column) > 0")
        val params = mutableListOf<Any?>()

        assertEquals("custom_function(column) > 0", predicate.toSql(dialect, params))
        assertTrue(params.isEmpty())
    }

    @Test
    fun `RawPredicate toSql adds params`() {
        val predicate = RawPredicate("column = ? AND other = ?", listOf(1, "test"))
        val params = mutableListOf<Any?>()

        predicate.toSql(dialect, params)

        assertEquals(listOf<Any?>(1, "test"), params)
    }

    @Test
    fun `RawPredicate toString returns raw SQL`() {
        val predicate = RawPredicate("custom_function(column) > 0")
        assertEquals("custom_function(column) > 0", predicate.toString())
    }

    // ============== ExistsPredicate Tests ==============

    @Test
    fun `ExistsPredicate toSql generates EXISTS clause`() {
        val predicate = ExistsPredicate(
            subquerySql = "SELECT 1 FROM users WHERE id = ?",
            subqueryParams = listOf(1L)
        )
        val params = mutableListOf<Any?>()

        val sql = predicate.toSql(dialect, params)

        assertEquals("EXISTS (SELECT 1 FROM users WHERE id = ?)", sql)
        assertEquals(listOf<Any?>(1L), params)
    }

    @Test
    fun `ExistsPredicate negated generates NOT EXISTS clause`() {
        val predicate = ExistsPredicate(
            subquerySql = "SELECT 1 FROM users WHERE id = ?",
            subqueryParams = listOf(1L),
            negated = true
        )
        val params = mutableListOf<Any?>()

        val sql = predicate.toSql(dialect, params)

        assertEquals("NOT EXISTS (SELECT 1 FROM users WHERE id = ?)", sql)
    }

    @Test
    fun `ExistsPredicate with empty params`() {
        val predicate = ExistsPredicate(
            subquerySql = "SELECT 1 FROM users",
            subqueryParams = emptyList()
        )
        val params = mutableListOf<Any?>()

        val sql = predicate.toSql(dialect, params)

        assertEquals("EXISTS (SELECT 1 FROM users)", sql)
        assertTrue(params.isEmpty())
    }

    // ============== NotPredicate Tests ==============

    @Test
    fun `NotPredicate toSql wraps predicate with NOT`() {
        val innerPredicate = TruePredicate
        val notPredicate = NotPredicate(innerPredicate)
        val params = mutableListOf<Any?>()

        val sql = notPredicate.toSql(dialect, params)

        assertEquals("NOT (1 = 1)", sql)
    }

    @Test
    fun `NotPredicate double negation returns original`() {
        val original = TruePredicate
        val notPredicate = NotPredicate(original)
        val doubleNot = notPredicate.not()

        assertEquals(original, doubleNot)
    }

    @Test
    fun `NotPredicate toString wraps predicate`() {
        val notPredicate = NotPredicate(TruePredicate)
        assertEquals("NOT (TRUE)", notPredicate.toString())
    }

    // ============== AndPredicate/OrPredicate Edge Cases ==============

    @Test
    fun `AndPredicate with empty conditions returns 1 = 1`() {
        val predicate = AndPredicate(emptyList())
        val params = mutableListOf<Any?>()

        assertEquals("1 = 1", predicate.toSql(dialect, params))
    }

    @Test
    fun `OrPredicate with empty conditions returns 1 = 0`() {
        val predicate = OrPredicate(emptyList())
        val params = mutableListOf<Any?>()

        assertEquals("1 = 0", predicate.toSql(dialect, params))
    }

    @Test
    fun `AndPredicate with single condition returns unwrapped`() {
        val inner = TruePredicate
        val predicate = AndPredicate(listOf(inner))
        val params = mutableListOf<Any?>()

        assertEquals("1 = 1", predicate.toSql(dialect, params))
    }

    @Test
    fun `OrPredicate with single condition returns unwrapped`() {
        val inner = TruePredicate
        val predicate = OrPredicate(listOf(inner))
        val params = mutableListOf<Any?>()

        assertEquals("1 = 1", predicate.toSql(dialect, params))
    }

    @Test
    fun `AndPredicate chaining with and appends conditions`() {
        val predicate = AndPredicate(listOf(TruePredicate))
        val result = predicate.and(FalsePredicate)

        assertTrue(result is AndPredicate)
        val andPredicate = result as AndPredicate
        assertEquals(2, andPredicate.conditions.size)
    }

    @Test
    fun `OrPredicate chaining with or appends conditions`() {
        val predicate = OrPredicate(listOf(TruePredicate))
        val result = predicate.or(FalsePredicate)

        assertTrue(result is OrPredicate)
        val orPredicate = result as OrPredicate
        assertEquals(2, orPredicate.conditions.size)
    }
}
