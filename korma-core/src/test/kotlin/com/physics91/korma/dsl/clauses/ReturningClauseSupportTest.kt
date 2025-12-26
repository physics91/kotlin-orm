package com.physics91.korma.dsl.clauses

import com.physics91.korma.fixtures.TestUsers
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReturningClauseSupportTest {

    // Simple implementation for testing
    class TestReturningBuilder : ReturningClauseSupport<TestReturningBuilder> {
        override var returningColumns: List<Column<*>> = emptyList()
        override val table: Table = TestUsers
    }

    @Test
    fun `returning with vararg columns sets returningColumns`() {
        val builder = TestReturningBuilder()

        builder.returning(TestUsers.id, TestUsers.name)

        assertEquals(2, builder.returningColumns.size)
        assertEquals("id", builder.returningColumns[0].name)
        assertEquals("name", builder.returningColumns[1].name)
    }

    @Test
    fun `returning with single column`() {
        val builder = TestReturningBuilder()

        builder.returning(TestUsers.id)

        assertEquals(1, builder.returningColumns.size)
        assertEquals("id", builder.returningColumns[0].name)
    }

    @Test
    fun `returning with list sets returningColumns`() {
        val builder = TestReturningBuilder()
        val columns = listOf<Column<*>>(TestUsers.id, TestUsers.name, TestUsers.email)

        builder.returning(columns)

        assertEquals(3, builder.returningColumns.size)
    }

    @Test
    fun `returningAll sets all table columns`() {
        val builder = TestReturningBuilder()

        builder.returningAll()

        // TestUsers table has id, name, email, age, active columns
        assertTrue(builder.returningColumns.isNotEmpty())
        assertEquals(TestUsers.columns.size, builder.returningColumns.size)
    }

    @Test
    fun `self returns builder instance`() {
        val builder = TestReturningBuilder()

        assertEquals(builder, builder.self())
    }

    @Test
    fun `chained returning calls override previous`() {
        val builder = TestReturningBuilder()

        builder.returning(TestUsers.id)
        assertEquals(1, builder.returningColumns.size)

        builder.returning(TestUsers.name, TestUsers.email)
        assertEquals(2, builder.returningColumns.size)
    }

    @Test
    fun `empty returning list is allowed`() {
        val builder = TestReturningBuilder()

        builder.returning(emptyList())

        assertTrue(builder.returningColumns.isEmpty())
    }
}
