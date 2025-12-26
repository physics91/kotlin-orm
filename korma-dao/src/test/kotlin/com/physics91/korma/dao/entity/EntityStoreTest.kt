package com.physics91.korma.dao.entity

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.sql.BaseSqlDialect
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach

/**
 * Tests for EntityStore operations.
 */
class EntityStoreTest {

    private lateinit var store: EntityStore
    private val dialect = object : BaseSqlDialect() {
        override val name = "test"
        override val supportsReturning: Boolean = true
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    @BeforeEach
    fun setup() {
        store = EntityStore(dialect)
    }

    // ============== Test Entity Implementations ==============

    object Users : LongEntityTable<User>("users") {
        val name = varchar("name", 100)
        val email = varchar("email", 255)

        override fun createEntity() = User()
        override fun entityToMap(entity: User): Map<Column<*>, Any?> = mapOf(
            name to entity.name,
            email to entity.email
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: User) {
            entity.name = row[name] as String
            entity.email = row[email] as String
        }
    }

    class User(id: Long = 0L) : LongEntity(id) {
        var name: String = ""
        var email: String = ""
        override val entityTable get() = Users

        companion object : LongEntityClass<User>(Users)
    }

    // ============== Persist Tests ==============

    @Test
    fun `persist generates INSERT SQL`() {
        val user = User.new {
            name = "John"
            email = "john@example.com"
        }

        val sql = store.persist(user)

        assertTrue(sql.sql.contains("INSERT INTO"))
        assertTrue(sql.sql.contains("\"users\""))
        assertEquals(2, sql.params.size)
        assertTrue(sql.params.contains("John"))
        assertTrue(sql.params.contains("john@example.com"))
    }

    @Test
    fun `persistReturning generates INSERT with RETURNING`() {
        val user = User.new {
            name = "John"
            email = "john@example.com"
        }

        val sql = store.persistReturning(user, Users.id)

        assertTrue(sql.sql.contains("INSERT INTO"))
        assertTrue(sql.sql.contains("RETURNING"))
        assertTrue(sql.sql.contains("\"id\""))
    }

    // ============== Update Tests ==============

    @Test
    fun `update generates UPDATE SQL with WHERE clause`() {
        val user = User(id = 1L).apply {
            name = "Jane"
            email = "jane@example.com"
        }

        val sql = store.update(user)

        assertTrue(sql.sql.contains("UPDATE"))
        assertTrue(sql.sql.contains("\"users\""))
        assertTrue(sql.sql.contains("SET"))
        assertTrue(sql.sql.contains("WHERE"))
        assertTrue(sql.params.contains(1L))
    }

    @Test
    fun `update throws for new entity`() {
        val user = User.new {
            name = "John"
        }

        assertThrows(IllegalArgumentException::class.java) {
            store.update(user)
        }
    }

    // ============== Delete Tests ==============

    @Test
    fun `delete generates DELETE SQL`() {
        val user = User(id = 42L)

        val sql = store.delete(user)

        assertTrue(sql.sql.contains("DELETE FROM"))
        assertTrue(sql.sql.contains("\"users\""))
        assertTrue(sql.sql.contains("WHERE"))
        assertTrue(sql.params.contains(42L))
    }

    @Test
    fun `delete throws for new entity`() {
        val user = User.new()

        assertThrows(IllegalArgumentException::class.java) {
            store.delete(user)
        }
    }

    @Test
    fun `deleteById generates DELETE SQL`() {
        val sql = store.deleteById(Users, 99L)

        assertTrue(sql.sql.contains("DELETE FROM"))
        assertTrue(sql.sql.contains("WHERE"))
        assertTrue(sql.params.contains(99L))
    }

    // ============== Query Building Tests ==============

    @Test
    fun `buildSelect generates SELECT SQL`() {
        val query = User.all()
        val sql = store.buildSelect(query)

        assertTrue(sql.sql.contains("SELECT"))
        assertTrue(sql.sql.contains("*"))
        assertTrue(sql.sql.contains("FROM"))
        assertTrue(sql.sql.contains("\"users\""))
    }

    @Test
    fun `buildSelect with where generates correct SQL`() {
        val query = User.find { ColumnExpression(Users.name) eq "John" }
        val sql = store.buildSelect(query)

        assertTrue(sql.sql.contains("WHERE"))
        assertTrue(sql.params.contains("John"))
    }

    @Test
    fun `buildCount generates COUNT SQL`() {
        val query = User.count()
        val sql = store.buildCount(query)

        assertTrue(sql.sql.contains("SELECT"))
        assertTrue(sql.sql.contains("COUNT"))
        assertTrue(sql.sql.contains("FROM"))
    }

    @Test
    fun `buildExists generates SELECT with LIMIT`() {
        val query = User.exists { ColumnExpression(Users.email) eq "test@example.com" }
        val sql = store.buildExists(query)

        assertTrue(sql.sql.contains("SELECT"))
        assertTrue(sql.sql.contains("WHERE"))
        assertTrue(sql.sql.contains("LIMIT 1") || sql.sql.contains("FETCH FIRST 1"))
    }

    // ============== Row Mapping Tests ==============

    @Test
    fun `createFromRow creates entity with values`() {
        val row = mapOf<Column<*>, Any?>(
            Users.id to 1L,
            Users.name to "John",
            Users.email to "john@example.com"
        )

        val user = store.createFromRow(Users, row)

        assertEquals(1L, user.id)
        assertEquals("John", user.name)
        assertEquals("john@example.com", user.email)
        assertFalse(user.isNew)
    }
}
