package com.physics91.korma.dao.entity

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.schema.Column
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/**
 * Tests for EntityClass and query builders.
 */
class EntityClassTest {

    // ============== Test Entity Implementations ==============

    object Users : LongEntityTable<User>("users") {
        val name = varchar("name", 100)
        val email = varchar("email", 255)
        val age = integer("age").nullable()

        override fun createEntity() = User()
        override fun entityToMap(entity: User): Map<Column<*>, Any?> = mapOf(
            name to entity.name,
            email to entity.email,
            age to entity.age
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: User) {
            entity.name = row[name] as String
            entity.email = row[email] as String
            entity.age = row[age] as Int?
        }
    }

    class User(id: Long = 0L) : LongEntity(id) {
        var name: String = ""
        var email: String = ""
        var age: Int? = null

        override val entityTable get() = Users

        companion object : LongEntityClass<User>(Users)
    }

    // ============== EntityClass Tests ==============

    @Test
    fun `EntityClass new creates entity with initializer`() {
        val user = User.new {
            name = "John"
            email = "john@example.com"
            age = 25
        }

        assertTrue(user.isNew)
        assertEquals("John", user.name)
        assertEquals("john@example.com", user.email)
        assertEquals(25, user.age)
    }

    @Test
    fun `EntityClass new creates entity without initializer`() {
        val user = User.new()

        assertTrue(user.isNew)
        assertEquals("", user.name)
        assertEquals("", user.email)
        assertNull(user.age)
    }

    @Test
    fun `EntityClass table reference is correct`() {
        assertEquals(Users, User.table)
    }

    // ============== EntityQuery Tests ==============

    @Test
    fun `EntityQuery can chain where conditions`() {
        val query = User.find { ColumnExpression(Users.name) eq "John" }
            .andWhere { ColumnExpression(Users.age) gt 18 }

        assertNotNull(query.toSelectBuilder())
    }

    @Test
    fun `EntityQuery can set limit and offset`() {
        val query = User.all()
            .limit(10)
            .offset(20)

        assertNotNull(query.toSelectBuilder())
    }

    @Test
    fun `EntityQuery can paginate`() {
        val query = User.all()
            .paginate(page = 3, pageSize = 25)

        assertNotNull(query.toSelectBuilder())
    }

    @Test
    fun `EntityQuery can order by column`() {
        val query = User.all()
            .orderBy(Users.name)

        assertNotNull(query.toSelectBuilder())
    }

    @Test
    fun `EntityQuery can add forUpdate`() {
        val query = User.all()
            .forUpdate()

        assertNotNull(query.toSelectBuilder())
    }

    @Test
    fun `EntityQuery getTable returns correct table`() {
        val query = User.all()

        assertEquals(Users, query.getTable())
    }

    // ============== CountQuery Tests ==============

    @Test
    fun `CountQuery can be created`() {
        val countQuery = User.count()

        assertNotNull(countQuery)
        assertEquals(Users, countQuery.getTable())
        assertNull(countQuery.getWhere())
    }

    @Test
    fun `CountQuery with predicate stores where clause`() {
        val countQuery = User.count { ColumnExpression(Users.age) gt 18 }

        assertNotNull(countQuery.getWhere())
    }

    // ============== ExistsQuery Tests ==============

    @Test
    fun `ExistsQuery can be created`() {
        val existsQuery = User.exists { ColumnExpression(Users.email) eq "test@example.com" }

        assertNotNull(existsQuery)
        assertEquals(Users, existsQuery.getTable())
        assertNotNull(existsQuery.getWhere())
    }
}
