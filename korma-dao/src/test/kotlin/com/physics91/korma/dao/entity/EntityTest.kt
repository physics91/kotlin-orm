package com.physics91.korma.dao.entity

import com.physics91.korma.schema.Column
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.UUID

/**
 * Tests for Entity base classes.
 */
class EntityTest {

    // ============== Test Entity Implementations ==============

    object TestUsers : LongEntityTable<TestUser>("test_users") {
        val name = varchar("name", 100)
        val email = varchar("email", 255)

        override fun createEntity() = TestUser()
        override fun entityToMap(entity: TestUser): Map<Column<*>, Any?> = mapOf(
            name to entity.name,
            email to entity.email
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: TestUser) {
            entity.name = row[name] as String
            entity.email = row[email] as String
        }
    }

    class TestUser(id: Long = 0L) : LongEntity(id) {
        var name: String = ""
        var email: String = ""
        override val entityTable get() = TestUsers
    }

    object TestProducts : IntEntityTable<TestProduct>("test_products") {
        val name = varchar("name", 100)

        override fun createEntity() = TestProduct()
        override fun entityToMap(entity: TestProduct): Map<Column<*>, Any?> = mapOf(name to entity.name)
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: TestProduct) {
            entity.name = row[name] as String
        }
    }

    class TestProduct(id: Int = 0) : IntEntity(id) {
        var name: String = ""
        override val entityTable get() = TestProducts
    }

    object TestTokens : UUIDEntityTable<TestToken>("test_tokens") {
        val value = varchar("value", 100)

        override fun createEntity() = TestToken()
        override fun entityToMap(entity: TestToken): Map<Column<*>, Any?> = mapOf(value to entity.value)
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: TestToken) {
            entity.value = row[value] as String
        }
    }

    class TestToken(id: UUID = UUID.randomUUID()) : UUIDEntity(id) {
        var value: String = ""
        override val entityTable get() = TestTokens
    }

    object TestConfigs : StringEntityTable<TestConfig>("test_configs") {
        val value = varchar("value", 500)

        override fun createEntity() = TestConfig()
        override fun entityToMap(entity: TestConfig): Map<Column<*>, Any?> = mapOf(value to entity.value)
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: TestConfig) {
            entity.value = row[value] as String
        }
    }

    class TestConfig(id: String = "") : StringEntity(id) {
        var value: String = ""
        override val entityTable get() = TestConfigs
    }

    // ============== LongEntity Tests ==============

    @Test
    fun `LongEntity isNew returns true for new entity`() {
        val user = TestUser()
        assertTrue(user.isNew)
        assertEquals(0L, user.id)
    }

    @Test
    fun `LongEntity isNew returns false for persisted entity`() {
        val user = TestUser(id = 123L)
        assertFalse(user.isNew)
        assertEquals(123L, user.id)
    }

    @Test
    fun `LongEntity equals works correctly`() {
        val user1 = TestUser(id = 1L)
        val user2 = TestUser(id = 1L)
        val user3 = TestUser(id = 2L)
        val newUser = TestUser()

        assertEquals(user1, user2)
        assertNotEquals(user1, user3)
        assertNotEquals(newUser, TestUser()) // New entities are not equal
    }

    @Test
    fun `LongEntity hashCode is consistent`() {
        val user1 = TestUser(id = 1L)
        val user2 = TestUser(id = 1L)

        assertEquals(user1.hashCode(), user2.hashCode())
    }

    @Test
    fun `LongEntity toString contains class name and id`() {
        val user = TestUser(id = 42L)
        assertEquals("TestUser(id=42)", user.toString())
    }

    // ============== IntEntity Tests ==============

    @Test
    fun `IntEntity isNew returns true for new entity`() {
        val product = TestProduct()
        assertTrue(product.isNew)
    }

    @Test
    fun `IntEntity isNew returns false for persisted entity`() {
        val product = TestProduct(id = 10)
        assertFalse(product.isNew)
    }

    // ============== UUIDEntity Tests ==============

    @Test
    fun `UUIDEntity is new by default`() {
        val token = TestToken()
        assertTrue(token.isNew)
        assertNotNull(token.id)
    }

    @Test
    fun `UUIDEntity markPersisted changes isNew`() {
        val token = TestToken()
        assertTrue(token.isNew)

        token.markPersisted()
        assertFalse(token.isNew)
    }

    @Test
    fun `UUIDEntity equals uses id`() {
        val id = UUID.randomUUID()
        val token1 = TestToken(id)
        val token2 = TestToken(id)

        assertEquals(token1, token2)
    }

    // ============== StringEntity Tests ==============

    @Test
    fun `StringEntity isNew returns true for empty id`() {
        val config = TestConfig()
        assertTrue(config.isNew)
        assertEquals("", config.id)
    }

    @Test
    fun `StringEntity isNew returns false for non-empty id`() {
        val config = TestConfig(id = "app.setting")
        assertFalse(config.isNew)
    }

    @Test
    fun `StringEntity equals works correctly`() {
        val config1 = TestConfig(id = "key1")
        val config2 = TestConfig(id = "key1")
        val config3 = TestConfig(id = "key2")

        assertEquals(config1, config2)
        assertNotEquals(config1, config3)
    }

    // ============== EntityTable Tests ==============

    @Test
    fun `EntityTable has correct table name`() {
        assertEquals("test_users", TestUsers.tableName)
        assertEquals("test_products", TestProducts.tableName)
    }

    @Test
    fun `EntityTable has primary key column`() {
        assertEquals("id", TestUsers.id.name)
        assertEquals("id", TestProducts.id.name)
        assertEquals("id", TestTokens.id.name)
    }

    @Test
    fun `EntityTable can create entities`() {
        val user = TestUsers.createEntity()
        assertNotNull(user)
        assertTrue(user is TestUser)
        assertTrue(user.isNew)
    }

    @Test
    fun `EntityTable entityToMap extracts properties`() {
        val user = TestUser(id = 1L).apply {
            name = "John"
            email = "john@example.com"
        }

        val map = TestUsers.entityToMap(user)

        assertEquals("John", map[TestUsers.name])
        assertEquals("john@example.com", map[TestUsers.email])
    }

    @Test
    fun `EntityTable mapToEntity populates entity`() {
        val user = TestUser(id = 1L)
        val row = mapOf<Column<*>, Any?>(
            TestUsers.name to "Jane",
            TestUsers.email to "jane@example.com"
        )

        TestUsers.mapToEntity(row, user)

        assertEquals("Jane", user.name)
        assertEquals("jane@example.com", user.email)
    }
}
