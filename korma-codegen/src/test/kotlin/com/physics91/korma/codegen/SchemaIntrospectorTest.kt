package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaIntrospectorTest {

    private lateinit var connection: Connection
    private val dbUrl = "jdbc:h2:mem:testdb_introspect;DB_CLOSE_DELAY=-1"

    @BeforeEach
    fun setup() {
        connection = DriverManager.getConnection(dbUrl)
        createTestSchema()
    }

    @AfterEach
    fun tearDown() {
        connection.createStatement().execute("DROP ALL OBJECTS")
        connection.close()
    }

    private fun createTestSchema() {
        connection.createStatement().execute("""
            CREATE TABLE users (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                CONSTRAINT uk_email UNIQUE (email)
            )
        """.trimIndent())

        connection.createStatement().execute("""
            CREATE TABLE orders (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL,
                order_date DATE NOT NULL,
                status VARCHAR(20) DEFAULT 'PENDING',
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """.trimIndent())

        connection.createStatement().execute("""
            CREATE TABLE order_items (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                order_id BIGINT NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                quantity INT NOT NULL,
                unit_price DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id)
            )
        """.trimIndent())

        connection.createStatement().execute("""
            CREATE INDEX idx_orders_user_id ON orders(user_id)
        """.trimIndent())

        connection.createStatement().execute("""
            CREATE INDEX idx_orders_date ON orders(order_date)
        """.trimIndent())
    }

    private fun createConfig(
        includePatterns: List<String> = listOf(".*"),
        excludePatterns: List<String> = emptyList()
    ): CodegenConfig {
        return CodegenConfig(
            packageName = "com.example.generated",
            databaseConfig = DatabaseConfig(url = dbUrl),
            includePatterns = includePatterns.map { Regex(it) },
            excludePatterns = excludePatterns.map { Regex(it) }
        )
    }

    @Test
    fun `introspects all tables`() {
        val config = createConfig()
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()

        assertEquals(3, schema.tables.size)
        assertNotNull(schema.getTable("USERS"))
        assertNotNull(schema.getTable("ORDERS"))
        assertNotNull(schema.getTable("ORDER_ITEMS"))
    }

    @Test
    fun `introspects table columns`() {
        val config = createConfig()
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()
        val usersTable = schema.getTable("USERS")!!

        assertEquals(5, usersTable.columns.size)

        val idColumn = usersTable.columns.find { it.name == "ID" }!!
        assertEquals(false, idColumn.nullable)
        assertEquals(true, idColumn.autoIncrement)

        val usernameColumn = usersTable.columns.find { it.name == "USERNAME" }!!
        assertEquals(false, usernameColumn.nullable)
        assertEquals(50, usernameColumn.size)

        val createdAtColumn = usersTable.columns.find { it.name == "CREATED_AT" }!!
        assertEquals(true, createdAtColumn.nullable)
    }

    @Test
    fun `introspects primary keys`() {
        val config = createConfig()
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()
        val usersTable = schema.getTable("USERS")!!

        assertTrue(usersTable.hasPrimaryKey)
        assertFalse(usersTable.hasCompositePrimaryKey)
        assertEquals(listOf("ID"), usersTable.primaryKeyColumns)
    }

    @Test
    fun `introspects foreign keys`() {
        val config = createConfig()
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()
        val ordersTable = schema.getTable("ORDERS")!!

        assertEquals(1, ordersTable.foreignKeys.size)
        val fk = ordersTable.foreignKeys.first()

        assertEquals(listOf("USER_ID"), fk.localColumns)
        assertEquals("USERS", fk.foreignTable)
        assertEquals(listOf("ID"), fk.foreignColumns)
        assertTrue(fk.isSimple)
    }

    @Test
    fun `introspects indices`() {
        val config = createConfig()
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()
        val ordersTable = schema.getTable("ORDERS")!!

        val nonPkIndices = ordersTable.indices.filter { !it.name.contains("PRIMARY") }
        assertTrue(nonPkIndices.isNotEmpty())
    }

    @Test
    fun `respects include patterns`() {
        val config = createConfig(includePatterns = listOf("USERS"))
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()

        assertEquals(1, schema.tables.size)
        assertNotNull(schema.getTable("USERS"))
    }

    @Test
    fun `respects exclude patterns`() {
        val config = createConfig(excludePatterns = listOf("ORDER.*"))
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()

        assertEquals(1, schema.tables.size)
        assertNotNull(schema.getTable("USERS"))
    }

    @Test
    fun `combines include and exclude patterns`() {
        val config = createConfig(
            includePatterns = listOf("ORDER.*"),
            excludePatterns = listOf("ORDER_ITEMS")
        )
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()

        assertEquals(1, schema.tables.size)
        assertNotNull(schema.getTable("ORDERS"))
    }

    @Test
    fun `returns empty schema when no tables match`() {
        val config = createConfig(includePatterns = listOf("NONEXISTENT"))
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()

        assertTrue(schema.tables.isEmpty())
    }

    @Test
    fun `introspects unique constraints`() {
        val config = createConfig()
        val introspector = SchemaIntrospector(config)

        val schema = introspector.introspect()
        val usersTable = schema.getTable("USERS")!!

        val uniqueConstraints = usersTable.uniqueConstraints.filter { it.name.contains("UK_EMAIL") }
        assertTrue(uniqueConstraints.isNotEmpty())
        assertTrue(uniqueConstraints.any { it.columns.contains("EMAIL") })
    }
}
