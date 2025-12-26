package com.physics91.korma.jdbc

import com.physics91.korma.dialect.mysql.MySqlDialect
import com.physics91.korma.dsl.*
import com.physics91.korma.schema.Table
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import java.sql.DriverManager

/**
 * Integration test against a real MySQL instance.
 *
 * Uses env overrides when provided:
 * - KORMA_MYSQL_HOST (default: localhost)
 * - KORMA_MYSQL_PORT (default: 3306)
 * - KORMA_MYSQL_DB   (default: korma_test)
 * - KORMA_MYSQL_USER (default: root)
 * - KORMA_MYSQL_PASSWORD (default: 1q2w3e4r)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MySqlIntegrationTest {

    private val host = System.getenv("KORMA_MYSQL_HOST") ?: "localhost"
    private val port = System.getenv("KORMA_MYSQL_PORT") ?: "3306"
    private val dbName = System.getenv("KORMA_MYSQL_DB") ?: "korma_test"
    private val user = System.getenv("KORMA_MYSQL_USER") ?: "root"
    private val password = System.getenv("KORMA_MYSQL_PASSWORD") ?: "1q2w3e4r"

    private val baseUrl =
        "jdbc:mysql://$host:$port/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    private val dbUrl =
        "jdbc:mysql://$host:$port/$dbName?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"

    private lateinit var database: JdbcDatabase

    object MySqlUsers : Table("mysql_users") {
        val id = long("id").primaryKey().autoIncrement()
        val name = varchar("name", 100).notNull()
        val email = varchar("email", 255).unique().notNull()
        val age = integer("age").nullable()
    }

    @BeforeAll
    fun setup() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver")

            DriverManager.getConnection(baseUrl, user, password).use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE DATABASE IF NOT EXISTS `$dbName`")
                }
            }

            database = JdbcDatabase(
                DatabaseConfig(
                    jdbcUrl = dbUrl,
                    username = user,
                    password = password,
                    driverClassName = "com.mysql.cj.jdbc.Driver"
                ),
                MySqlDialect
            )

            // Clean slate
            database.dropTable(MySqlUsers, ifExists = true)
            database.createTable(MySqlUsers, ifNotExists = true)
        } catch (e: Exception) {
            e.printStackTrace()
            Assumptions.assumeTrue(false, "MySQL not available: ${e.message}")
        }
    }

    @AfterAll
    fun teardown() {
        if (::database.isInitialized) {
            try {
                database.dropTable(MySqlUsers, ifExists = true)
                database.close()
            } finally {
                DriverManager.getConnection(baseUrl, user, password).use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.execute("DROP DATABASE IF EXISTS `$dbName`")
                    }
                }
            }
        }
    }

    @BeforeEach
    fun clearTable() {
        if (::database.isInitialized) {
            database.executeRaw("DELETE FROM `${MySqlUsers.tableName}`")
        }
    }

    @Test
    fun `should connect and create table`() {
        assertNotNull(database)
        assertTrue(database.tableExists(MySqlUsers))
    }

    @Test
    fun `should perform CRUD with real MySQL`() {
        val id = database.transaction {
            insertInto(MySqlUsers) {
                this[MySqlUsers.name] = "Alice"
                this[MySqlUsers.email] = "alice@example.com"
                this[MySqlUsers.age] = 30
            }.executeAndGetId()
        }

        val name = database.transaction {
            from(MySqlUsers)
                .select(MySqlUsers.name)
                .where { MySqlUsers.id eq id }
                .fetchSingle { row -> row[MySqlUsers.name] }
        }
        assertEquals("Alice", name)

        database.transaction {
            update(MySqlUsers)
                .set(MySqlUsers.age, 31)
                .where { MySqlUsers.id eq id }
                .execute()
        }

        val age = database.transaction {
            from(MySqlUsers)
                .select(MySqlUsers.age)
                .where { MySqlUsers.id eq id }
                .fetchSingle { row -> row[MySqlUsers.age] }
        }
        assertEquals(31, age)

        database.transaction {
            deleteFrom(MySqlUsers)
                .where { MySqlUsers.id eq id }
                .execute()
        }

        val exists = database.transaction {
            from(MySqlUsers)
                .selectAll()
                .where { MySqlUsers.id eq id }
                .exists()
        }
        assertFalse(exists)
    }
}
