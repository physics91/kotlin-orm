package com.physics91.korma.r2dbc

import com.physics91.korma.dsl.InsertBuilder
import com.physics91.korma.dsl.SelectBuilder
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*

/**
 * Integration tests for R2dbcDatabase.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R2dbcDatabaseTest {

    private lateinit var database: R2dbcDatabase

    private val dialect = object : BaseSqlDialect() {
        override val name = "H2"
        override val supportsReturning = true
        override fun autoIncrementType(baseType: ColumnType<*>): String = "BIGINT AUTO_INCREMENT"
    }

    // Test table definition
    object Users : Table("users") {
        val id = long("id").primaryKey().autoIncrement()
        val name = varchar("name", 100)
        val email = varchar("email", 255)
        val age = integer("age").nullable()
    }

    @BeforeAll
    fun setup() {
        val options = R2dbcConnectionFactory.options()
            .h2Mem("testdb")
            .option("DB_CLOSE_DELAY", "-1")

        database = R2dbcDatabase.create(options, dialect)

        runBlocking {
            database.createTable(Users)
        }
    }

    @AfterAll
    fun teardown() {
        runBlocking {
            database.dropTable(Users)
        }
        database.close()
    }

    @BeforeEach
    fun clearTable() {
        runBlocking {
            database.execute("DELETE FROM \"users\"")
        }
    }

    // ============== Connection Tests ==============

    @Test
    fun `should create database with connection factory`() = runTest {
        assertNotNull(database)
    }

    @Test
    fun `should get pool metrics`() {
        val metrics = database.getPoolMetrics()
        assertNotNull(metrics)
    }

    // ============== Insert Tests ==============

    @Test
    fun `should insert single row`() = runTest {
        val builder = InsertBuilder(Users).apply {
            set(Users.name, "John")
            set(Users.email, "john@example.com")
            set(Users.age, 25)
        }

        val result = database.insert(builder)
        assertEquals(1L, result)
    }

    @Test
    fun `should insert and query`() = runTest {
        // Insert
        val insertBuilder = InsertBuilder(Users).apply {
            set(Users.name, "Jane")
            set(Users.email, "jane@example.com")
            set(Users.age, 30)
        }
        database.insert(insertBuilder)

        // Query
        val results = database.query("SELECT * FROM \"users\" WHERE \"name\" = ?", "Jane") { row ->
            mapOf(
                "name" to row.get("name", String::class.java),
                "email" to row.get("email", String::class.java),
                "age" to (row.get("age", Any::class.java) as? Number)?.toInt()
            )
        }

        assertEquals(1, results.size)
        assertEquals("Jane", results[0]["name"])
        assertEquals("jane@example.com", results[0]["email"])
        assertEquals(30, results[0]["age"])
    }

    // ============== Transaction Tests ==============

    @Test
    fun `should commit transaction`() = runTest {
        database.transaction {
            executor.execute("INSERT INTO \"users\" (\"name\", \"email\") VALUES (?, ?)", listOf("Alice", "alice@example.com"))
        }

        val count = database.query("SELECT COUNT(*) as cnt FROM \"users\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(1L, count[0])
    }

    @Test
    fun `should rollback transaction on exception`() = runTest {
        try {
            database.transaction {
                executor.execute("INSERT INTO \"users\" (\"name\", \"email\") VALUES (?, ?)", listOf("Bob", "bob@example.com"))
                throw RuntimeException("Test rollback")
            }
        } catch (e: RuntimeException) {
            // Expected
        }

        val count = database.query("SELECT COUNT(*) as cnt FROM \"users\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(0L, count[0])
    }

    // ============== Flow Tests ==============

    @Test
    fun `should stream results with Flow`() = runTest {
        // Insert multiple rows
        repeat(5) { i ->
            database.execute("INSERT INTO \"users\" (\"name\", \"email\") VALUES (?, ?)", "User$i", "user$i@example.com")
        }

        // Stream results
        val flow = database.queryFlow("SELECT * FROM \"users\"") { row ->
            row.get("name", String::class.java)
        }

        val results = flow.toList()
        assertEquals(5, results.size)
    }

    // ============== Batch Insert Tests ==============

    @Test
    fun `should batch insert`() = runTest {
        val items = listOf(
            "User1" to "user1@example.com",
            "User2" to "user2@example.com",
            "User3" to "user3@example.com"
        )

        val count = database.batchInsert(Users, items) { (name, email) ->
            set(Users.name, name)
            set(Users.email, email)
        }

        assertEquals(3L, count)

        val dbCount = database.query("SELECT COUNT(*) as cnt FROM \"users\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(3L, dbCount[0])
    }

    // ============== Savepoint Tests ==============

    @Test
    fun `should support savepoints`() = runTest {
        database.transaction {
            executor.execute("INSERT INTO \"users\" (\"name\", \"email\") VALUES (?, ?)", listOf("User1", "user1@example.com"))

            val sp = savepoint("sp1")

            executor.execute("INSERT INTO \"users\" (\"name\", \"email\") VALUES (?, ?)", listOf("User2", "user2@example.com"))

            // Rollback to savepoint
            rollbackToSavepoint(sp)

            // Only User1 should remain after commit
        }

        val count = database.query("SELECT COUNT(*) as cnt FROM \"users\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(1L, count[0])
    }

    // ============== Isolation Level Tests ==============

    @Test
    fun `should support different isolation levels`() = runTest {
        database.transaction(TransactionIsolation.SERIALIZABLE) {
            executor.execute("INSERT INTO \"users\" (\"name\", \"email\") VALUES (?, ?)", listOf("Isolated", "isolated@example.com"))
        }

        val count = database.query("SELECT COUNT(*) as cnt FROM \"users\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(1L, count[0])
    }
}
