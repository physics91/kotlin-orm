package com.physics91.korma.r2dbc

import com.physics91.korma.dsl.InsertBuilder
import com.physics91.korma.dsl.SelectBuilder
import com.physics91.korma.dsl.eq
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*

/**
 * Advanced integration tests for R2DBC features including:
 * - DSL extensions
 * - Transaction propagation
 * - Backpressure configuration
 * - Structured operations
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R2dbcAdvancedTest {

    private lateinit var database: R2dbcDatabase

    private val dialect = object : BaseSqlDialect() {
        override val name = "H2"
        override val supportsReturning = true
        override fun autoIncrementType(baseType: ColumnType<*>): String = "BIGINT AUTO_INCREMENT"
    }

    // Test table definition
    object TestItems : Table("test_items") {
        val id = long("id").primaryKey().autoIncrement()
        val name = varchar("name", 100)
        val value = integer("value").nullable()
    }

    @BeforeAll
    fun setup() {
        val options = R2dbcConnectionFactory.options()
            .h2Mem("advancedtestdb")
            .option("DB_CLOSE_DELAY", "-1")

        database = R2dbcDatabase.create(options, dialect)

        runBlocking {
            database.createTable(TestItems)
        }
    }

    @AfterAll
    fun teardown() {
        runBlocking {
            database.dropTable(TestItems)
        }
        database.close()
    }

    @BeforeEach
    fun clearTable() {
        runBlocking {
            database.execute("DELETE FROM \"test_items\"")
        }
    }

    // ============== DSL Extension Tests ==============

    @Test
    fun `should insert with DSL extension`() = runTest {
        val affected = database.insert(TestItems) {
            set(TestItems.name, "TestItem")
            set(TestItems.value, 100)
        }

        assertEquals(1L, affected)
    }

    @Test
    fun `should select with DSL extension`() = runTest {
        // Insert test data
        database.insert(TestItems) {
            set(TestItems.name, "Item1")
            set(TestItems.value, 10)
        }
        database.insert(TestItems) {
            set(TestItems.name, "Item2")
            set(TestItems.value, 20)
        }

        val results = database.select(TestItems, { row ->
            row.get("name", String::class.java)
        }) {
            selectAll()
        }

        assertEquals(2, results.size)
        assertTrue(results.contains("Item1"))
        assertTrue(results.contains("Item2"))
    }

    @Test
    fun `should selectOne with DSL extension`() = runTest {
        database.insert(TestItems) {
            set(TestItems.name, "UniqueItem")
            set(TestItems.value, 42)
        }

        val result = database.selectOne(TestItems, { row ->
            row.get("name", String::class.java)
        }) {
            select(TestItems.name)
            where { TestItems.name eq "UniqueItem" }
        }

        assertEquals("UniqueItem", result)
    }

    @Test
    fun `should update with DSL extension`() = runTest {
        database.insert(TestItems) {
            set(TestItems.name, "OldName")
            set(TestItems.value, 1)
        }

        val affected = database.update(TestItems) {
            set(TestItems.name, "NewName")
            where { TestItems.name eq "OldName" }
        }

        assertEquals(1L, affected)

        val updated = database.selectOne(TestItems, { row ->
            row.get("name", String::class.java)
        }) {
            select(TestItems.name)
        }

        assertEquals("NewName", updated)
    }

    @Test
    fun `should delete with DSL extension`() = runTest {
        database.insert(TestItems) {
            set(TestItems.name, "ToDelete")
            set(TestItems.value, 0)
        }

        val affected = database.delete(TestItems) {
            where { TestItems.name eq "ToDelete" }
        }

        assertEquals(1L, affected)

        val count = database.query("SELECT COUNT(*) as cnt FROM \"test_items\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(0L, count[0])
    }

    // ============== Transaction DSL Tests ==============

    @Test
    fun `should use DSL within transaction`() = runTest {
        database.transaction {
            // Insert using executor
            executor.execute(
                "INSERT INTO \"test_items\" (\"name\", \"value\") VALUES (?, ?)",
                listOf("TxItem", 100)
            )
        }

        val count = database.query("SELECT COUNT(*) as cnt FROM \"test_items\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(1L, count[0])
    }

    // ============== Transaction Propagation Tests ==============

    @Test
    fun `should use REQUIRED propagation with existing transaction`() = runTest {
        database.suspendTransaction(propagation = TransactionPropagation.REQUIRED) {
            executor.execute(
                "INSERT INTO \"test_items\" (\"name\", \"value\") VALUES (?, ?)",
                listOf("RequiredOuter", 1)
            )

            // Nested REQUIRED should join the existing transaction
            database.suspendTransaction(propagation = TransactionPropagation.REQUIRED) {
                executor.execute(
                    "INSERT INTO \"test_items\" (\"name\", \"value\") VALUES (?, ?)",
                    listOf("RequiredInner", 2)
                )
            }
        }

        val count = database.query("SELECT COUNT(*) as cnt FROM \"test_items\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(2L, count[0])
    }

    @Test
    fun `should throw on MANDATORY without existing transaction`() = runTest {
        assertThrows<IllegalStateException> {
            runBlocking {
                database.suspendTransaction(propagation = TransactionPropagation.MANDATORY) {
                    executor.execute(
                        "INSERT INTO \"test_items\" (\"name\", \"value\") VALUES (?, ?)",
                        listOf("MandatoryItem", 1)
                    )
                }
            }
        }
    }

    @Test
    fun `should throw on NEVER with existing transaction`() = runTest {
        assertThrows<IllegalStateException> {
            runBlocking {
                database.suspendTransaction(propagation = TransactionPropagation.REQUIRED) {
                    database.suspendTransaction(propagation = TransactionPropagation.NEVER) {
                        // Should throw
                    }
                }
            }
        }
    }

    // ============== Backpressure Configuration Tests ==============

    @Test
    fun `should create default backpressure config`() {
        val config = BackpressureConfig.DEFAULT

        assertEquals(BackpressureStrategy.BUFFER, config.strategy)
        assertEquals(64, config.bufferCapacity)
        assertEquals(100, config.fetchSize)
    }

    @Test
    fun `should create buffered backpressure config`() {
        val config = BackpressureConfig.buffered(capacity = 128, fetchSize = 200)

        assertEquals(BackpressureStrategy.BUFFER, config.strategy)
        assertEquals(128, config.bufferCapacity)
        assertEquals(200, config.fetchSize)
    }

    @Test
    fun `should create drop-oldest config`() {
        val config = BackpressureConfig.dropOldest(capacity = 32)

        assertEquals(BackpressureStrategy.DROP_OLDEST, config.strategy)
        assertEquals(32, config.bufferCapacity)
    }

    @Test
    fun `should create high-throughput config`() {
        val config = BackpressureConfig.HIGH_THROUGHPUT

        assertEquals(1024, config.bufferCapacity)
        assertEquals(500, config.fetchSize)
    }

    @Test
    fun `should create low-memory config`() {
        val config = BackpressureConfig.LOW_MEMORY

        assertEquals(16, config.bufferCapacity)
        assertEquals(50, config.fetchSize)
    }

    // ============== Batch Operations Tests ==============

    @Test
    fun `should execute batch operations with chunking`() = runTest {
        val items = (1..25).map { "BatchItem$it" to it }
        val batchOps = database.batchOperations(chunkSize = 10)

        val result = batchOps.batchInsert(TestItems, items) { (name, value) ->
            set(TestItems.name, name)
            set(TestItems.value, value)
        }

        assertTrue(result.isFullySuccessful)
        assertEquals(25, result.successCount)
        assertEquals(0, result.failureCount)
        assertEquals(25L, result.totalAffected)

        val count = database.query("SELECT COUNT(*) as cnt FROM \"test_items\"") { row ->
            (row.get(0, Any::class.java) as? Number)?.toLong() ?: 0L
        }
        assertEquals(25L, count[0])
    }

    // ============== TransactionScope Interface Tests ==============

    @Test
    fun `should identify transactional scope`() = runTest {
        database.transaction {
            assertTrue(isTransactional())
            assertTrue(isActive())
        }
    }

    @Test
    fun `should identify non-transactional scope with SUPPORTS`() = runTest {
        database.suspendTransaction(propagation = TransactionPropagation.SUPPORTS) {
            // Without an existing transaction, SUPPORTS runs non-transactionally
            assertFalse(isTransactional())
            assertFalse(isActive())
        }
    }
}
