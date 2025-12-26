package com.physics91.korma.spring

import com.physics91.korma.schema.Table
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for SpringTransactionManager.
 */
class SpringTransactionManagerTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DataSourceAutoConfiguration::class.java,
                DataSourceTransactionManagerAutoConfiguration::class.java,
                KormaAutoConfiguration::class.java,
                KormaRepositoryConfiguration::class.java
            )
        )
        .withPropertyValues(
            "spring.datasource.url=jdbc:h2:mem:txtest${System.nanoTime()};DB_CLOSE_DELAY=-1",
            "spring.datasource.driver-class-name=org.h2.Driver"
        )

    @Test
    fun `should create SpringTransactionManager bean`() {
        contextRunner.run { context ->
            assertNotNull(context.getBean(SpringTransactionManager::class.java))
        }
    }

    @Test
    fun `should execute transaction`() {
        val TxTable = object : Table("tx_test") {
            val id = long("id").primaryKey().autoIncrement()
            val value = varchar("value", 100).notNull()
        }

        contextRunner.run { context ->
            val txManager = context.getBean(SpringTransactionManager::class.java)
            val template = context.getBean(KormaTemplate::class.java)

            // Create table
            template.createTable(TxTable)

            // Execute in transaction
            val result = txManager.transaction {
                template.insert(TxTable) {
                    it[TxTable.value] = "test"
                }
                template.count(TxTable)
            }

            assertEquals(1, result)

            // Cleanup
            template.dropTable(TxTable)
        }
    }

    @Test
    fun `should check if transaction is active`() {
        contextRunner.run { context ->
            val txManager = context.getBean(SpringTransactionManager::class.java)

            // Outside transaction
            assertFalse(txManager.isTransactionActive())

            // Inside transaction
            txManager.transaction {
                assertTrue(txManager.isTransactionActive())
            }

            // After transaction
            assertFalse(txManager.isTransactionActive())
        }
    }

    @Test
    fun `should get connection within transaction`() {
        contextRunner.run { context ->
            val txManager = context.getBean(SpringTransactionManager::class.java)

            txManager.transaction {
                val conn = txManager.getConnection()
                assertNotNull(conn)
                assertFalse(conn.isClosed)
                // Don't manually close - let Spring manage it
                txManager.releaseConnection(conn)
            }
        }
    }

    @Test
    fun `should commit transaction successfully`() {
        val CommitTable = object : Table("commit_test") {
            val id = long("id").primaryKey().autoIncrement()
            val value = varchar("value", 100).notNull()
        }

        contextRunner.run { context ->
            val txManager = context.getBean(SpringTransactionManager::class.java)
            val template = context.getBean(KormaTemplate::class.java)

            // Create table
            template.createTable(CommitTable)

            // Execute and commit
            txManager.transaction {
                template.insert(CommitTable) {
                    it[CommitTable.value] = "committed"
                }
            }

            // Verify committed
            assertEquals(1, template.count(CommitTable))

            // Cleanup
            template.dropTable(CommitTable)
        }
    }

    @Test
    fun `should use mandatory propagation`() {
        contextRunner.run { context ->
            val txManager = context.getBean(SpringTransactionManager::class.java)

            // Mandatory should fail without existing transaction
            var caughtException = false
            try {
                txManager.mandatory {
                    // This should fail
                }
            } catch (e: Exception) {
                caughtException = true
            }
            assertTrue(caughtException)
        }
    }
}
