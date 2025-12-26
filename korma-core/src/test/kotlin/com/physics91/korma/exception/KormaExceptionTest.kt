package com.physics91.korma.exception

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class KormaExceptionTest {

    @Nested
    inner class QueryExceptionTests {

        @Test
        fun `QueryBuildException should have correct error code`() {
            val exception = QueryBuildException(
                message = "Invalid column name",
                sql = "SELECT * FROM users WHERE invalid = ?",
                params = listOf("value")
            )

            exception.errorCode shouldBe "KORMA-Q001"
            exception.isRecoverable shouldBe false
            exception.sql shouldBe "SELECT * FROM users WHERE invalid = ?"
            exception.params shouldBe listOf("value")
        }

        @Test
        fun `QueryExecutionException should include SQL state`() {
            val exception = QueryExecutionException(
                message = "Unique constraint violation",
                sql = "INSERT INTO users (email) VALUES (?)",
                params = listOf("test@example.com"),
                sqlState = "23505",
                vendorCode = 0
            )

            exception.errorCode shouldBe "KORMA-Q002"
            exception.sqlState shouldBe "23505"
            exception.toString() shouldContain "SQLState: 23505"
        }

        @Test
        fun `QueryTimeoutException should be recoverable`() {
            val exception = QueryTimeoutException(
                message = "Query timed out",
                timeoutMillis = 5000L
            )

            exception.errorCode shouldBe "KORMA-Q003"
            exception.isRecoverable shouldBe true
            exception.timeoutMillis shouldBe 5000L
        }

        @Test
        fun `NoResultException should have correct error code`() {
            val exception = NoResultException(
                message = "No result found",
                sql = "SELECT * FROM users WHERE id = ?",
                params = listOf(999L)
            )

            exception.errorCode shouldBe "KORMA-Q004"
            exception.isRecoverable shouldBe false
        }

        @Test
        fun `NonUniqueResultException should include actual count`() {
            val exception = NonUniqueResultException(
                message = "Expected single result",
                sql = "SELECT * FROM users",
                actualCount = 5
            )

            exception.errorCode shouldBe "KORMA-Q005"
            exception.actualCount shouldBe 5
        }
    }

    @Nested
    inner class TransactionExceptionTests {

        @Test
        fun `DeadlockException should be recoverable`() {
            val exception = DeadlockException(
                message = "Deadlock detected",
                involvedTables = listOf("users", "orders")
            )

            exception.errorCode shouldBe "KORMA-T002"
            exception.isRecoverable shouldBe true
            exception.involvedTables shouldBe listOf("users", "orders")
        }

        @Test
        fun `OptimisticLockException should include version info`() {
            val exception = OptimisticLockException(
                message = "Optimistic lock failed",
                tableName = "users",
                expectedVersion = 5L,
                actualVersion = 6L
            )

            exception.errorCode shouldBe "KORMA-T003"
            exception.isRecoverable shouldBe true
            exception.expectedVersion shouldBe 5L
            exception.actualVersion shouldBe 6L
        }

        @Test
        fun `TransactionRollbackException should not be recoverable`() {
            val originalCause = RuntimeException("Original error")
            val exception = TransactionRollbackException(
                message = "Transaction rolled back",
                cause = originalCause,
                rollbackCause = RuntimeException("Rollback failed")
            )

            exception.errorCode shouldBe "KORMA-T001"
            exception.isRecoverable shouldBe false
            exception.cause shouldBe originalCause
            exception.rollbackCause shouldNotBe null
        }
    }

    @Nested
    inner class ConnectionExceptionTests {

        @Test
        fun `ConnectionAcquisitionException should be recoverable`() {
            val exception = ConnectionAcquisitionException(
                message = "Cannot acquire connection",
                waitTimeMillis = 30000L
            )

            exception.errorCode shouldBe "KORMA-C001"
            exception.isRecoverable shouldBe true
            exception.waitTimeMillis shouldBe 30000L
        }

        @Test
        fun `ConnectionPoolExhaustedException should include pool info`() {
            val exception = ConnectionPoolExhaustedException(
                message = "Connection pool exhausted",
                activeConnections = 10,
                maxPoolSize = 10
            )

            exception.errorCode shouldBe "KORMA-C002"
            exception.activeConnections shouldBe 10
            exception.maxPoolSize shouldBe 10
        }

        @Test
        fun `ConnectionLostException should be recoverable`() {
            val exception = ConnectionLostException(
                message = "Connection lost"
            )

            exception.errorCode shouldBe "KORMA-C003"
            exception.isRecoverable shouldBe true
        }
    }

    @Nested
    inner class CacheExceptionTests {

        @Test
        fun `CacheInitializationException should not be recoverable`() {
            val exception = CacheInitializationException(
                message = "Failed to initialize cache"
            )

            exception.errorCode shouldBe "KORMA-CA001"
            exception.isRecoverable shouldBe false
        }

        @Test
        fun `CacheOperationException should include operation details`() {
            val exception = CacheOperationException(
                message = "Cache get failed",
                operation = "GET",
                key = "user:123"
            )

            exception.errorCode shouldBe "KORMA-CA002"
            exception.isRecoverable shouldBe true
            exception.operation shouldBe "GET"
            exception.key shouldBe "user:123"
        }
    }

    @Nested
    inner class MigrationExceptionTests {

        @Test
        fun `MigrationScriptException should include script info`() {
            val exception = MigrationScriptException(
                message = "Migration failed",
                scriptName = "V001__create_users.sql",
                version = "001"
            )

            exception.errorCode shouldBe "KORMA-M001"
            exception.isRecoverable shouldBe false
            exception.scriptName shouldBe "V001__create_users.sql"
            exception.version shouldBe "001"
        }

        @Test
        fun `MigrationValidationException should include checksum info`() {
            val exception = MigrationValidationException(
                message = "Checksum mismatch",
                expectedChecksum = "abc123",
                actualChecksum = "xyz789"
            )

            exception.errorCode shouldBe "KORMA-M002"
            exception.expectedChecksum shouldBe "abc123"
            exception.actualChecksum shouldBe "xyz789"
        }
    }

    @Nested
    inner class SealedClassExhaustivenessTests {

        @Test
        fun `when expression should be exhaustive for KormaException`() {
            val exceptions: List<KormaException> = listOf(
                QueryBuildException("test"),
                QueryExecutionException("test"),
                QueryTimeoutException("test"),
                NoResultException("test"),
                NonUniqueResultException("test"),
                TransactionRollbackException("test"),
                DeadlockException("test"),
                OptimisticLockException("test"),
                TransactionStartException("test"),
                TransactionCommitException("test"),
                ConnectionAcquisitionException("test"),
                ConnectionPoolExhaustedException("test"),
                ConnectionLostException("test"),
                ConnectionValidationException("test"),
                CacheInitializationException("test"),
                CacheOperationException("test"),
                MigrationScriptException("test"),
                MigrationValidationException("test"),
                MigrationOrderException("test"),
                ConfigurationException("test"),
                MappingException("test")
            )

            exceptions.forEach { exception ->
                val category = when (exception) {
                    is QueryException -> "query"
                    is TransactionException -> "transaction"
                    is ConnectionException -> "connection"
                    is CacheException -> "cache"
                    is MigrationException -> "migration"
                    is ConfigurationException -> "configuration"
                    is MappingException -> "mapping"
                }
                category shouldNotBe null
            }
        }
    }
}
