package com.physics91.korma.resilience

import com.physics91.korma.exception.QueryTimeoutException
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class TimeoutConfigTest {

    @Nested
    inner class ConfigurationTests {

        @Test
        fun `should use default timeout for undefined operations`() {
            val config = TimeoutConfig.create {
                default = 10.seconds
            }

            config.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 10.seconds
            config.forOperation(TimeoutConfig.OperationType.WRITE) shouldBe 10.seconds
        }

        @Test
        fun `should use specific timeout when defined`() {
            val config = TimeoutConfig.create {
                default = 30.seconds
                query = 5.seconds
                transaction = 60.seconds
            }

            config.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 5.seconds
            config.forOperation(TimeoutConfig.OperationType.TRANSACTION) shouldBe 60.seconds
            config.forOperation(TimeoutConfig.OperationType.WRITE) shouldBe 30.seconds // default
        }

        @Test
        fun `should convert to milliseconds correctly`() {
            val config = TimeoutConfig.create {
                query = 5.seconds
            }

            config.forOperationMillis(TimeoutConfig.OperationType.QUERY) shouldBe 5000L
        }

        @Test
        fun `should convert to seconds correctly`() {
            val config = TimeoutConfig.create {
                query = 5.seconds
            }

            config.forOperationSeconds(TimeoutConfig.OperationType.QUERY) shouldBe 5
        }

        @Test
        fun `withTimeout should create new config with override`() {
            val config = TimeoutConfig.create {
                default = 30.seconds
                query = 5.seconds
            }

            val newConfig = config.withTimeout(TimeoutConfig.OperationType.QUERY, 10.seconds)

            config.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 5.seconds
            newConfig.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 10.seconds
        }
    }

    @Nested
    inner class PredefinedConfigTests {

        @Test
        fun `DEFAULT config should have reasonable values`() {
            TimeoutConfig.DEFAULT.forOperation(TimeoutConfig.OperationType.CONNECTION) shouldBe 5.seconds
            TimeoutConfig.DEFAULT.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 30.seconds
            TimeoutConfig.DEFAULT.forOperation(TimeoutConfig.OperationType.TRANSACTION) shouldBe 60.seconds
        }

        @Test
        fun `STRICT config should have shorter timeouts`() {
            TimeoutConfig.STRICT.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 5.seconds
            TimeoutConfig.STRICT.forOperation(TimeoutConfig.OperationType.CONNECTION) shouldBe 2.seconds
        }

        @Test
        fun `LENIENT config should have longer timeouts`() {
            TimeoutConfig.LENIENT.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 120.seconds
            TimeoutConfig.LENIENT.forOperation(TimeoutConfig.OperationType.TRANSACTION) shouldBe 300.seconds
        }
    }

    @Nested
    inner class ExecuteWithTimeoutTests {

        @Test
        fun `should return result when operation completes in time`() {
            val config = TimeoutConfig.create {
                query = 1.seconds
            }

            val result = config.executeWithTimeout(TimeoutConfig.OperationType.QUERY) {
                Thread.sleep(50)
                "success"
            }

            result shouldBe "success"
        }

        @Test
        fun `should throw QueryTimeoutException when operation times out`() {
            val config = TimeoutConfig.create {
                query = 100.milliseconds
            }

            val exception = assertThrows<QueryTimeoutException> {
                config.executeWithTimeout(TimeoutConfig.OperationType.QUERY) {
                    Thread.sleep(500)
                    "should not return"
                }
            }

            exception.timeoutMillis shouldBe 100L
        }

        @Test
        fun `should propagate exceptions from block`() {
            val config = TimeoutConfig.create {
                query = 1.seconds
            }

            assertThrows<IllegalStateException> {
                config.executeWithTimeout(TimeoutConfig.OperationType.QUERY) {
                    throw IllegalStateException("error")
                }
            }
        }
    }

    @Nested
    inner class ExecuteWithTimeoutSuspendTests {

        @Test
        fun `should return result when coroutine completes in time`() = runBlocking {
            val config = TimeoutConfig.create {
                query = 1.seconds
            }

            val result = config.executeWithTimeoutSuspend(TimeoutConfig.OperationType.QUERY) {
                kotlinx.coroutines.delay(50)
                "success"
            }

            result shouldBe "success"
        }

        @Test
        fun `should throw QueryTimeoutException when coroutine times out`() = runBlocking {
            val config = TimeoutConfig.create {
                query = 100.milliseconds
            }

            val exception = assertThrows<QueryTimeoutException> {
                runBlocking {
                    config.executeWithTimeoutSuspend(TimeoutConfig.OperationType.QUERY) {
                        kotlinx.coroutines.delay(500)
                        "should not return"
                    }
                }
            }

            exception.timeoutMillis shouldBe 100L
        }
    }

    @Nested
    inner class DSLTests {

        @Test
        fun `timeoutConfig DSL should create valid config`() {
            val config = timeoutConfig {
                default = 30.seconds
                connection = 5.seconds
                query = 10.seconds
            }

            config.forOperation(TimeoutConfig.OperationType.CONNECTION) shouldBe 5.seconds
            config.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 10.seconds
            config.forOperation(TimeoutConfig.OperationType.WRITE) shouldBe 30.seconds
        }
    }

    @Nested
    inner class AllOperationTypesTests {

        @Test
        fun `should handle all operation types`() {
            val config = TimeoutConfig.create {
                default = 30.seconds
                connection = 5.seconds
                query = 10.seconds
                write = 15.seconds
                transaction = 60.seconds
                batch = 120.seconds
                ddl = 300.seconds
                lock = 10.seconds
            }

            config.forOperation(TimeoutConfig.OperationType.CONNECTION) shouldBe 5.seconds
            config.forOperation(TimeoutConfig.OperationType.QUERY) shouldBe 10.seconds
            config.forOperation(TimeoutConfig.OperationType.WRITE) shouldBe 15.seconds
            config.forOperation(TimeoutConfig.OperationType.TRANSACTION) shouldBe 60.seconds
            config.forOperation(TimeoutConfig.OperationType.BATCH) shouldBe 120.seconds
            config.forOperation(TimeoutConfig.OperationType.DDL) shouldBe 300.seconds
            config.forOperation(TimeoutConfig.OperationType.LOCK) shouldBe 10.seconds
        }
    }
}
