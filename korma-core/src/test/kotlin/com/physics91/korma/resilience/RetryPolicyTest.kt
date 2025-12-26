package com.physics91.korma.resilience

import com.physics91.korma.exception.ConnectionLostException
import com.physics91.korma.exception.DeadlockException
import com.physics91.korma.exception.QueryBuildException
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class RetryPolicyTest {

    @Nested
    inner class NoRetryTests {

        @Test
        fun `noRetry should execute once and return result`() {
            val policy = RetryPolicy.noRetry()
            val counter = AtomicInteger(0)

            val result = policy.execute {
                counter.incrementAndGet()
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 1
        }

        @Test
        fun `noRetry should throw exception immediately`() {
            val policy = RetryPolicy.noRetry()
            val counter = AtomicInteger(0)

            assertThrows<RuntimeException> {
                policy.execute {
                    counter.incrementAndGet()
                    throw RuntimeException("fail")
                }
            }

            counter.get() shouldBe 1
        }
    }

    @Nested
    inner class FixedDelayTests {

        @Test
        fun `fixedDelay should retry on recoverable exceptions`() {
            val policy = RetryPolicy.fixedDelay(
                maxAttempts = 3,
                delay = 10.milliseconds
            )
            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 3) {
                    throw ConnectionLostException("Connection lost")
                }
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 3
        }

        @Test
        fun `fixedDelay should not retry on non-recoverable exceptions`() {
            val policy = RetryPolicy.fixedDelay(maxAttempts = 3)
            val counter = AtomicInteger(0)

            assertThrows<QueryBuildException> {
                policy.execute {
                    counter.incrementAndGet()
                    throw QueryBuildException("Syntax error")
                }
            }

            counter.get() shouldBe 1
        }

        @Test
        fun `fixedDelay should throw after max attempts`() {
            val policy = RetryPolicy.fixedDelay(
                maxAttempts = 3,
                delay = 10.milliseconds
            )
            val counter = AtomicInteger(0)

            assertThrows<ConnectionLostException> {
                policy.execute {
                    counter.incrementAndGet()
                    throw ConnectionLostException("Always fails")
                }
            }

            counter.get() shouldBe 3
        }
    }

    @Nested
    inner class ExponentialBackoffTests {

        @Test
        fun `exponentialBackoff should retry with increasing delays`() {
            val delays = mutableListOf<Long>()
            val policy = RetryPolicy.exponentialBackoff(
                maxAttempts = 4,
                initialDelay = 10.milliseconds,
                maxDelay = 1000.milliseconds,
                multiplier = 2.0
            ).onRetry { context ->
                delays.add(context.nextDelay.inWholeMilliseconds)
            }

            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 4) {
                    throw ConnectionLostException("Connection lost")
                }
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 4

            // Delays should approximately double each time
            // First retry: 10ms, second: 20ms, third: 40ms
            delays.size shouldBe 3
            delays[0] shouldBe 10L
            delays[1] shouldBe 20L
            delays[2] shouldBe 40L
        }

        @Test
        fun `exponentialBackoff should respect maxDelay`() {
            val delays = mutableListOf<Long>()
            val policy = RetryPolicy.exponentialBackoff(
                maxAttempts = 5,
                initialDelay = 100.milliseconds,
                maxDelay = 250.milliseconds,
                multiplier = 2.0
            ).onRetry { context ->
                delays.add(context.nextDelay.inWholeMilliseconds)
            }

            val counter = AtomicInteger(0)

            assertThrows<ConnectionLostException> {
                policy.execute {
                    counter.incrementAndGet()
                    throw ConnectionLostException("Always fails")
                }
            }

            // Delays should be: 100, 200, 250 (capped), 250 (capped)
            delays.size shouldBe 4
            delays[0] shouldBe 100L
            delays[1] shouldBe 200L
            delays[2] shouldBe 250L // capped
            delays[3] shouldBe 250L // capped
        }
    }

    @Nested
    inner class DeadlockRetryTests {

        @Test
        fun `forDeadlocks should only retry on DeadlockException`() {
            val policy = RetryPolicy.forDeadlocks(
                maxAttempts = 3,
                initialDelay = 10.milliseconds
            )
            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 3) {
                    throw DeadlockException("Deadlock detected")
                }
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 3
        }

        @Test
        fun `forDeadlocks should not retry on other exceptions`() {
            val policy = RetryPolicy.forDeadlocks(maxAttempts = 3)
            val counter = AtomicInteger(0)

            assertThrows<ConnectionLostException> {
                policy.execute {
                    counter.incrementAndGet()
                    throw ConnectionLostException("Not a deadlock")
                }
            }

            counter.get() shouldBe 1
        }
    }

    @Nested
    inner class ConnectionRetryTests {

        @Test
        fun `forConnectionErrors should retry on connection exceptions`() {
            val policy = RetryPolicy.forConnectionErrors(
                maxAttempts = 3,
                initialDelay = 10.milliseconds
            )
            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 3) {
                    throw ConnectionLostException("Connection lost")
                }
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 3
        }
    }

    @Nested
    inner class CustomRetryConditionTests {

        @Test
        fun `retryIf should allow custom retry conditions`() {
            val policy = RetryPolicy.fixedDelay(
                maxAttempts = 3,
                delay = 10.milliseconds
            ).retryIf { e -> e.message?.contains("retry") == true }

            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 3) {
                    throw RuntimeException("Please retry")
                }
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 3
        }

        @Test
        fun `custom condition should not retry when not matched`() {
            val policy = RetryPolicy.fixedDelay(maxAttempts = 3)
                .retryIf { e -> e.message?.contains("RETRY_ME") == true }

            val counter = AtomicInteger(0)

            assertThrows<RuntimeException> {
                policy.execute {
                    counter.incrementAndGet()
                    throw RuntimeException("Do not repeat this")
                }
            }

            counter.get() shouldBe 1
        }
    }

    @Nested
    inner class CallbackTests {

        @Test
        fun `onRetry callback should receive correct context`() {
            val contexts = mutableListOf<RetryContext>()
            val policy = RetryPolicy.fixedDelay(
                maxAttempts = 4,
                delay = 10.milliseconds
            ).onRetry { context ->
                contexts.add(context)
            }

            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 4) {
                    throw ConnectionLostException("Fail")
                }
                "success"
            }

            result shouldBe "success"
            contexts.size shouldBe 3

            contexts[0].attempt shouldBe 1
            contexts[0].maxAttempts shouldBe 4
            contexts[0].remainingAttempts shouldBe 3
            contexts[0].isLastAttempt shouldBe false

            contexts[2].attempt shouldBe 3
            contexts[2].remainingAttempts shouldBe 1
            contexts[2].isLastAttempt shouldBe false
        }
    }

    @Nested
    inner class SuspendTests {

        @Test
        fun `executeSuspend should work with coroutines`() = runBlocking {
            val policy = RetryPolicy.fixedDelay(
                maxAttempts = 3,
                delay = 10.milliseconds
            )
            val counter = AtomicInteger(0)

            val result = policy.executeSuspend {
                if (counter.incrementAndGet() < 3) {
                    throw ConnectionLostException("Connection lost")
                }
                "success"
            }

            result shouldBe "success"
            counter.get() shouldBe 3
        }
    }

    @Nested
    inner class DSLTests {

        @Test
        fun `retryPolicy DSL should create valid policy`() {
            val contexts = mutableListOf<RetryContext>()
            val policy = retryPolicy {
                maxAttempts = 3
                initialDelay = 10.milliseconds
                retryIf { it is ConnectionLostException }
                onRetry { contexts.add(it) }
            }

            val counter = AtomicInteger(0)

            val result = policy.execute {
                if (counter.incrementAndGet() < 3) {
                    throw ConnectionLostException("Fail")
                }
                "success"
            }

            result shouldBe "success"
            contexts.size shouldBe 2
        }
    }
}
