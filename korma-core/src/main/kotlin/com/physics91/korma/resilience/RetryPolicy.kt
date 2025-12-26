package com.physics91.korma.resilience

import com.physics91.korma.exception.KormaException
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Configurable retry policy for database operations.
 *
 * Supports exponential backoff with jitter, configurable retry conditions,
 * and hooks for monitoring retry attempts.
 *
 * Usage:
 * ```kotlin
 * val policy = RetryPolicy.exponentialBackoff(
 *     maxAttempts = 3,
 *     initialDelay = 100.milliseconds,
 *     maxDelay = 5.seconds
 * )
 *
 * val result = policy.execute {
 *     database.query(sql)
 * }
 * ```
 */
class RetryPolicy internal constructor(
    private val maxAttempts: Int,
    private val delayStrategy: DelayStrategy,
    private val retryOn: (Throwable) -> Boolean,
    private val onRetry: ((RetryContext) -> Unit)?
) {
    // Logger in class body for access by instance methods
    private val logger = Companion.logger

    /**
     * Executes a block with retry logic (blocking version).
     */
    fun <T> execute(block: () -> T): T {
        return executeWithRetry(
            executeBlock = { block() },
            sleepFn = { delay -> Thread.sleep(delay.inWholeMilliseconds) }
        )
    }

    /**
     * Executes a suspending block with retry logic.
     */
    suspend fun <T> executeSuspend(block: suspend () -> T): T {
        return executeSuspendWithRetry(block)
    }

    /**
     * Common retry logic for blocking execution (DRY: extracted from execute).
     */
    private inline fun <T> executeWithRetry(
        executeBlock: () -> T,
        sleepFn: (Duration) -> Unit
    ): T {
        var lastException: Throwable? = null
        var attempt = 0

        while (attempt < maxAttempts) {
            try {
                return executeBlock()
            } catch (e: Throwable) {
                lastException = e
                attempt++

                if (attempt >= maxAttempts || !retryOn(e)) {
                    throw e
                }

                handleRetryAttempt(attempt, e, sleepFn)
            }
        }

        throw lastException ?: IllegalStateException("Retry exhausted without exception")
    }

    /**
     * Common retry logic for suspending execution.
     * Note: Cannot be inlined due to suspend function type parameter limitation.
     */
    private suspend fun <T> executeSuspendWithRetry(block: suspend () -> T): T {
        var lastException: Throwable? = null
        var attempt = 0

        while (attempt < maxAttempts) {
            try {
                return block()
            } catch (e: Throwable) {
                lastException = e
                attempt++

                if (attempt >= maxAttempts || !retryOn(e)) {
                    throw e
                }

                val nextDelay = handleRetryAttemptAndGetDelay(attempt, e)
                delay(nextDelay)
            }
        }

        throw lastException ?: IllegalStateException("Retry exhausted without exception")
    }

    /**
     * Handles a retry attempt: logs, invokes callback, and sleeps (DRY: single source of truth).
     */
    private inline fun handleRetryAttempt(
        attempt: Int,
        exception: Throwable,
        sleepFn: (Duration) -> Unit
    ) {
        val nextDelay = handleRetryAttemptAndGetDelay(attempt, exception)
        sleepFn(nextDelay)
    }

    /**
     * Common logic for handling retry attempt: calculates delay, logs, and invokes callback.
     */
    private fun handleRetryAttemptAndGetDelay(attempt: Int, exception: Throwable): Duration {
        val nextDelay = delayStrategy.nextDelay(attempt)
        val context = RetryContext(
            attempt = attempt,
            maxAttempts = maxAttempts,
            lastException = exception,
            nextDelay = nextDelay
        )

        logger.debug(
            "Retry attempt {} of {} after {} - {}",
            attempt, maxAttempts, nextDelay, exception.message
        )
        onRetry?.invoke(context)

        return nextDelay
    }

    /**
     * Creates a new policy with a different retry condition.
     */
    fun retryIf(predicate: (Throwable) -> Boolean): RetryPolicy {
        return RetryPolicy(maxAttempts, delayStrategy, predicate, onRetry)
    }

    /**
     * Creates a new policy with an onRetry callback.
     */
    fun onRetry(callback: (RetryContext) -> Unit): RetryPolicy {
        return RetryPolicy(maxAttempts, delayStrategy, retryOn, callback)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RetryPolicy::class.java)

        /**
         * Creates a retry policy with no retries (execute once).
         */
        fun noRetry(): RetryPolicy = RetryPolicy(
            maxAttempts = 1,
            delayStrategy = FixedDelayStrategy(Duration.ZERO),
            retryOn = { false },
            onRetry = null
        )

        /**
         * Creates a retry policy with fixed delay between attempts.
         */
        fun fixedDelay(
            maxAttempts: Int = 3,
            delay: Duration = 1.seconds
        ): RetryPolicy = RetryPolicy(
            maxAttempts = maxAttempts,
            delayStrategy = FixedDelayStrategy(delay),
            retryOn = { it.isRecoverable() },
            onRetry = null
        )

        /**
         * Creates a retry policy with exponential backoff.
         */
        fun exponentialBackoff(
            maxAttempts: Int = 3,
            initialDelay: Duration = 100.milliseconds,
            maxDelay: Duration = 30.seconds,
            multiplier: Double = 2.0
        ): RetryPolicy = RetryPolicy(
            maxAttempts = maxAttempts,
            delayStrategy = ExponentialBackoffStrategy(initialDelay, maxDelay, multiplier),
            retryOn = { it.isRecoverable() },
            onRetry = null
        )

        /**
         * Creates a retry policy with exponential backoff and jitter.
         */
        fun exponentialBackoffWithJitter(
            maxAttempts: Int = 3,
            initialDelay: Duration = 100.milliseconds,
            maxDelay: Duration = 30.seconds,
            multiplier: Double = 2.0,
            jitterFactor: Double = 0.5
        ): RetryPolicy = RetryPolicy(
            maxAttempts = maxAttempts,
            delayStrategy = ExponentialBackoffWithJitterStrategy(
                initialDelay, maxDelay, multiplier, jitterFactor
            ),
            retryOn = { it.isRecoverable() },
            onRetry = null
        )

        /**
         * Creates a retry policy for deadlock recovery.
         */
        fun forDeadlocks(
            maxAttempts: Int = 5,
            initialDelay: Duration = 50.milliseconds,
            maxDelay: Duration = 5.seconds
        ): RetryPolicy = RetryPolicy(
            maxAttempts = maxAttempts,
            delayStrategy = ExponentialBackoffWithJitterStrategy(
                initialDelay, maxDelay, 2.0, 0.5
            ),
            retryOn = { it.isDeadlock() },
            onRetry = null
        )

        /**
         * Creates a retry policy for connection errors.
         */
        fun forConnectionErrors(
            maxAttempts: Int = 3,
            initialDelay: Duration = 500.milliseconds,
            maxDelay: Duration = 10.seconds
        ): RetryPolicy = RetryPolicy(
            maxAttempts = maxAttempts,
            delayStrategy = ExponentialBackoffStrategy(initialDelay, maxDelay, 2.0),
            retryOn = { it.isConnectionError() },
            onRetry = null
        )

        private fun Throwable.isRecoverable(): Boolean {
            return when (this) {
                is KormaException -> this.isRecoverable
                else -> false
            }
        }

        private fun Throwable.isDeadlock(): Boolean {
            return this is com.physics91.korma.exception.DeadlockException
        }

        private fun Throwable.isConnectionError(): Boolean {
            return this is com.physics91.korma.exception.ConnectionException
        }
    }
}

/**
 * Context information provided to retry callbacks.
 */
data class RetryContext(
    val attempt: Int,
    val maxAttempts: Int,
    val lastException: Throwable,
    val nextDelay: Duration
) {
    val remainingAttempts: Int get() = maxAttempts - attempt
    val isLastAttempt: Boolean get() = remainingAttempts == 0
}

/**
 * Strategy for calculating delay between retry attempts.
 */
sealed interface DelayStrategy {
    fun nextDelay(attempt: Int): Duration
}

/**
 * Fixed delay between attempts.
 */
class FixedDelayStrategy(private val delay: Duration) : DelayStrategy {
    override fun nextDelay(attempt: Int): Duration = delay
}

/**
 * Exponential backoff delay.
 */
class ExponentialBackoffStrategy(
    private val initialDelay: Duration,
    private val maxDelay: Duration,
    private val multiplier: Double
) : DelayStrategy {
    override fun nextDelay(attempt: Int): Duration {
        val delayMillis = initialDelay.inWholeMilliseconds * multiplier.pow(attempt - 1)
        return min(delayMillis.toLong(), maxDelay.inWholeMilliseconds).milliseconds
    }
}

/**
 * Exponential backoff with random jitter.
 */
class ExponentialBackoffWithJitterStrategy(
    private val initialDelay: Duration,
    private val maxDelay: Duration,
    private val multiplier: Double,
    private val jitterFactor: Double
) : DelayStrategy {
    override fun nextDelay(attempt: Int): Duration {
        val baseDelayMillis = initialDelay.inWholeMilliseconds * multiplier.pow(attempt - 1)
        val cappedDelay = min(baseDelayMillis, maxDelay.inWholeMilliseconds.toDouble())

        // Add random jitter: delay * (1 - jitterFactor + random * 2 * jitterFactor)
        val jitter = 1.0 - jitterFactor + Random.nextDouble() * 2 * jitterFactor
        return (cappedDelay * jitter).toLong().milliseconds
    }
}

/**
 * DSL builder for retry policies.
 */
class RetryPolicyBuilder {
    var maxAttempts: Int = 3
    var initialDelay: Duration = 100.milliseconds
    var maxDelay: Duration = 30.seconds
    var multiplier: Double = 2.0
    var jitterFactor: Double = 0.0
    private var retryCondition: (Throwable) -> Boolean = {
        it is KormaException && it.isRecoverable
    }
    private var onRetryCallback: ((RetryContext) -> Unit)? = null

    fun retryIf(predicate: (Throwable) -> Boolean) {
        retryCondition = predicate
    }

    fun onRetry(callback: (RetryContext) -> Unit) {
        onRetryCallback = callback
    }

    fun build(): RetryPolicy {
        val strategy = if (jitterFactor > 0) {
            ExponentialBackoffWithJitterStrategy(initialDelay, maxDelay, multiplier, jitterFactor)
        } else if (multiplier != 1.0) {
            ExponentialBackoffStrategy(initialDelay, maxDelay, multiplier)
        } else {
            FixedDelayStrategy(initialDelay)
        }

        return RetryPolicy(
            maxAttempts = maxAttempts,
            delayStrategy = strategy,
            retryOn = retryCondition,
            onRetry = onRetryCallback
        )
    }
}

/**
 * DSL function to create a retry policy.
 */
fun retryPolicy(block: RetryPolicyBuilder.() -> Unit): RetryPolicy {
    return RetryPolicyBuilder().apply(block).build()
}
