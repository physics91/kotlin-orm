package com.physics91.korma.resilience

import com.physics91.korma.exception.KormaException
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Circuit breaker for database operations.
 *
 * Implements the circuit breaker pattern to prevent cascading failures
 * when a database becomes unavailable or unresponsive.
 *
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Failures exceeded threshold, requests fail fast
 * - HALF_OPEN: Testing if service has recovered
 *
 * Usage:
 * ```kotlin
 * val circuitBreaker = CircuitBreaker.create("database") {
 *     failureThreshold = 5
 *     openDuration = 30.seconds
 *     halfOpenPermittedCalls = 3
 * }
 *
 * val result = circuitBreaker.execute {
 *     database.query(sql)
 * }
 * ```
 */
class CircuitBreaker internal constructor(
    val name: String,
    private val config: CircuitBreakerConfig,
    private val stateChangeListener: ((State, State) -> Unit)?
) {
    private val logger = LoggerFactory.getLogger(CircuitBreaker::class.java)

    private val state = AtomicReference(State.CLOSED)
    private val failureCount = AtomicInteger(0)
    private val successCount = AtomicInteger(0)
    private val lastFailureTime = AtomicLong(0)
    private val halfOpenCallCount = AtomicInteger(0)

    /**
     * Current state of the circuit breaker.
     */
    enum class State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    /**
     * Returns the current state.
     */
    fun currentState(): State = state.get()

    /**
     * Returns circuit breaker metrics.
     */
    fun metrics(): CircuitBreakerMetrics = CircuitBreakerMetrics(
        name = name,
        state = state.get(),
        failureCount = failureCount.get(),
        successCount = successCount.get(),
        lastFailureTime = if (lastFailureTime.get() > 0) {
            Instant.ofEpochMilli(lastFailureTime.get())
        } else null
    )

    /**
     * Executes a block with circuit breaker protection.
     */
    fun <T> execute(block: () -> T): T {
        val currentState = checkAndUpdateState()

        return when (currentState) {
            State.OPEN -> {
                logger.debug("Circuit breaker '{}' is OPEN, failing fast", name)
                throw CircuitBreakerOpenException(
                    name = name,
                    remainingWait = remainingOpenDuration()
                )
            }
            State.HALF_OPEN -> {
                if (!tryAcquireHalfOpenPermit()) {
                    throw CircuitBreakerOpenException(
                        name = name,
                        remainingWait = Duration.ZERO
                    )
                }
                executeWithTracking(block)
            }
            State.CLOSED -> executeWithTracking(block)
        }
    }

    /**
     * Executes a suspending block with circuit breaker protection.
     */
    suspend fun <T> executeSuspend(block: suspend () -> T): T {
        val currentState = checkAndUpdateState()

        return when (currentState) {
            State.OPEN -> {
                logger.debug("Circuit breaker '{}' is OPEN, failing fast", name)
                throw CircuitBreakerOpenException(
                    name = name,
                    remainingWait = remainingOpenDuration()
                )
            }
            State.HALF_OPEN -> {
                if (!tryAcquireHalfOpenPermit()) {
                    throw CircuitBreakerOpenException(
                        name = name,
                        remainingWait = Duration.ZERO
                    )
                }
                executeSuspendWithTracking(block)
            }
            State.CLOSED -> executeSuspendWithTracking(block)
        }
    }

    /**
     * Manually opens the circuit breaker.
     */
    fun open() {
        transitionTo(State.OPEN)
    }

    /**
     * Manually closes the circuit breaker.
     */
    fun close() {
        transitionTo(State.CLOSED)
        reset()
    }

    /**
     * Resets all counters.
     */
    fun reset() {
        failureCount.set(0)
        successCount.set(0)
        halfOpenCallCount.set(0)
    }

    private fun <T> executeWithTracking(block: () -> T): T {
        return try {
            val result = block()
            onSuccess()
            result
        } catch (e: Throwable) {
            onFailure(e)
            throw e
        }
    }

    private suspend fun <T> executeSuspendWithTracking(block: suspend () -> T): T {
        return try {
            val result = block()
            onSuccess()
            result
        } catch (e: Throwable) {
            onFailure(e)
            throw e
        }
    }

    private fun onSuccess() {
        val currentState = state.get()

        when (currentState) {
            State.HALF_OPEN -> {
                val successes = successCount.incrementAndGet()
                if (successes >= config.halfOpenPermittedCalls) {
                    logger.info(
                        "Circuit breaker '{}' transitioning to CLOSED after {} successful calls",
                        name, successes
                    )
                    transitionTo(State.CLOSED)
                    reset()
                }
            }
            State.CLOSED -> {
                // Reset failure count on success in closed state
                if (config.resetFailureCountOnSuccess) {
                    failureCount.set(0)
                }
                successCount.incrementAndGet()
            }
            else -> {}
        }
    }

    private fun onFailure(exception: Throwable) {
        if (!config.failurePredicate(exception)) {
            return
        }

        lastFailureTime.set(System.currentTimeMillis())
        val currentState = state.get()

        when (currentState) {
            State.HALF_OPEN -> {
                logger.info(
                    "Circuit breaker '{}' transitioning to OPEN after failure in HALF_OPEN: {}",
                    name, exception.message
                )
                transitionTo(State.OPEN)
            }
            State.CLOSED -> {
                val failures = failureCount.incrementAndGet()
                if (failures >= config.failureThreshold) {
                    logger.info(
                        "Circuit breaker '{}' transitioning to OPEN after {} failures",
                        name, failures
                    )
                    transitionTo(State.OPEN)
                }
            }
            else -> {}
        }
    }

    private fun checkAndUpdateState(): State {
        val currentState = state.get()

        if (currentState == State.OPEN) {
            val lastFailure = lastFailureTime.get()
            val elapsed = System.currentTimeMillis() - lastFailure

            if (elapsed >= config.openDuration.inWholeMilliseconds) {
                logger.info(
                    "Circuit breaker '{}' transitioning to HALF_OPEN after {} wait",
                    name, config.openDuration
                )
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    halfOpenCallCount.set(0)
                    successCount.set(0)
                    stateChangeListener?.invoke(State.OPEN, State.HALF_OPEN)
                    return State.HALF_OPEN
                }
            }
        }

        return state.get()
    }

    private fun transitionTo(newState: State) {
        val oldState = state.getAndSet(newState)
        if (oldState != newState) {
            stateChangeListener?.invoke(oldState, newState)
        }
    }

    private fun tryAcquireHalfOpenPermit(): Boolean {
        val current = halfOpenCallCount.get()
        if (current >= config.halfOpenPermittedCalls) {
            return false
        }
        return halfOpenCallCount.compareAndSet(current, current + 1)
    }

    private fun remainingOpenDuration(): Duration {
        val lastFailure = lastFailureTime.get()
        val elapsed = System.currentTimeMillis() - lastFailure
        val remaining = config.openDuration.inWholeMilliseconds - elapsed
        return if (remaining > 0) remaining.milliseconds else Duration.ZERO
    }

    companion object {
        /**
         * Creates a circuit breaker with default settings.
         */
        fun create(name: String): CircuitBreaker {
            return CircuitBreaker(name, CircuitBreakerConfig(), null)
        }

        /**
         * Creates a circuit breaker with custom settings.
         */
        fun create(name: String, configure: CircuitBreakerConfig.() -> Unit): CircuitBreaker {
            val config = CircuitBreakerConfig().apply(configure)
            return CircuitBreaker(name, config, null)
        }

        /**
         * Creates a circuit breaker with a state change listener.
         */
        fun create(
            name: String,
            config: CircuitBreakerConfig,
            stateChangeListener: (State, State) -> Unit
        ): CircuitBreaker {
            return CircuitBreaker(name, config, stateChangeListener)
        }
    }
}

/**
 * Configuration for circuit breaker.
 */
data class CircuitBreakerConfig(
    /**
     * Number of failures before opening the circuit.
     */
    var failureThreshold: Int = 5,

    /**
     * Duration to wait in OPEN state before transitioning to HALF_OPEN.
     */
    var openDuration: Duration = 30.seconds,

    /**
     * Number of calls allowed in HALF_OPEN state to test recovery.
     */
    var halfOpenPermittedCalls: Int = 3,

    /**
     * Whether to reset failure count after a successful call.
     */
    var resetFailureCountOnSuccess: Boolean = true,

    /**
     * Predicate to determine if an exception should count as a failure.
     */
    var failurePredicate: (Throwable) -> Boolean = { true }
)

/**
 * Metrics exposed by the circuit breaker.
 */
data class CircuitBreakerMetrics(
    val name: String,
    val state: CircuitBreaker.State,
    val failureCount: Int,
    val successCount: Int,
    val lastFailureTime: Instant?
)

/**
 * Exception thrown when the circuit breaker is open.
 */
class CircuitBreakerOpenException(
    val name: String,
    val remainingWait: Duration
) : RuntimeException(
    "Circuit breaker '$name' is open, will retry after $remainingWait"
) {
    val errorCode: String = "KORMA-CB001"
    val isRecoverable: Boolean = true
}

/**
 * DSL builder for circuit breaker.
 */
class CircuitBreakerBuilder(private val name: String) {
    var failureThreshold: Int = 5
    var openDuration: Duration = 30.seconds
    var halfOpenPermittedCalls: Int = 3
    var resetFailureCountOnSuccess: Boolean = true
    private var failurePredicate: (Throwable) -> Boolean = { true }
    private var stateChangeListener: ((CircuitBreaker.State, CircuitBreaker.State) -> Unit)? = null

    fun failOn(predicate: (Throwable) -> Boolean) {
        failurePredicate = predicate
    }

    fun onStateChange(listener: (CircuitBreaker.State, CircuitBreaker.State) -> Unit) {
        stateChangeListener = listener
    }

    fun build(): CircuitBreaker {
        val config = CircuitBreakerConfig(
            failureThreshold = failureThreshold,
            openDuration = openDuration,
            halfOpenPermittedCalls = halfOpenPermittedCalls,
            resetFailureCountOnSuccess = resetFailureCountOnSuccess,
            failurePredicate = failurePredicate
        )
        return CircuitBreaker(name, config, stateChangeListener)
    }
}

/**
 * DSL function to create a circuit breaker.
 */
fun circuitBreaker(name: String, block: CircuitBreakerBuilder.() -> Unit): CircuitBreaker {
    return CircuitBreakerBuilder(name).apply(block).build()
}
