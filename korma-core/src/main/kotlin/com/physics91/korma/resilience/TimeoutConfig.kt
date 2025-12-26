package com.physics91.korma.resilience

import com.physics91.korma.exception.QueryTimeoutException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Hierarchical timeout configuration for database operations.
 *
 * Supports different timeout levels for different operation types,
 * with inheritance from parent configurations.
 *
 * Usage:
 * ```kotlin
 * val config = TimeoutConfig.create {
 *     default = 30.seconds
 *     query = 10.seconds
 *     transaction = 60.seconds
 *     connection = 5.seconds
 * }
 *
 * val queryTimeout = config.forOperation(OperationType.QUERY)
 * ```
 */
class TimeoutConfig internal constructor(
    private val timeouts: Map<OperationType, Duration>,
    private val defaultTimeout: Duration
) {
    /**
     * Types of database operations with different timeout requirements.
     */
    enum class OperationType {
        /**
         * Connection acquisition from pool.
         */
        CONNECTION,

        /**
         * Query execution (SELECT).
         */
        QUERY,

        /**
         * Write operations (INSERT, UPDATE, DELETE).
         */
        WRITE,

        /**
         * Transaction operations.
         */
        TRANSACTION,

        /**
         * Batch operations.
         */
        BATCH,

        /**
         * DDL operations (CREATE, ALTER, DROP).
         */
        DDL,

        /**
         * Lock acquisition.
         */
        LOCK
    }

    /**
     * Gets the timeout for a specific operation type.
     */
    fun forOperation(type: OperationType): Duration {
        return timeouts[type] ?: defaultTimeout
    }

    /**
     * Gets the timeout in milliseconds for JDBC operations.
     */
    fun forOperationMillis(type: OperationType): Long {
        return forOperation(type).inWholeMilliseconds
    }

    /**
     * Gets the timeout in seconds for JDBC operations.
     */
    fun forOperationSeconds(type: OperationType): Int {
        return forOperation(type).inWholeSeconds.toInt()
    }

    /**
     * Creates a new config with an overridden timeout.
     */
    fun withTimeout(type: OperationType, timeout: Duration): TimeoutConfig {
        return TimeoutConfig(
            timeouts = timeouts + (type to timeout),
            defaultTimeout = defaultTimeout
        )
    }

    /**
     * Executes a block with the specified timeout (blocking version).
     */
    fun <T> executeWithTimeout(type: OperationType, block: () -> T): T {
        val timeout = forOperation(type)
        val future = CompletableFuture.supplyAsync { block() }

        return try {
            future.get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            throw QueryTimeoutException(
                message = "Operation timed out after $timeout",
                timeoutMillis = timeout.inWholeMilliseconds
            )
        } catch (e: java.util.concurrent.ExecutionException) {
            throw e.cause ?: e
        }
    }

    /**
     * Executes a suspending block with the specified timeout.
     */
    suspend fun <T> executeWithTimeoutSuspend(type: OperationType, block: suspend () -> T): T {
        val timeout = forOperation(type)

        return try {
            withTimeout(timeout) {
                block()
            }
        } catch (e: TimeoutCancellationException) {
            throw QueryTimeoutException(
                message = "Operation timed out after $timeout",
                timeoutMillis = timeout.inWholeMilliseconds
            )
        }
    }

    companion object {
        /**
         * Default timeout configuration.
         */
        val DEFAULT = create {
            default = 30.seconds
            connection = 5.seconds
            query = 30.seconds
            write = 30.seconds
            transaction = 60.seconds
            batch = 120.seconds
            ddl = 300.seconds
            lock = 10.seconds
        }

        /**
         * Strict timeout configuration for high-performance scenarios.
         */
        val STRICT = create {
            default = 5.seconds
            connection = 2.seconds
            query = 5.seconds
            write = 5.seconds
            transaction = 15.seconds
            batch = 30.seconds
            ddl = 60.seconds
            lock = 3.seconds
        }

        /**
         * Lenient timeout configuration for long-running operations.
         */
        val LENIENT = create {
            default = 120.seconds
            connection = 30.seconds
            query = 120.seconds
            write = 120.seconds
            transaction = 300.seconds
            batch = 600.seconds
            ddl = 900.seconds
            lock = 60.seconds
        }

        /**
         * Creates a custom timeout configuration.
         */
        fun create(block: TimeoutConfigBuilder.() -> Unit): TimeoutConfig {
            return TimeoutConfigBuilder().apply(block).build()
        }
    }
}

/**
 * Builder for timeout configuration.
 */
class TimeoutConfigBuilder {
    var default: Duration = 30.seconds
    var connection: Duration? = null
    var query: Duration? = null
    var write: Duration? = null
    var transaction: Duration? = null
    var batch: Duration? = null
    var ddl: Duration? = null
    var lock: Duration? = null

    fun build(): TimeoutConfig {
        val timeouts = mutableMapOf<TimeoutConfig.OperationType, Duration>()

        connection?.let { timeouts[TimeoutConfig.OperationType.CONNECTION] = it }
        query?.let { timeouts[TimeoutConfig.OperationType.QUERY] = it }
        write?.let { timeouts[TimeoutConfig.OperationType.WRITE] = it }
        transaction?.let { timeouts[TimeoutConfig.OperationType.TRANSACTION] = it }
        batch?.let { timeouts[TimeoutConfig.OperationType.BATCH] = it }
        ddl?.let { timeouts[TimeoutConfig.OperationType.DDL] = it }
        lock?.let { timeouts[TimeoutConfig.OperationType.LOCK] = it }

        return TimeoutConfig(timeouts, default)
    }
}

/**
 * DSL function to create timeout configuration.
 */
fun timeoutConfig(block: TimeoutConfigBuilder.() -> Unit): TimeoutConfig {
    return TimeoutConfigBuilder().apply(block).build()
}
