@file:OptIn(kotlinx.coroutines.FlowPreview::class)

package com.physics91.korma.r2dbc

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Configuration for backpressure handling in R2DBC streaming operations.
 *
 * Usage:
 * ```kotlin
 * val config = BackpressureConfig.buffered(capacity = 100)
 *
 * database.queryFlow(sql, config) { row ->
 *     // map row
 * }
 * ```
 */
data class BackpressureConfig(
    val strategy: BackpressureStrategy,
    val bufferCapacity: Int = 64,
    val onBufferOverflow: BufferOverflow = BufferOverflow.SUSPEND,
    val fetchSize: Int = 100,
    val batchTimeout: Duration = 1000.milliseconds
) {
    companion object {
        /**
         * Default configuration with buffer and suspend on overflow.
         */
        val DEFAULT = BackpressureConfig(
            strategy = BackpressureStrategy.BUFFER,
            bufferCapacity = 64
        )

        /**
         * High-throughput configuration with larger buffer.
         */
        val HIGH_THROUGHPUT = BackpressureConfig(
            strategy = BackpressureStrategy.BUFFER,
            bufferCapacity = 1024,
            fetchSize = 500
        )

        /**
         * Memory-conscious configuration with smaller buffer.
         */
        val LOW_MEMORY = BackpressureConfig(
            strategy = BackpressureStrategy.BUFFER,
            bufferCapacity = 16,
            fetchSize = 50
        )

        /**
         * Create a buffered configuration.
         */
        fun buffered(
            capacity: Int = 64,
            fetchSize: Int = 100
        ) = BackpressureConfig(
            strategy = BackpressureStrategy.BUFFER,
            bufferCapacity = capacity,
            fetchSize = fetchSize
        )

        /**
         * Create a drop-oldest configuration for when keeping latest data is important.
         */
        fun dropOldest(
            capacity: Int = 64,
            fetchSize: Int = 100
        ) = BackpressureConfig(
            strategy = BackpressureStrategy.DROP_OLDEST,
            bufferCapacity = capacity,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
            fetchSize = fetchSize
        )

        /**
         * Create a drop-latest configuration for when processing order is important.
         */
        fun dropLatest(
            capacity: Int = 64,
            fetchSize: Int = 100
        ) = BackpressureConfig(
            strategy = BackpressureStrategy.DROP_LATEST,
            bufferCapacity = capacity,
            onBufferOverflow = BufferOverflow.DROP_LATEST,
            fetchSize = fetchSize
        )
    }
}

/**
 * Backpressure strategies for flow processing.
 */
enum class BackpressureStrategy {
    /**
     * Buffer elements and suspend producer when buffer is full.
     * This is the safest option but can cause producer to slow down.
     */
    BUFFER,

    /**
     * Drop the oldest elements when buffer is full.
     * Use when latest data is more important than historical data.
     */
    DROP_OLDEST,

    /**
     * Drop the latest elements when buffer is full.
     * Use when processing order must be preserved.
     */
    DROP_LATEST,

    /**
     * Throw exception when buffer is full.
     * Use when overflow indicates a system problem that needs attention.
     */
    ERROR
}

/**
 * Apply backpressure configuration to a flow.
 */
fun <T> Flow<T>.withBackpressure(config: BackpressureConfig): Flow<T> {
    return when (config.strategy) {
        BackpressureStrategy.BUFFER -> buffer(config.bufferCapacity, config.onBufferOverflow)
        BackpressureStrategy.DROP_OLDEST -> buffer(config.bufferCapacity, BufferOverflow.DROP_OLDEST)
        BackpressureStrategy.DROP_LATEST -> buffer(config.bufferCapacity, BufferOverflow.DROP_LATEST)
        BackpressureStrategy.ERROR -> conflate() // No overflow handling, may throw
    }
}

/**
 * Flow control utilities for database streaming.
 */
object FlowControl {

    /**
     * Apply rate limiting to a flow.
     *
     * @param itemsPerSecond Maximum items to emit per second
     */
    fun <T> Flow<T>.rateLimit(itemsPerSecond: Int): Flow<T> = flow {
        val delayMs = 1000L / itemsPerSecond
        var lastEmit = 0L

        collect { value ->
            val now = System.currentTimeMillis()
            val elapsed = now - lastEmit
            if (elapsed < delayMs && lastEmit > 0) {
                kotlinx.coroutines.delay(delayMs - elapsed)
            }
            lastEmit = System.currentTimeMillis()
            emit(value)
        }
    }

    /**
     * Sample a flow at regular intervals, emitting the latest value.
     *
     * @param interval Sampling interval
     */
    fun <T> Flow<T>.sampleAt(interval: Duration): Flow<T> = sample(interval)

    /**
     * Debounce a flow, emitting only after a quiet period.
     *
     * @param timeout Quiet period duration
     */
    fun <T> Flow<T>.debounceWith(timeout: Duration): Flow<T> = debounce(timeout)

    /**
     * Buffer with automatic flush on timeout or size.
     *
     * @param maxSize Maximum batch size before flush
     * @param timeout Maximum time before flush
     */
    fun <T> Flow<T>.bufferWithFlush(
        maxSize: Int,
        timeout: Duration
    ): Flow<List<T>> = flow {
        val buffer = mutableListOf<T>()
        var lastFlush = System.currentTimeMillis()

        collect { value ->
            buffer.add(value)
            val now = System.currentTimeMillis()
            val shouldFlush = buffer.size >= maxSize ||
                    (now - lastFlush) >= timeout.inWholeMilliseconds

            if (shouldFlush && buffer.isNotEmpty()) {
                emit(buffer.toList())
                buffer.clear()
                lastFlush = now
            }
        }

        if (buffer.isNotEmpty()) {
            emit(buffer.toList())
        }
    }

    /**
     * Chunk a flow into fixed-size lists.
     */
    fun <T> Flow<T>.chunk(size: Int): Flow<List<T>> = flow {
        val chunk = mutableListOf<T>()
        collect { value ->
            chunk.add(value)
            if (chunk.size >= size) {
                emit(chunk.toList())
                chunk.clear()
            }
        }
        if (chunk.isNotEmpty()) {
            emit(chunk.toList())
        }
    }

    /**
     * Window a flow into overlapping lists.
     *
     * @param size Window size
     * @param step Step size (elements to skip between windows)
     */
    fun <T> Flow<T>.window(size: Int, step: Int = 1): Flow<List<T>> = flow {
        val window = ArrayDeque<T>(size)

        collect { value ->
            window.addLast(value)

            if (window.size == size) {
                emit(window.toList())
                repeat(minOf(step, window.size)) {
                    window.removeFirst()
                }
            }
        }

        // Emit remaining partial window if any
        if (window.isNotEmpty()) {
            emit(window.toList())
        }
    }
}

/**
 * Extension to apply backpressure to R2DBC query flows.
 */
fun <T> R2dbcExecutor.queryWithBackpressure(
    sql: String,
    params: List<Any?> = emptyList(),
    config: BackpressureConfig = BackpressureConfig.DEFAULT,
    mapper: (io.r2dbc.spi.Row) -> T
): Flow<T> {
    return queryStream(sql, params, config.fetchSize, mapper)
        .withBackpressure(config)
}

/**
 * Extension to apply backpressure to R2DBC database flows.
 */
fun <T> R2dbcDatabase.selectFlowWithBackpressure(
    builder: com.physics91.korma.dsl.SelectBuilder,
    config: BackpressureConfig = BackpressureConfig.DEFAULT,
    mapper: (io.r2dbc.spi.Row) -> T
): Flow<T> {
    return selectFlow(builder, mapper).withBackpressure(config)
}
