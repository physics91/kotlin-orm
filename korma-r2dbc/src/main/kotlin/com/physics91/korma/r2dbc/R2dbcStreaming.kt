package com.physics91.korma.r2dbc

import io.r2dbc.spi.Row
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore

/**
 * Streaming utilities for handling large result sets with backpressure.
 */
object R2dbcStreaming {

    /**
     * Stream query results in chunks.
     */
    fun <T> R2dbcExecutor.queryChunked(
        sql: String,
        params: List<Any?> = emptyList(),
        chunkSize: Int = 1000,
        mapper: (Row) -> T
    ): Flow<List<T>> = flow {
        val chunk = mutableListOf<T>()
        query(sql, params, mapper).collect { item ->
            chunk.add(item)
            if (chunk.size >= chunkSize) {
                emit(chunk.toList())
                chunk.clear()
            }
        }
        if (chunk.isNotEmpty()) {
            emit(chunk.toList())
        }
    }

    /**
     * Process query results with controlled concurrency.
     */
    fun <T, R> R2dbcExecutor.queryWithConcurrency(
        sql: String,
        params: List<Any?> = emptyList(),
        concurrency: Int = 4,
        mapper: (Row) -> T,
        processor: suspend (T) -> R
    ): Flow<R> = query(sql, params, mapper)
        .buffer(Channel.BUFFERED)
        .map { processor(it) }

    /**
     * Apply rate limiting to a flow.
     */
    fun <T> Flow<T>.rateLimited(
        maxConcurrent: Int
    ): Flow<T> = flow {
        val semaphore = Semaphore(maxConcurrent)
        collect { value ->
            semaphore.acquire()
            try {
                emit(value)
            } finally {
                semaphore.release()
            }
        }
    }
}

/**
 * Pagination support for streaming large datasets.
 */
class PagedStream<T>(
    private val pageSize: Int,
    private val fetcher: suspend (offset: Long, limit: Int) -> List<T>
) {
    /**
     * Stream all pages.
     */
    fun asFlow(): Flow<T> = flow {
        var offset = 0L
        var hasMore = true

        while (hasMore) {
            val page = fetcher(offset, pageSize)
            page.forEach { emit(it) }

            if (page.size < pageSize) {
                hasMore = false
            } else {
                offset += pageSize
            }
        }
    }

    /**
     * Stream pages as lists.
     */
    fun asPageFlow(): Flow<List<T>> = flow {
        var offset = 0L
        var hasMore = true

        while (hasMore) {
            val page = fetcher(offset, pageSize)
            if (page.isNotEmpty()) {
                emit(page)
            }

            if (page.size < pageSize) {
                hasMore = false
            } else {
                offset += pageSize
            }
        }
    }
}

/**
 * Cursor-based pagination for efficient streaming.
 */
class CursorStream<T, C : Comparable<C>>(
    private val pageSize: Int,
    private val cursorExtractor: (T) -> C,
    private val fetcher: suspend (cursor: C?, limit: Int) -> List<T>
) {
    /**
     * Stream all items using cursor-based pagination.
     */
    fun asFlow(): Flow<T> = flow {
        var cursor: C? = null
        var hasMore = true

        while (hasMore) {
            val page = fetcher(cursor, pageSize)
            page.forEach { emit(it) }

            if (page.size < pageSize) {
                hasMore = false
            } else {
                cursor = cursorExtractor(page.last())
            }
        }
    }
}

/**
 * Extension functions for Flow transformations useful in database operations.
 */
object FlowExtensions {

    /**
     * Batch collect items from a flow.
     */
    fun <T> Flow<T>.batch(size: Int): Flow<List<T>> = flow {
        val batch = mutableListOf<T>()
        collect { item ->
            batch.add(item)
            if (batch.size >= size) {
                emit(batch.toList())
                batch.clear()
            }
        }
        if (batch.isNotEmpty()) {
            emit(batch.toList())
        }
    }

    /**
     * Buffer with timeout - emit batch when size reached or timeout elapsed.
     */
    fun <T> Flow<T>.batchWithTimeout(
        size: Int,
        timeoutMillis: Long
    ): Flow<List<T>> = flow {
        val batch = mutableListOf<T>()
        var lastEmitTime = System.currentTimeMillis()

        collect { item ->
            batch.add(item)
            val now = System.currentTimeMillis()

            if (batch.size >= size || (now - lastEmitTime) >= timeoutMillis) {
                if (batch.isNotEmpty()) {
                    emit(batch.toList())
                    batch.clear()
                    lastEmitTime = now
                }
            }
        }

        if (batch.isNotEmpty()) {
            emit(batch.toList())
        }
    }

    /**
     * Retry failed emissions.
     */
    fun <T> Flow<T>.retryWithBackoff(
        maxRetries: Int = 3,
        initialDelayMillis: Long = 100,
        maxDelayMillis: Long = 10000,
        factor: Double = 2.0
    ): Flow<T> = retryWhen { cause, attempt ->
        if (attempt < maxRetries) {
            val delay = minOf(
                initialDelayMillis * Math.pow(factor, attempt.toDouble()).toLong(),
                maxDelayMillis
            )
            kotlinx.coroutines.delay(delay)
            true
        } else {
            false
        }
    }

    /**
     * Count items in a flow.
     */
    suspend fun <T> Flow<T>.count(): Long {
        var count = 0L
        collect { count++ }
        return count
    }

    /**
     * Take first N items.
     */
    fun <T> Flow<T>.takeExactly(n: Int): Flow<T> = flow {
        var count = 0
        collect { value ->
            if (count < n) {
                emit(value)
                count++
            }
        }
    }
}
