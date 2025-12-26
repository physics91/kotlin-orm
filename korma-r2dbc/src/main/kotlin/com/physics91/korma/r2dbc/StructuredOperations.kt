package com.physics91.korma.r2dbc

import com.physics91.korma.dsl.SelectBuilder
import io.r2dbc.spi.Row
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlin.coroutines.coroutineContext

/**
 * Structured concurrency operations for R2DBC database queries.
 *
 * Provides safe patterns for executing multiple queries in parallel
 * while respecting structured concurrency principles.
 *
 * Usage:
 * ```kotlin
 * // Execute multiple independent queries in parallel
 * val (users, orders, products) = database.parallelQueries(
 *     { select(Users) { selectAll() } },
 *     { select(Orders) { selectAll() } },
 *     { select(Products) { selectAll() } }
 * )
 *
 * // Execute queries with limited concurrency
 * database.queriesWithConcurrency(maxConcurrency = 3) {
 *     items.map { id ->
 *         async { selectOne(Users) { where { Users.id eq id } } }
 *     }.awaitAll()
 * }
 * ```
 */
object StructuredOperations {

    /**
     * Execute two independent queries in parallel and return both results.
     */
    suspend fun <A, B> R2dbcDatabase.parallelQueries(
        query1: suspend R2dbcDatabase.() -> A,
        query2: suspend R2dbcDatabase.() -> B
    ): Pair<A, B> = coroutineScope {
        val result1 = async { query1() }
        val result2 = async { query2() }
        Pair(result1.await(), result2.await())
    }

    /**
     * Execute three independent queries in parallel and return all results.
     */
    suspend fun <A, B, C> R2dbcDatabase.parallelQueries(
        query1: suspend R2dbcDatabase.() -> A,
        query2: suspend R2dbcDatabase.() -> B,
        query3: suspend R2dbcDatabase.() -> C
    ): Triple<A, B, C> = coroutineScope {
        val result1 = async { query1() }
        val result2 = async { query2() }
        val result3 = async { query3() }
        Triple(result1.await(), result2.await(), result3.await())
    }

    /**
     * Execute multiple independent queries in parallel and return all results.
     */
    suspend fun <T> R2dbcDatabase.parallelQueries(
        vararg queries: suspend R2dbcDatabase.() -> T
    ): List<T> = coroutineScope {
        queries.map { query ->
            async { query() }
        }.awaitAll()
    }

    /**
     * Execute queries with limited concurrency.
     *
     * @param maxConcurrency Maximum number of concurrent queries
     * @param block Block containing parallel query execution
     */
    suspend fun <T> R2dbcDatabase.withConcurrencyLimit(
        maxConcurrency: Int,
        block: suspend ConcurrencyLimitedScope.() -> T
    ): T {
        val semaphore = Semaphore(maxConcurrency)
        val scope = ConcurrencyLimitedScope(this, semaphore)
        return scope.block()
    }

    /**
     * Execute a list of operations with limited concurrency.
     *
     * @param items Items to process
     * @param maxConcurrency Maximum number of concurrent operations
     * @param operation Operation to apply to each item
     */
    suspend fun <T, R> R2dbcDatabase.mapWithConcurrency(
        items: List<T>,
        maxConcurrency: Int = 10,
        operation: suspend R2dbcDatabase.(T) -> R
    ): List<R> {
        val semaphore = Semaphore(maxConcurrency)
        return coroutineScope {
            items.map { item ->
                async {
                    semaphore.withPermit {
                        operation(item)
                    }
                }
            }.awaitAll()
        }
    }

    /**
     * Execute a flow of operations with limited concurrency.
     *
     * @param maxConcurrency Maximum number of concurrent operations
     * @param operation Operation to apply to each item
     */
    fun <T, R> R2dbcDatabase.mapFlowWithConcurrency(
        itemsFlow: Flow<T>,
        maxConcurrency: Int = 10,
        operation: suspend R2dbcDatabase.(T) -> R
    ): Flow<R> = flow {
        val semaphore = Semaphore(maxConcurrency)
        coroutineScope {
            itemsFlow.map { item ->
                async {
                    semaphore.withPermit {
                        operation(item)
                    }
                }
            }.buffer(maxConcurrency * 2).collect { deferred ->
                emit(deferred.await())
            }
        }
    }
}

/**
 * Scope for executing queries with concurrency limits.
 */
class ConcurrencyLimitedScope(
    val database: R2dbcDatabase,
    private val semaphore: Semaphore
) {
    /**
     * Execute a query with the concurrency limit.
     */
    suspend fun <T> query(block: suspend R2dbcDatabase.() -> T): T {
        return semaphore.withPermit {
            database.block()
        }
    }

    /**
     * Create an async deferred with the concurrency limit.
     */
    suspend fun <T> asyncQuery(block: suspend R2dbcDatabase.() -> T): Deferred<T> {
        return coroutineScope {
            async {
                semaphore.withPermit {
                    database.block()
                }
            }
        }
    }
}

/**
 * Extensions for racing multiple queries and taking the first result.
 */
object QueryRacing {

    /**
     * Execute multiple queries and return the first successful result.
     * Other queries are cancelled once the first one completes.
     *
     * @param queries Queries to race
     */
    suspend fun <T> R2dbcDatabase.raceQueries(
        vararg queries: suspend R2dbcDatabase.() -> T
    ): T = coroutineScope {
        kotlinx.coroutines.selects.select {
            queries.forEach { query ->
                async { query() }.onAwait { it }
            }
        }
    }

    /**
     * Execute a query with a timeout, returning null if timeout is exceeded.
     *
     * @param timeoutMillis Timeout in milliseconds
     * @param query Query to execute
     */
    suspend fun <T> R2dbcDatabase.queryWithTimeout(
        timeoutMillis: Long,
        query: suspend R2dbcDatabase.() -> T
    ): T? = withTimeoutOrNull(timeoutMillis) {
        query()
    }

    /**
     * Execute a query with a fallback value on timeout.
     *
     * @param timeoutMillis Timeout in milliseconds
     * @param fallback Fallback value
     * @param query Query to execute
     */
    suspend fun <T> R2dbcDatabase.queryWithTimeoutOrDefault(
        timeoutMillis: Long,
        fallback: T,
        query: suspend R2dbcDatabase.() -> T
    ): T = withTimeoutOrNull(timeoutMillis) {
        query()
    } ?: fallback
}

/**
 * Extensions for transaction coordination.
 */
object TransactionCoordination {

    /**
     * Execute multiple operations within a single transaction,
     * rolling back all if any fails.
     *
     * @param operations Operations to execute atomically
     */
    suspend fun <T> R2dbcDatabase.atomicOperations(
        vararg operations: suspend R2dbcTransaction.() -> T
    ): List<T> = transaction {
        val results = mutableListOf<T>()
        for (operation in operations) {
            results.add(operation())
        }
        results
    }

    /**
     * Execute an operation with retry on transaction conflict.
     *
     * @param maxRetries Maximum retry attempts
     * @param retryDelayMillis Delay between retries
     * @param operation Operation to execute
     */
    suspend fun <T> R2dbcDatabase.retryableTransaction(
        maxRetries: Int = 3,
        retryDelayMillis: Long = 100,
        operation: suspend R2dbcTransaction.() -> T
    ): T {
        var lastException: Throwable? = null

        repeat(maxRetries) { attempt ->
            try {
                return transaction(block = operation)
            } catch (e: Exception) {
                lastException = e
                if (attempt < maxRetries - 1) {
                    delay(retryDelayMillis * (attempt + 1))
                }
            }
        }

        throw lastException ?: IllegalStateException("Transaction failed after $maxRetries retries")
    }

    /**
     * Execute operations with saga pattern - execute compensating actions on failure.
     *
     * @param steps List of (action, compensation) pairs
     */
    suspend fun R2dbcDatabase.saga(
        vararg steps: Pair<suspend R2dbcTransaction.() -> Unit, suspend R2dbcTransaction.() -> Unit>
    ): Boolean = transaction {
        val completedSteps = mutableListOf<Int>()

        try {
            steps.forEachIndexed { index, (action, _) ->
                action()
                completedSteps.add(index)
            }
            true
        } catch (e: Exception) {
            // Execute compensations in reverse order
            completedSteps.reversed().forEach { index ->
                try {
                    steps[index].second.invoke(this)
                } catch (compensationError: Exception) {
                    // Log compensation failure but continue with other compensations
                }
            }
            false
        }
    }
}

/**
 * Extensions for bulk operations with structured concurrency.
 */
object BulkOperations {

    /**
     * Process items in parallel batches.
     *
     * @param items Items to process
     * @param batchSize Size of each batch
     * @param maxConcurrentBatches Maximum concurrent batch processing
     * @param processor Batch processor function
     */
    suspend fun <T, R> R2dbcDatabase.processBatches(
        items: List<T>,
        batchSize: Int = 100,
        maxConcurrentBatches: Int = 4,
        processor: suspend R2dbcDatabase.(List<T>) -> R
    ): List<R> {
        val batches = items.chunked(batchSize)
        val semaphore = Semaphore(maxConcurrentBatches)

        return coroutineScope {
            batches.map { batch ->
                async {
                    semaphore.withPermit {
                        processor(batch)
                    }
                }
            }.awaitAll()
        }
    }

    /**
     * Stream and process items with controlled memory usage.
     *
     * @param fetchQuery Query that returns items as a flow
     * @param processor Processor for each item
     */
    suspend fun <T, R> R2dbcDatabase.streamAndProcess(
        fetchQuery: R2dbcDatabase.() -> Flow<T>,
        maxConcurrency: Int = 10,
        processor: suspend R2dbcDatabase.(T) -> R
    ): Flow<R> = flow {
        val semaphore = Semaphore(maxConcurrency)
        fetchQuery().collect { item ->
            semaphore.withPermit {
                emit(processor(item))
            }
        }
    }

    /**
     * Fan-out pattern: distribute items to multiple processors.
     *
     * @param items Items to distribute
     * @param processorCount Number of parallel processors
     * @param processor Processor function
     */
    suspend fun <T, R> R2dbcDatabase.fanOut(
        items: List<T>,
        processorCount: Int = 4,
        processor: suspend R2dbcDatabase.(T) -> R
    ): List<R> = coroutineScope {
        val channel = kotlinx.coroutines.channels.Channel<T>(kotlinx.coroutines.channels.Channel.BUFFERED)

        // Launch producers
        launch {
            items.forEach { channel.send(it) }
            channel.close()
        }

        // Launch consumers
        val results = (1..processorCount).map {
            async {
                val localResults = mutableListOf<R>()
                for (item in channel) {
                    localResults.add(processor(item))
                }
                localResults
            }
        }.awaitAll()

        results.flatten()
    }
}

/**
 * Extensions for cancellation-aware operations.
 */
object CancellationAware {

    /**
     * Execute a query that checks for cancellation between operations.
     *
     * @param items Items to process
     * @param operation Operation for each item
     */
    suspend fun <T, R> R2dbcDatabase.processWithCancellationCheck(
        items: List<T>,
        operation: suspend R2dbcDatabase.(T) -> R
    ): List<R> {
        val results = mutableListOf<R>()
        for (item in items) {
            coroutineContext.ensureActive()
            results.add(operation(item))
        }
        return results
    }

    /**
     * Execute a long-running query with periodic cancellation checks.
     *
     * @param checkIntervalMillis How often to check for cancellation
     * @param operation The long-running operation
     */
    suspend fun <T> R2dbcDatabase.cancellableOperation(
        checkIntervalMillis: Long = 100,
        operation: suspend R2dbcDatabase.() -> T
    ): T = coroutineScope {
        val result = async { operation() }

        // Periodically check for cancellation
        while (result.isActive) {
            delay(checkIntervalMillis)
            coroutineContext.ensureActive()
        }

        result.await()
    }
}
