package com.physics91.korma.micrometer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.binder.MeterBinder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds

/**
 * Central metrics collection for Korma ORM operations.
 *
 * Provides comprehensive metrics for:
 * - Query execution (duration, success/failure)
 * - Connection pool statistics
 * - Cache performance
 * - Transaction metrics
 *
 * Usage:
 * ```kotlin
 * val metrics = KormaMetrics(meterRegistry)
 *
 * // Record query execution
 * metrics.recordQuery("users", QueryType.SELECT, 15.milliseconds, true)
 *
 * // Get metrics for slow query detection
 * val slowQueryCount = metrics.getSlowQueryCount()
 * ```
 */
class KormaMetrics(
    private val registry: MeterRegistry,
    private val prefix: String = "korma",
    private val slowQueryThreshold: Duration = 1000.milliseconds
) : MeterBinder {

    companion object {
        /** Default percentiles for query and transaction timers. */
        private val DEFAULT_PERCENTILES = doubleArrayOf(0.5, 0.75, 0.9, 0.95, 0.99)
        /** Percentiles for connection timers. */
        private val CONNECTION_PERCENTILES = doubleArrayOf(0.5, 0.9, 0.99)
    }

    private val activeQueries = AtomicLong(0)
    private val activeTransactions = AtomicLong(0)

    // Timer cache for performance optimization - avoids creating Timer on every call
    private val queryTimerCache = ConcurrentHashMap<String, Timer>()

    // Counters
    private lateinit var querySuccessCounter: Counter
    private lateinit var queryFailureCounter: Counter
    private lateinit var slowQueryCounter: Counter
    private lateinit var transactionCommitCounter: Counter
    private lateinit var transactionRollbackCounter: Counter
    private lateinit var deadlockCounter: Counter
    private lateinit var connectionTimeoutCounter: Counter

    // Timers
    private lateinit var queryTimer: Timer
    private lateinit var transactionTimer: Timer
    private lateinit var connectionAcquireTimer: Timer

    // Distribution summaries
    private lateinit var resultSetSizeSummary: DistributionSummary
    private lateinit var batchSizeSummary: DistributionSummary

    override fun bindTo(registry: MeterRegistry) {
        // Query counters
        querySuccessCounter = Counter.builder("$prefix.query.success")
            .description("Number of successful queries")
            .register(registry)

        queryFailureCounter = Counter.builder("$prefix.query.failure")
            .description("Number of failed queries")
            .register(registry)

        slowQueryCounter = Counter.builder("$prefix.query.slow")
            .description("Number of slow queries")
            .register(registry)

        // Transaction counters
        transactionCommitCounter = Counter.builder("$prefix.transaction.commit")
            .description("Number of committed transactions")
            .register(registry)

        transactionRollbackCounter = Counter.builder("$prefix.transaction.rollback")
            .description("Number of rolled back transactions")
            .register(registry)

        // Error counters
        deadlockCounter = Counter.builder("$prefix.error.deadlock")
            .description("Number of deadlock occurrences")
            .register(registry)

        connectionTimeoutCounter = Counter.builder("$prefix.error.connection.timeout")
            .description("Number of connection timeouts")
            .register(registry)

        // Timers
        queryTimer = Timer.builder("$prefix.query.duration")
            .description("Query execution time")
            .publishPercentiles(*DEFAULT_PERCENTILES)
            .register(registry)

        transactionTimer = Timer.builder("$prefix.transaction.duration")
            .description("Transaction duration")
            .publishPercentiles(*DEFAULT_PERCENTILES)
            .register(registry)

        connectionAcquireTimer = Timer.builder("$prefix.connection.acquire")
            .description("Time to acquire a connection from the pool")
            .publishPercentiles(*CONNECTION_PERCENTILES)
            .register(registry)

        // Distribution summaries
        resultSetSizeSummary = DistributionSummary.builder("$prefix.query.result.size")
            .description("Number of rows returned by queries")
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)

        batchSizeSummary = DistributionSummary.builder("$prefix.batch.size")
            .description("Number of operations in batch executions")
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)

        // Gauges for active operations
        Gauge.builder("$prefix.query.active", activeQueries::get)
            .description("Number of currently active queries")
            .register(registry)

        Gauge.builder("$prefix.transaction.active", activeTransactions::get)
            .description("Number of currently active transactions")
            .register(registry)
    }

    /**
     * Records a query execution.
     */
    fun recordQuery(
        table: String?,
        queryType: QueryType,
        duration: Duration,
        success: Boolean,
        resultSize: Int? = null
    ) {
        val timer = getOrCreateQueryTimer(table, queryType, success)
        timer.record(duration.inWholeNanoseconds, TimeUnit.NANOSECONDS)
        recordQueryOutcome(success, resultSize)

        if (duration > slowQueryThreshold) {
            slowQueryCounter.increment()
        }
    }

    /**
     * Builds query tags for metrics (DRY: Single source of truth for tag building).
     */
    private fun buildQueryTags(table: String?, queryType: QueryType, success: Boolean): List<Tag> =
        buildList {
            table?.let { add(Tag.of("table", it)) }
            add(Tag.of("type", queryType.name.lowercase()))
            add(Tag.of("success", success.toString()))
        }

    /**
     * Gets or creates a cached Timer for the given query parameters.
     */
    private fun getOrCreateQueryTimer(table: String?, queryType: QueryType, success: Boolean): Timer {
        val cacheKey = "${table ?: "unknown"}_${queryType.name}_$success"
        return queryTimerCache.getOrPut(cacheKey) {
            val tags = buildQueryTags(table, queryType, success)
            Timer.builder("$prefix.query.duration")
                .tags(tags)
                .publishPercentiles(*DEFAULT_PERCENTILES)
                .register(registry)
        }
    }

    /**
     * Records query outcome (success/failure and result size).
     */
    private fun recordQueryOutcome(success: Boolean, resultSize: Int? = null) {
        if (success) {
            querySuccessCounter.increment()
            resultSize?.let { resultSetSizeSummary.record(it.toDouble()) }
        } else {
            queryFailureCounter.increment()
        }
    }

    /**
     * Times a query execution.
     */
    fun <T> timeQuery(
        table: String? = null,
        queryType: QueryType = QueryType.SELECT,
        block: () -> T
    ): T {
        activeQueries.incrementAndGet()
        val startTime = System.nanoTime()
        var success = true

        return try {
            block()
        } catch (e: Exception) {
            success = false
            throw e
        } finally {
            val durationNanos = System.nanoTime() - startTime
            activeQueries.decrementAndGet()
            recordQuery(table, queryType, durationNanos.nanoseconds, success)
        }
    }

    /**
     * Records a transaction outcome.
     */
    fun recordTransaction(
        duration: Duration,
        committed: Boolean
    ) {
        transactionTimer.record(duration.inWholeNanoseconds, TimeUnit.NANOSECONDS)

        if (committed) {
            transactionCommitCounter.increment()
        } else {
            transactionRollbackCounter.increment()
        }
    }

    /**
     * Times a transaction execution.
     */
    fun <T> timeTransaction(block: () -> T): T {
        activeTransactions.incrementAndGet()
        val startTime = System.nanoTime()
        var committed = true

        return try {
            block()
        } catch (e: Exception) {
            committed = false
            throw e
        } finally {
            val durationNanos = System.nanoTime() - startTime
            activeTransactions.decrementAndGet()
            recordTransaction(durationNanos.nanoseconds, committed)
        }
    }

    /**
     * Records a connection acquisition.
     */
    fun recordConnectionAcquisition(duration: Duration) {
        connectionAcquireTimer.record(duration.inWholeNanoseconds, TimeUnit.NANOSECONDS)
    }

    /**
     * Records a connection timeout.
     */
    fun recordConnectionTimeout() {
        connectionTimeoutCounter.increment()
    }

    /**
     * Records a deadlock occurrence.
     */
    fun recordDeadlock() {
        deadlockCounter.increment()
    }

    /**
     * Records a batch operation.
     */
    fun recordBatch(size: Int) {
        batchSizeSummary.record(size.toDouble())
    }

    /**
     * Gets the current count of slow queries.
     */
    fun getSlowQueryCount(): Long = slowQueryCounter.count().toLong()

    /**
     * Gets the current count of active queries.
     */
    fun getActiveQueryCount(): Long = activeQueries.get()

    /**
     * Gets the current count of active transactions.
     */
    fun getActiveTransactionCount(): Long = activeTransactions.get()

    /**
     * Creates a timer sample that can be stopped later.
     */
    fun startQueryTimer(): Timer.Sample = Timer.start(registry)

    /**
     * Stops a timer sample and records the duration.
     */
    fun stopQueryTimer(sample: Timer.Sample, table: String?, queryType: QueryType, success: Boolean) {
        val timer = getOrCreateQueryTimer(table, queryType, success)
        sample.stop(timer)
        recordQueryOutcome(success)
    }

    init {
        bindTo(registry)
    }
}

/**
 * Types of database queries for metric categorization.
 */
enum class QueryType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    DDL,
    BATCH,
    OTHER
}

/**
 * Builder for KormaMetrics configuration.
 */
class KormaMetricsBuilder(private val registry: MeterRegistry) {
    var prefix: String = "korma"
    var slowQueryThreshold: Duration = 1000.milliseconds

    fun build(): KormaMetrics = KormaMetrics(
        registry = registry,
        prefix = prefix,
        slowQueryThreshold = slowQueryThreshold
    )
}

/**
 * DSL function to create KormaMetrics.
 */
fun kormaMetrics(registry: MeterRegistry, block: KormaMetricsBuilder.() -> Unit = {}): KormaMetrics {
    return KormaMetricsBuilder(registry).apply(block).build()
}
