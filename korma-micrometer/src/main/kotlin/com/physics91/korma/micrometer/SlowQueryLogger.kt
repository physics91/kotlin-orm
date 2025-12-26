package com.physics91.korma.micrometer

import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingDeque
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Logs and tracks slow database queries.
 *
 * Provides configurable thresholds and multiple notification mechanisms
 * for identifying and monitoring slow queries.
 *
 * Usage:
 * ```kotlin
 * val slowQueryLogger = SlowQueryLogger(
 *     threshold = 500.milliseconds,
 *     logLevel = SlowQueryLogger.LogLevel.WARN
 * )
 *
 * slowQueryLogger.onSlowQuery { info ->
 *     alertService.notify("Slow query detected: ${info.sql}")
 * }
 *
 * // In query execution
 * slowQueryLogger.checkAndLog(sql, params, duration)
 * ```
 */
class SlowQueryLogger(
    private val threshold: Duration = 1000.milliseconds,
    private val logLevel: LogLevel = LogLevel.WARN,
    private val maxRecentQueries: Int = 100
) {
    private val logger = LoggerFactory.getLogger(SlowQueryLogger::class.java)
    private val listeners = CopyOnWriteArrayList<(SlowQueryInfo) -> Unit>()

    // Using LinkedBlockingDeque for O(1) removal from front instead of CopyOnWriteArrayList's O(n)
    private val recentSlowQueries = LinkedBlockingDeque<SlowQueryInfo>(maxRecentQueries)

    /**
     * Log level for slow query logging.
     */
    enum class LogLevel {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    /**
     * Information about a slow query.
     */
    data class SlowQueryInfo(
        val sql: String,
        val params: List<Any?>?,
        val duration: Duration,
        val timestamp: Long = System.currentTimeMillis(),
        val threshold: Duration,
        val stackTrace: List<StackTraceElement>? = null
    ) {
        val exceedsBy: Duration get() = duration - threshold
        val percentOverThreshold: Double get() = ((duration.inWholeMilliseconds.toDouble() / threshold.inWholeMilliseconds) - 1) * 100
    }

    /**
     * Checks if a query is slow and logs it if so.
     *
     * @return true if the query was slow
     */
    fun checkAndLog(
        sql: String,
        params: List<Any?>? = null,
        duration: Duration,
        captureStackTrace: Boolean = false
    ): Boolean {
        if (duration < threshold) {
            return false
        }

        val stackTrace = if (captureStackTrace) {
            Thread.currentThread().stackTrace.drop(2).take(10).toList()
        } else null

        val info = SlowQueryInfo(
            sql = sql,
            params = params,
            duration = duration,
            threshold = threshold,
            stackTrace = stackTrace
        )

        // Log the slow query
        logSlowQuery(info)

        // Store in recent queries
        addToRecentQueries(info)

        // Notify listeners
        notifyListeners(info)

        return true
    }

    /**
     * Registers a listener for slow query notifications.
     */
    fun onSlowQuery(listener: (SlowQueryInfo) -> Unit) {
        listeners.add(listener)
    }

    /**
     * Removes a slow query listener.
     */
    fun removeListener(listener: (SlowQueryInfo) -> Unit) {
        listeners.remove(listener)
    }

    /**
     * Gets the list of recent slow queries.
     */
    fun getRecentSlowQueries(): List<SlowQueryInfo> {
        return recentSlowQueries.toList()
    }

    /**
     * Gets the count of recent slow queries.
     */
    fun getSlowQueryCount(): Int = recentSlowQueries.size

    /**
     * Clears the recent slow queries list.
     */
    fun clearHistory() {
        recentSlowQueries.clear()
    }

    /**
     * Gets statistics about slow queries.
     * Optimized to avoid intermediate list allocation.
     */
    fun getStatistics(): SlowQueryStatistics {
        val queries = recentSlowQueries.toList()

        if (queries.isEmpty()) {
            return SlowQueryStatistics(
                count = 0,
                avgDuration = Duration.ZERO,
                maxDuration = Duration.ZERO,
                minDuration = Duration.ZERO,
                totalDuration = Duration.ZERO
            )
        }

        // Single-pass calculation to avoid multiple iterations
        var sum = 0L
        var min = Long.MAX_VALUE
        var max = Long.MIN_VALUE
        queries.forEach { query ->
            val ms = query.duration.inWholeMilliseconds
            sum += ms
            if (ms < min) min = ms
            if (ms > max) max = ms
        }

        return SlowQueryStatistics(
            count = queries.size,
            avgDuration = (sum / queries.size).milliseconds,
            maxDuration = max.milliseconds,
            minDuration = min.milliseconds,
            totalDuration = sum.milliseconds
        )
    }

    private fun logSlowQuery(info: SlowQueryInfo) {
        val message = buildString {
            append("Slow query detected [${info.duration}]: ")
            append(info.sql.take(200))
            if (info.sql.length > 200) append("...")
            info.params?.let { params ->
                if (params.isNotEmpty()) {
                    append(" | params: ")
                    append(params.take(5).joinToString())
                    if (params.size > 5) append(", ...")
                }
            }
        }

        when (logLevel) {
            LogLevel.DEBUG -> logger.debug(message)
            LogLevel.INFO -> logger.info(message)
            LogLevel.WARN -> logger.warn(message)
            LogLevel.ERROR -> logger.error(message)
        }

        // Log stack trace if available
        info.stackTrace?.let { trace ->
            if (logger.isDebugEnabled) {
                logger.debug("Stack trace:\n${trace.joinToString("\n") { "  at $it" }}")
            }
        }
    }

    private fun addToRecentQueries(info: SlowQueryInfo) {
        // Bounded deque with O(1) operations - automatically evicts oldest when full
        while (!recentSlowQueries.offerLast(info)) {
            recentSlowQueries.pollFirst()
        }
    }

    private fun notifyListeners(info: SlowQueryInfo) {
        listeners.forEach { listener ->
            try {
                listener(info)
            } catch (e: Exception) {
                logger.warn("Error in slow query listener", e)
            }
        }
    }
}

/**
 * Statistics about slow queries.
 */
data class SlowQueryStatistics(
    val count: Int,
    val avgDuration: Duration,
    val maxDuration: Duration,
    val minDuration: Duration,
    val totalDuration: Duration
)

/**
 * Builder for SlowQueryLogger.
 */
class SlowQueryLoggerBuilder {
    var threshold: Duration = 1000.milliseconds
    var logLevel: SlowQueryLogger.LogLevel = SlowQueryLogger.LogLevel.WARN
    var maxRecentQueries: Int = 100
    private val listeners = mutableListOf<(SlowQueryLogger.SlowQueryInfo) -> Unit>()

    fun onSlowQuery(listener: (SlowQueryLogger.SlowQueryInfo) -> Unit) {
        listeners.add(listener)
    }

    fun build(): SlowQueryLogger {
        return SlowQueryLogger(
            threshold = threshold,
            logLevel = logLevel,
            maxRecentQueries = maxRecentQueries
        ).apply {
            listeners.forEach { onSlowQuery(it) }
        }
    }
}

/**
 * DSL function to create a SlowQueryLogger.
 */
fun slowQueryLogger(block: SlowQueryLoggerBuilder.() -> Unit): SlowQueryLogger {
    return SlowQueryLoggerBuilder().apply(block).build()
}
