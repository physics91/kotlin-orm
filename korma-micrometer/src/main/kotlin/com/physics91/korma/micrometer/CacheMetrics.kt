package com.physics91.korma.micrometer

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.MeterBinder
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/**
 * Metrics collector for Korma cache operations.
 *
 * Provides visibility into:
 * - Cache hit/miss rates
 * - Cache size
 * - Evictions
 * - Operation latencies
 *
 * Usage:
 * ```kotlin
 * val cacheMetrics = CacheMetrics(
 *     registry = meterRegistry,
 *     cacheName = "query-cache"
 * )
 *
 * // Record cache operations
 * cacheMetrics.recordHit()
 * cacheMetrics.recordMiss()
 * ```
 */
class CacheMetrics(
    private val registry: MeterRegistry,
    private val cacheName: String = "default",
    private val prefix: String = "korma.cache"
) : MeterBinder {

    private val hits = AtomicLong(0)
    private val misses = AtomicLong(0)
    private val puts = AtomicLong(0)
    private val evictions = AtomicLong(0)
    private val currentSize = AtomicLong(0)

    private lateinit var hitCounter: Counter
    private lateinit var missCounter: Counter
    private lateinit var putCounter: Counter
    private lateinit var evictionCounter: Counter
    private lateinit var getTimer: Timer
    private lateinit var putTimer: Timer

    override fun bindTo(registry: MeterRegistry) {
        val tags = listOf(Tag.of("cache", cacheName))

        hitCounter = Counter.builder("$prefix.hits")
            .description("Number of cache hits")
            .tags(tags)
            .register(registry)

        missCounter = Counter.builder("$prefix.misses")
            .description("Number of cache misses")
            .tags(tags)
            .register(registry)

        putCounter = Counter.builder("$prefix.puts")
            .description("Number of cache puts")
            .tags(tags)
            .register(registry)

        evictionCounter = Counter.builder("$prefix.evictions")
            .description("Number of cache evictions")
            .tags(tags)
            .register(registry)

        getTimer = Timer.builder("$prefix.get.duration")
            .description("Cache get operation duration")
            .tags(tags)
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)

        putTimer = Timer.builder("$prefix.put.duration")
            .description("Cache put operation duration")
            .tags(tags)
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)

        Gauge.builder("$prefix.size", currentSize::get)
            .description("Current cache size")
            .tags(tags)
            .register(registry)

        Gauge.builder("$prefix.hit.rate") { getHitRate() }
            .description("Cache hit rate")
            .tags(tags)
            .baseUnit("percent")
            .register(registry)
    }

    /**
     * Records a cache hit.
     */
    fun recordHit() {
        hits.incrementAndGet()
        hitCounter.increment()
    }

    /**
     * Records a cache miss.
     */
    fun recordMiss() {
        misses.incrementAndGet()
        missCounter.increment()
    }

    /**
     * Records a cache put.
     */
    fun recordPut() {
        puts.incrementAndGet()
        putCounter.increment()
    }

    /**
     * Records a cache eviction.
     */
    fun recordEviction() {
        evictions.incrementAndGet()
        evictionCounter.increment()
    }

    /**
     * Records a get operation with timing.
     */
    fun recordGet(duration: Duration, hit: Boolean) {
        getTimer.record(duration.inWholeNanoseconds, TimeUnit.NANOSECONDS)
        if (hit) recordHit() else recordMiss()
    }

    /**
     * Records a put operation with timing.
     */
    fun recordPut(duration: Duration) {
        putTimer.record(duration.inWholeNanoseconds, TimeUnit.NANOSECONDS)
        recordPut()
    }

    /**
     * Times a cache get operation.
     */
    fun <T> timeGet(block: () -> T?): T? {
        val startTime = System.nanoTime()
        val result = block()
        val durationNanos = System.nanoTime() - startTime

        if (result != null) {
            recordHit()
        } else {
            recordMiss()
        }
        getTimer.record(durationNanos, TimeUnit.NANOSECONDS)

        return result
    }

    /**
     * Times a cache put operation.
     */
    fun <T> timePut(block: () -> T): T {
        val startTime = System.nanoTime()
        val result = block()
        val durationNanos = System.nanoTime() - startTime

        recordPut()
        putTimer.record(durationNanos, TimeUnit.NANOSECONDS)

        return result
    }

    /**
     * Updates the current cache size.
     */
    fun updateSize(size: Long) {
        currentSize.set(size)
    }

    /**
     * Increments the current cache size.
     */
    fun incrementSize() {
        currentSize.incrementAndGet()
    }

    /**
     * Decrements the current cache size.
     */
    fun decrementSize() {
        currentSize.decrementAndGet()
    }

    /**
     * Gets the current hit rate as a percentage.
     */
    fun getHitRate(): Double {
        val total = hits.get() + misses.get()
        return if (total > 0) (hits.get().toDouble() / total) * 100 else 0.0
    }

    /**
     * Gets the total number of hits.
     */
    fun getHitCount(): Long = hits.get()

    /**
     * Gets the total number of misses.
     */
    fun getMissCount(): Long = misses.get()

    /**
     * Gets the total number of requests.
     */
    fun getRequestCount(): Long = hits.get() + misses.get()

    /**
     * Gets cache statistics.
     */
    fun getStatistics(): CacheStatistics = CacheStatistics(
        hits = hits.get(),
        misses = misses.get(),
        puts = puts.get(),
        evictions = evictions.get(),
        size = currentSize.get(),
        hitRate = getHitRate()
    )

    /**
     * Resets all counters.
     */
    fun reset() {
        hits.set(0)
        misses.set(0)
        puts.set(0)
        evictions.set(0)
    }

    init {
        bindTo(registry)
    }
}

/**
 * Cache statistics snapshot.
 */
data class CacheStatistics(
    val hits: Long,
    val misses: Long,
    val puts: Long,
    val evictions: Long,
    val size: Long,
    val hitRate: Double
) {
    val requests: Long get() = hits + misses
}

/**
 * Builder for CacheMetrics.
 */
class CacheMetricsBuilder(private val registry: MeterRegistry) {
    var cacheName: String = "default"
    var prefix: String = "korma.cache"

    fun build(): CacheMetrics = CacheMetrics(
        registry = registry,
        cacheName = cacheName,
        prefix = prefix
    )
}

/**
 * DSL function to create CacheMetrics.
 */
fun cacheMetrics(registry: MeterRegistry, block: CacheMetricsBuilder.() -> Unit = {}): CacheMetrics {
    return CacheMetricsBuilder(registry).apply(block).build()
}
