package com.physics91.korma.micrometer

import io.kotest.matchers.doubles.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class CacheMetricsTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var cacheMetrics: CacheMetrics

    @BeforeEach
    fun setup() {
        registry = SimpleMeterRegistry()
        cacheMetrics = CacheMetrics(registry, cacheName = "test-cache")
    }

    @Nested
    inner class HitMissTests {

        @Test
        fun `should record cache hit`() {
            cacheMetrics.recordHit()

            cacheMetrics.getHitCount() shouldBe 1
            registry.counter("korma.cache.hits", "cache", "test-cache").count() shouldBe 1.0
        }

        @Test
        fun `should record cache miss`() {
            cacheMetrics.recordMiss()

            cacheMetrics.getMissCount() shouldBe 1
            registry.counter("korma.cache.misses", "cache", "test-cache").count() shouldBe 1.0
        }

        @Test
        fun `should calculate hit rate correctly`() {
            cacheMetrics.recordHit()
            cacheMetrics.recordHit()
            cacheMetrics.recordHit()
            cacheMetrics.recordMiss()

            cacheMetrics.getHitRate() shouldBe 75.0
        }

        @Test
        fun `should return zero hit rate when no requests`() {
            cacheMetrics.getHitRate() shouldBe 0.0
        }
    }

    @Nested
    inner class PutEvictionTests {

        @Test
        fun `should record cache put`() {
            cacheMetrics.recordPut()

            registry.counter("korma.cache.puts", "cache", "test-cache").count() shouldBe 1.0
        }

        @Test
        fun `should record cache eviction`() {
            cacheMetrics.recordEviction()

            registry.counter("korma.cache.evictions", "cache", "test-cache").count() shouldBe 1.0
        }
    }

    @Nested
    inner class SizeTests {

        @Test
        fun `should update cache size`() {
            cacheMetrics.updateSize(100)

            cacheMetrics.getStatistics().size shouldBe 100
        }

        @Test
        fun `should increment cache size`() {
            cacheMetrics.incrementSize()
            cacheMetrics.incrementSize()

            cacheMetrics.getStatistics().size shouldBe 2
        }

        @Test
        fun `should decrement cache size`() {
            cacheMetrics.updateSize(10)
            cacheMetrics.decrementSize()

            cacheMetrics.getStatistics().size shouldBe 9
        }
    }

    @Nested
    inner class TimingTests {

        @Test
        fun `should record get with timing`() {
            cacheMetrics.recordGet(10.milliseconds, hit = true)

            cacheMetrics.getHitCount() shouldBe 1
            val timer = registry.timer("korma.cache.get.duration", "cache", "test-cache")
            timer.count() shouldBe 1
        }

        @Test
        fun `should record put with timing`() {
            cacheMetrics.recordPut(5.milliseconds)

            val timer = registry.timer("korma.cache.put.duration", "cache", "test-cache")
            timer.count() shouldBe 1
        }

        @Test
        fun `timeGet should measure and record hit`() {
            val result = cacheMetrics.timeGet { "value" }

            result shouldBe "value"
            cacheMetrics.getHitCount() shouldBe 1
        }

        @Test
        fun `timeGet should measure and record miss`() {
            val result = cacheMetrics.timeGet { null }

            result shouldBe null
            cacheMetrics.getMissCount() shouldBe 1
        }

        @Test
        fun `timePut should measure and record`() {
            val result = cacheMetrics.timePut { "stored" }

            result shouldBe "stored"
            registry.counter("korma.cache.puts", "cache", "test-cache").count() shouldBe 1.0
        }
    }

    @Nested
    inner class StatisticsTests {

        @Test
        fun `should return correct statistics`() {
            cacheMetrics.recordHit()
            cacheMetrics.recordHit()
            cacheMetrics.recordMiss()
            cacheMetrics.recordPut()
            cacheMetrics.recordEviction()
            cacheMetrics.updateSize(50)

            val stats = cacheMetrics.getStatistics()

            stats.hits shouldBe 2
            stats.misses shouldBe 1
            stats.puts shouldBe 1
            stats.evictions shouldBe 1
            stats.size shouldBe 50
            stats.hitRate shouldBe (2.0 / 3.0 * 100)
            stats.requests shouldBe 3
        }

        @Test
        fun `should reset counters`() {
            cacheMetrics.recordHit()
            cacheMetrics.recordMiss()
            cacheMetrics.recordPut()
            cacheMetrics.recordEviction()

            cacheMetrics.reset()

            cacheMetrics.getHitCount() shouldBe 0
            cacheMetrics.getMissCount() shouldBe 0
        }
    }

    @Nested
    inner class DSLTests {

        @Test
        fun `cacheMetrics DSL should create valid metrics`() {
            val customMetrics = cacheMetrics(registry) {
                cacheName = "custom-cache"
                prefix = "custom.cache"
            }

            customMetrics.recordHit()

            registry.counter("custom.cache.hits", "cache", "custom-cache").count() shouldBe 1.0
        }
    }
}
