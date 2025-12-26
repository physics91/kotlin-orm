package com.physics91.korma.micrometer

import io.kotest.matchers.shouldBe
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class ConnectionPoolMetricsTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var statsProvider: SimplePoolStatsProvider
    private lateinit var poolMetrics: ConnectionPoolMetrics

    @BeforeEach
    fun setup() {
        registry = SimpleMeterRegistry()
        statsProvider = SimplePoolStatsProvider(
            active = 3,
            idle = 7,
            total = 10,
            pending = 2,
            max = 20,
            min = 5
        )
        poolMetrics = ConnectionPoolMetrics(
            registry = registry,
            poolName = "test-pool",
            statsProvider = statsProvider
        )
    }

    @Nested
    inner class GaugeTests {

        @Test
        fun `should report active connections`() {
            val gauge = registry.find("korma.pool.active").gauge()
            gauge?.value() shouldBe 3.0
        }

        @Test
        fun `should report idle connections`() {
            val gauge = registry.find("korma.pool.idle").gauge()
            gauge?.value() shouldBe 7.0
        }

        @Test
        fun `should report total connections`() {
            val gauge = registry.find("korma.pool.total").gauge()
            gauge?.value() shouldBe 10.0
        }

        @Test
        fun `should report pending requests`() {
            val gauge = registry.find("korma.pool.pending").gauge()
            gauge?.value() shouldBe 2.0
        }

        @Test
        fun `should report max connections`() {
            val gauge = registry.find("korma.pool.max").gauge()
            gauge?.value() shouldBe 20.0
        }

        @Test
        fun `should report min connections`() {
            val gauge = registry.find("korma.pool.min").gauge()
            gauge?.value() shouldBe 5.0
        }

        @Test
        fun `should calculate utilization percentage`() {
            val gauge = registry.find("korma.pool.utilization").gauge()
            gauge?.value() shouldBe 15.0 // 3/20 * 100 = 15%
        }
    }

    @Nested
    inner class DynamicUpdateTests {

        @Test
        fun `should reflect stats changes`() {
            // Initial value
            registry.find("korma.pool.active").gauge()?.value() shouldBe 3.0

            // Update stats
            statsProvider.active = 10

            // Should reflect new value
            registry.find("korma.pool.active").gauge()?.value() shouldBe 10.0
        }

        @Test
        fun `should update utilization on stats change`() {
            // Initial: 3/20 = 15%
            registry.find("korma.pool.utilization").gauge()?.value() shouldBe 15.0

            // Update to 10/20 = 50%
            statsProvider.active = 10
            registry.find("korma.pool.utilization").gauge()?.value() shouldBe 50.0
        }
    }

    @Nested
    inner class TagTests {

        @Test
        fun `should tag metrics with pool name`() {
            val gauge = registry.find("korma.pool.active")
                .tag("pool", "test-pool")
                .gauge()

            gauge shouldBe registry.find("korma.pool.active").gauge()
        }
    }

    @Nested
    inner class SimplePoolStatsProviderTests {

        @Test
        fun `should return configured values`() {
            statsProvider.activeConnections() shouldBe 3
            statsProvider.idleConnections() shouldBe 7
            statsProvider.totalConnections() shouldBe 10
            statsProvider.pendingRequests() shouldBe 2
            statsProvider.maxConnections() shouldBe 20
            statsProvider.minConnections() shouldBe 5
        }
    }

    @Nested
    inner class ZeroUtilizationTests {

        @Test
        fun `should handle zero max connections`() {
            statsProvider.max = 0

            val gauge = registry.find("korma.pool.utilization").gauge()
            gauge?.value() shouldBe 0.0
        }
    }
}
