package com.physics91.korma.micrometer

import io.kotest.matchers.doubles.shouldBeGreaterThan
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class KormaMetricsTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var metrics: KormaMetrics

    @BeforeEach
    fun setup() {
        registry = SimpleMeterRegistry()
        metrics = KormaMetrics(registry, slowQueryThreshold = 100.milliseconds)
    }

    @Nested
    inner class QueryMetricsTests {

        @Test
        fun `should record successful query`() {
            metrics.recordQuery("users", QueryType.SELECT, 50.milliseconds, true, 10)

            val successCount = registry.counter("korma.query.success").count()
            successCount shouldBe 1.0
        }

        @Test
        fun `should record failed query`() {
            metrics.recordQuery("users", QueryType.INSERT, 50.milliseconds, false)

            val failureCount = registry.counter("korma.query.failure").count()
            failureCount shouldBe 1.0
        }

        @Test
        fun `should record slow query`() {
            metrics.recordQuery("users", QueryType.SELECT, 200.milliseconds, true)

            val slowCount = registry.counter("korma.query.slow").count()
            slowCount shouldBe 1.0
        }

        @Test
        fun `should not count fast query as slow`() {
            metrics.recordQuery("users", QueryType.SELECT, 50.milliseconds, true)

            val slowCount = registry.counter("korma.query.slow").count()
            slowCount shouldBe 0.0
        }

        @Test
        fun `timeQuery should measure execution time`() {
            var result = ""

            metrics.timeQuery(table = "users", queryType = QueryType.SELECT) {
                Thread.sleep(10)
                result = "success"
            }

            result shouldBe "success"
            metrics.getActiveQueryCount() shouldBe 0
        }

        @Test
        fun `timeQuery should handle exceptions`() {
            var caught = false

            try {
                metrics.timeQuery(table = "users") {
                    throw RuntimeException("Query failed")
                }
            } catch (e: RuntimeException) {
                caught = true
            }

            caught shouldBe true
            registry.counter("korma.query.failure").count() shouldBe 1.0
        }
    }

    @Nested
    inner class TransactionMetricsTests {

        @Test
        fun `should record committed transaction`() {
            metrics.recordTransaction(100.milliseconds, committed = true)

            val commitCount = registry.counter("korma.transaction.commit").count()
            commitCount shouldBe 1.0
        }

        @Test
        fun `should record rolled back transaction`() {
            metrics.recordTransaction(100.milliseconds, committed = false)

            val rollbackCount = registry.counter("korma.transaction.rollback").count()
            rollbackCount shouldBe 1.0
        }

        @Test
        fun `timeTransaction should measure execution`() {
            val result = metrics.timeTransaction {
                "completed"
            }

            result shouldBe "completed"
            registry.counter("korma.transaction.commit").count() shouldBe 1.0
        }

        @Test
        fun `timeTransaction should record rollback on exception`() {
            try {
                metrics.timeTransaction<String> {
                    throw RuntimeException("Failed")
                }
            } catch (e: RuntimeException) {
                // expected
            }

            registry.counter("korma.transaction.rollback").count() shouldBe 1.0
        }
    }

    @Nested
    inner class ConnectionMetricsTests {

        @Test
        fun `should record connection acquisition time`() {
            metrics.recordConnectionAcquisition(10.milliseconds)

            val timer = registry.timer("korma.connection.acquire")
            timer.count() shouldBe 1
        }

        @Test
        fun `should record connection timeout`() {
            metrics.recordConnectionTimeout()

            val count = registry.counter("korma.error.connection.timeout").count()
            count shouldBe 1.0
        }
    }

    @Nested
    inner class ErrorMetricsTests {

        @Test
        fun `should record deadlock`() {
            metrics.recordDeadlock()

            val count = registry.counter("korma.error.deadlock").count()
            count shouldBe 1.0
        }
    }

    @Nested
    inner class BatchMetricsTests {

        @Test
        fun `should record batch size`() {
            metrics.recordBatch(100)

            val summary = registry.summary("korma.batch.size")
            summary.count() shouldBe 1
            summary.totalAmount() shouldBe 100.0
        }
    }

    @Nested
    inner class GaugeTests {

        @Test
        fun `should track active queries`() {
            metrics.getActiveQueryCount() shouldBe 0
        }

        @Test
        fun `should track active transactions`() {
            metrics.getActiveTransactionCount() shouldBe 0
        }
    }

    @Nested
    inner class DSLTests {

        @Test
        fun `kormaMetrics DSL should create valid metrics`() {
            val customMetrics = kormaMetrics(registry) {
                prefix = "custom"
                slowQueryThreshold = 500.milliseconds
            }

            customMetrics.recordQuery("test", QueryType.SELECT, 100.milliseconds, true)

            val counter = registry.counter("custom.query.success")
            counter.count() shouldBe 1.0
        }
    }

    @Nested
    inner class TimerSampleTests {

        @Test
        fun `should start and stop query timer`() {
            val sample = metrics.startQueryTimer()
            Thread.sleep(10)
            metrics.stopQueryTimer(sample, "users", QueryType.SELECT, true)

            registry.counter("korma.query.success").count() shouldBe 1.0
        }
    }
}
