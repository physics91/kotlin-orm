package com.physics91.korma.micrometer

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class SlowQueryLoggerTest {

    private lateinit var logger: SlowQueryLogger

    @BeforeEach
    fun setup() {
        logger = SlowQueryLogger(
            threshold = 100.milliseconds,
            logLevel = SlowQueryLogger.LogLevel.DEBUG,
            maxRecentQueries = 10
        )
    }

    @Nested
    inner class DetectionTests {

        @Test
        fun `should detect slow query`() {
            val result = logger.checkAndLog("SELECT * FROM users", null, 150.milliseconds)
            result shouldBe true
        }

        @Test
        fun `should not flag fast query`() {
            val result = logger.checkAndLog("SELECT * FROM users", null, 50.milliseconds)
            result shouldBe false
        }

        @Test
        fun `should store slow query info`() {
            logger.checkAndLog("SELECT * FROM users", listOf(1L), 200.milliseconds)

            val queries = logger.getRecentSlowQueries()
            queries shouldHaveSize 1
            queries[0].sql shouldBe "SELECT * FROM users"
            queries[0].params shouldBe listOf(1L)
            queries[0].duration shouldBe 200.milliseconds
        }

        @Test
        fun `should calculate exceedsBy correctly`() {
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)

            val info = logger.getRecentSlowQueries()[0]
            info.exceedsBy shouldBe 50.milliseconds
        }

        @Test
        fun `should calculate percentOverThreshold correctly`() {
            logger.checkAndLog("SELECT 1", null, 200.milliseconds)

            val info = logger.getRecentSlowQueries()[0]
            info.percentOverThreshold shouldBe 100.0
        }
    }

    @Nested
    inner class ListenerTests {

        @Test
        fun `should notify listeners on slow query`() {
            val notified = AtomicInteger(0)

            logger.onSlowQuery { notified.incrementAndGet() }
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)

            notified.get() shouldBe 1
        }

        @Test
        fun `should not notify listeners on fast query`() {
            val notified = AtomicInteger(0)

            logger.onSlowQuery { notified.incrementAndGet() }
            logger.checkAndLog("SELECT 1", null, 50.milliseconds)

            notified.get() shouldBe 0
        }

        @Test
        fun `should support multiple listeners`() {
            val count1 = AtomicInteger(0)
            val count2 = AtomicInteger(0)

            logger.onSlowQuery { count1.incrementAndGet() }
            logger.onSlowQuery { count2.incrementAndGet() }
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)

            count1.get() shouldBe 1
            count2.get() shouldBe 1
        }

        @Test
        fun `should remove listener`() {
            val count = AtomicInteger(0)
            val listener: (SlowQueryLogger.SlowQueryInfo) -> Unit = { count.incrementAndGet() }

            logger.onSlowQuery(listener)
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)
            count.get() shouldBe 1

            logger.removeListener(listener)
            logger.checkAndLog("SELECT 2", null, 150.milliseconds)
            count.get() shouldBe 1
        }
    }

    @Nested
    inner class HistoryTests {

        @Test
        fun `should limit history size`() {
            repeat(15) { i ->
                logger.checkAndLog("SELECT $i", null, 150.milliseconds)
            }

            logger.getRecentSlowQueries() shouldHaveSize 10
        }

        @Test
        fun `should keep most recent queries`() {
            repeat(15) { i ->
                logger.checkAndLog("SELECT $i", null, 150.milliseconds)
            }

            val queries = logger.getRecentSlowQueries()
            queries[0].sql shouldBe "SELECT 5" // oldest remaining
            queries[9].sql shouldBe "SELECT 14" // newest
        }

        @Test
        fun `should clear history`() {
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)
            logger.getSlowQueryCount() shouldBe 1

            logger.clearHistory()

            logger.getSlowQueryCount() shouldBe 0
        }
    }

    @Nested
    inner class StatisticsTests {

        @Test
        fun `should calculate statistics correctly`() {
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)
            logger.checkAndLog("SELECT 2", null, 200.milliseconds)
            logger.checkAndLog("SELECT 3", null, 250.milliseconds)

            val stats = logger.getStatistics()
            stats.count shouldBe 3
            stats.minDuration shouldBe 150.milliseconds
            stats.maxDuration shouldBe 250.milliseconds
            stats.avgDuration shouldBe 200.milliseconds
            stats.totalDuration shouldBe 600.milliseconds
        }

        @Test
        fun `should return zero statistics when empty`() {
            val stats = logger.getStatistics()

            stats.count shouldBe 0
            stats.avgDuration shouldBe kotlin.time.Duration.ZERO
        }
    }

    @Nested
    inner class StackTraceTests {

        @Test
        fun `should capture stack trace when requested`() {
            logger.checkAndLog(
                sql = "SELECT 1",
                params = null,
                duration = 150.milliseconds,
                captureStackTrace = true
            )

            val info = logger.getRecentSlowQueries()[0]
            info.stackTrace shouldNotBe null
            info.stackTrace!!.isNotEmpty() shouldBe true
        }

        @Test
        fun `should not capture stack trace by default`() {
            logger.checkAndLog("SELECT 1", null, 150.milliseconds)

            val info = logger.getRecentSlowQueries()[0]
            info.stackTrace shouldBe null
        }
    }

    @Nested
    inner class DSLTests {

        @Test
        fun `slowQueryLogger DSL should create valid logger`() {
            val notified = AtomicInteger(0)

            val customLogger = slowQueryLogger {
                threshold = 50.milliseconds
                logLevel = SlowQueryLogger.LogLevel.WARN
                maxRecentQueries = 5
                onSlowQuery { notified.incrementAndGet() }
            }

            customLogger.checkAndLog("SELECT 1", null, 100.milliseconds)

            notified.get() shouldBe 1
        }
    }
}
