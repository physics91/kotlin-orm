package com.physics91.korma.sql

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.Marker

/**
 * Tests for SqlLogger using a capturing logger.
 */
class SqlLoggerTest {

    private lateinit var capturingLogger: CapturingLogger
    private val loggedMessages: MutableList<String> get() = capturingLogger.messages

    @BeforeEach
    fun setup() {
        capturingLogger = CapturingLogger()
    }

    @Test
    fun `logs SQL when showSql is true`() {
        val sqlLogger = SqlLogger(showSql = true, formatSql = false, logger = capturingLogger)

        sqlLogger.log("SELECT * FROM users")

        assert(loggedMessages.contains("Korma SQL: SELECT * FROM users"))
    }

    @Test
    fun `does not log when showSql is false`() {
        val sqlLogger = SqlLogger(showSql = false, formatSql = false, logger = capturingLogger)

        sqlLogger.log("SELECT * FROM users")

        assert(loggedMessages.isEmpty())
    }

    @Test
    fun `logs formatted SQL when formatSql is true`() {
        val sqlLogger = SqlLogger(showSql = true, formatSql = true, logger = capturingLogger)

        sqlLogger.log("SELECT * FROM users WHERE id = 1")

        assert(loggedMessages.any { it.contains("FROM users") && it.contains("WHERE") })
    }

    @Test
    fun `logs params when provided`() {
        val sqlLogger = SqlLogger(showSql = true, formatSql = false, logger = capturingLogger)

        sqlLogger.log("SELECT * FROM users WHERE id = ?", listOf(1L))

        assert(loggedMessages.contains("Korma SQL: SELECT * FROM users WHERE id = ?"))
        assert(loggedMessages.contains("Korma Params: [1]"))
    }

    @Test
    fun `does not log params when list is empty`() {
        val sqlLogger = SqlLogger(showSql = true, formatSql = false, logger = capturingLogger)

        sqlLogger.log("SELECT * FROM users", emptyList())

        assert(loggedMessages.size == 1)
        assert(loggedMessages[0] == "Korma SQL: SELECT * FROM users")
    }

    @Test
    fun `logs multiple params correctly`() {
        val sqlLogger = SqlLogger(showSql = true, formatSql = false, logger = capturingLogger)

        sqlLogger.log("SELECT * FROM users WHERE id = ? AND name = ?", listOf(1L, "John"))

        assert(loggedMessages.contains("Korma Params: [1, John]"))
    }

    @Test
    fun `log without params calls log with empty list`() {
        val sqlLogger = SqlLogger(showSql = true, formatSql = false, logger = capturingLogger)

        sqlLogger.log("SELECT 1")

        assert(loggedMessages.size == 1)
    }

    /**
     * Simple logger that captures log messages for testing.
     */
    private class CapturingLogger : Logger {
        val messages = mutableListOf<String>()

        override fun getName(): String = "CapturingLogger"
        override fun isTraceEnabled(): Boolean = true
        override fun isTraceEnabled(marker: Marker?): Boolean = true
        override fun trace(msg: String?) { msg?.let { messages.add(it) } }
        override fun trace(format: String?, arg: Any?) {}
        override fun trace(format: String?, arg1: Any?, arg2: Any?) {}
        override fun trace(format: String?, vararg arguments: Any?) {}
        override fun trace(msg: String?, t: Throwable?) {}
        override fun trace(marker: Marker?, msg: String?) {}
        override fun trace(marker: Marker?, format: String?, arg: Any?) {}
        override fun trace(marker: Marker?, format: String?, arg1: Any?, arg2: Any?) {}
        override fun trace(marker: Marker?, format: String?, vararg argArray: Any?) {}
        override fun trace(marker: Marker?, msg: String?, t: Throwable?) {}

        override fun isDebugEnabled(): Boolean = true
        override fun isDebugEnabled(marker: Marker?): Boolean = true
        override fun debug(msg: String?) { msg?.let { messages.add(it) } }
        override fun debug(format: String?, arg: Any?) {}
        override fun debug(format: String?, arg1: Any?, arg2: Any?) {}
        override fun debug(format: String?, vararg arguments: Any?) {}
        override fun debug(msg: String?, t: Throwable?) {}
        override fun debug(marker: Marker?, msg: String?) {}
        override fun debug(marker: Marker?, format: String?, arg: Any?) {}
        override fun debug(marker: Marker?, format: String?, arg1: Any?, arg2: Any?) {}
        override fun debug(marker: Marker?, format: String?, vararg argArray: Any?) {}
        override fun debug(marker: Marker?, msg: String?, t: Throwable?) {}

        override fun isInfoEnabled(): Boolean = true
        override fun isInfoEnabled(marker: Marker?): Boolean = true
        override fun info(msg: String?) { msg?.let { messages.add(it) } }
        override fun info(format: String?, arg: Any?) {}
        override fun info(format: String?, arg1: Any?, arg2: Any?) {}
        override fun info(format: String?, vararg arguments: Any?) {}
        override fun info(msg: String?, t: Throwable?) {}
        override fun info(marker: Marker?, msg: String?) {}
        override fun info(marker: Marker?, format: String?, arg: Any?) {}
        override fun info(marker: Marker?, format: String?, arg1: Any?, arg2: Any?) {}
        override fun info(marker: Marker?, format: String?, vararg argArray: Any?) {}
        override fun info(marker: Marker?, msg: String?, t: Throwable?) {}

        override fun isWarnEnabled(): Boolean = true
        override fun isWarnEnabled(marker: Marker?): Boolean = true
        override fun warn(msg: String?) { msg?.let { messages.add(it) } }
        override fun warn(format: String?, arg: Any?) {}
        override fun warn(format: String?, arg1: Any?, arg2: Any?) {}
        override fun warn(format: String?, vararg arguments: Any?) {}
        override fun warn(msg: String?, t: Throwable?) {}
        override fun warn(marker: Marker?, msg: String?) {}
        override fun warn(marker: Marker?, format: String?, arg: Any?) {}
        override fun warn(marker: Marker?, format: String?, arg1: Any?, arg2: Any?) {}
        override fun warn(marker: Marker?, format: String?, vararg argArray: Any?) {}
        override fun warn(marker: Marker?, msg: String?, t: Throwable?) {}

        override fun isErrorEnabled(): Boolean = true
        override fun isErrorEnabled(marker: Marker?): Boolean = true
        override fun error(msg: String?) { msg?.let { messages.add(it) } }
        override fun error(format: String?, arg: Any?) {}
        override fun error(format: String?, arg1: Any?, arg2: Any?) {}
        override fun error(format: String?, vararg arguments: Any?) {}
        override fun error(msg: String?, t: Throwable?) {}
        override fun error(marker: Marker?, msg: String?) {}
        override fun error(marker: Marker?, format: String?, arg: Any?) {}
        override fun error(marker: Marker?, format: String?, arg1: Any?, arg2: Any?) {}
        override fun error(marker: Marker?, format: String?, vararg argArray: Any?) {}
        override fun error(marker: Marker?, msg: String?, t: Throwable?) {}
    }
}
