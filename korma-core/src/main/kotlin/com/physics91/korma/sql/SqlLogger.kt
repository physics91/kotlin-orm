package com.physics91.korma.sql

import org.slf4j.Logger

/**
 * SQL logging utility.
 *
 * Provides consistent SQL logging across all Korma modules.
 * Single Source of Truth for SQL logging behavior.
 *
 * @param showSql whether to log SQL statements
 * @param formatSql whether to format SQL for readability
 * @param logger the SLF4J logger to use
 */
class SqlLogger(
    private val showSql: Boolean,
    private val formatSql: Boolean,
    private val logger: Logger
) {
    /**
     * Log an SQL statement with parameters.
     */
    fun log(sql: String, params: List<Any?>) {
        if (showSql) {
            val formattedSql = if (formatSql) SqlFormatter.format(sql) else sql
            logger.info("Korma SQL: $formattedSql")
            if (params.isNotEmpty()) {
                logger.info("Korma Params: $params")
            }
        }
    }

    /**
     * Log an SQL statement without parameters.
     */
    fun log(sql: String) {
        log(sql, emptyList())
    }
}
