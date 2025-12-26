package com.physics91.korma.spring

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Configuration properties for Korma ORM.
 *
 * These properties can be configured in application.yml or application.properties:
 *
 * ```yaml
 * korma:
 *   show-sql: true
 *   format-sql: true
 *   dialect: auto
 *   batch-size: 100
 *   pool:
 *     minimum-idle: 5
 *     maximum-pool-size: 20
 *     idle-timeout: 600000
 *     max-lifetime: 1800000
 *     connection-timeout: 30000
 * ```
 */
@ConfigurationProperties(prefix = "korma")
data class KormaProperties(
    /**
     * Whether to show SQL statements in logs.
     */
    var showSql: Boolean = false,

    /**
     * Whether to format SQL statements for readability.
     */
    var formatSql: Boolean = false,

    /**
     * SQL dialect to use. Options: auto, postgresql, mysql, sqlite, h2
     * When set to 'auto', the dialect is detected from the JDBC URL.
     */
    var dialect: String = "auto",

    /**
     * Default batch size for bulk operations.
     */
    var batchSize: Int = 100,

    /**
     * Whether to enable statement caching.
     */
    var statementCache: Boolean = true,

    /**
     * Maximum number of cached prepared statements.
     */
    var statementCacheSize: Int = 250,

    /**
     * Connection pool configuration.
     */
    var pool: PoolProperties = PoolProperties()
) {
    /**
     * Connection pool properties.
     * These override Spring's default HikariCP settings when specified.
     */
    data class PoolProperties(
        /**
         * Minimum number of idle connections.
         */
        var minimumIdle: Int? = null,

        /**
         * Maximum pool size.
         */
        var maximumPoolSize: Int? = null,

        /**
         * Maximum idle time in milliseconds.
         */
        var idleTimeout: Long? = null,

        /**
         * Maximum lifetime of a connection in milliseconds.
         */
        var maxLifetime: Long? = null,

        /**
         * Connection timeout in milliseconds.
         */
        var connectionTimeout: Long? = null,

        /**
         * Pool name for identification.
         */
        var poolName: String? = null
    )
}
