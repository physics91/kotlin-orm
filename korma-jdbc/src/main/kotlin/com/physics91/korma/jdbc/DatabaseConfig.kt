package com.physics91.korma.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/**
 * Database configuration for JDBC connections.
 *
 * Uses HikariCP for connection pooling.
 *
 * Example:
 * ```kotlin
 * val config = DatabaseConfig(
 *     jdbcUrl = "jdbc:h2:mem:test",
 *     username = "sa",
 *     password = ""
 * )
 * val database = JdbcDatabase(config)
 * ```
 */
data class DatabaseConfig(
    val jdbcUrl: String,
    val username: String = "",
    val password: String = "",
    val driverClassName: String? = null,
    val poolConfig: PoolConfig = PoolConfig()
) {
    /**
     * Create a HikariCP DataSource from this configuration.
     */
    fun createDataSource(): HikariDataSource {
        val hikariConfig = HikariConfig().apply {
            this.jdbcUrl = this@DatabaseConfig.jdbcUrl
            this.username = this@DatabaseConfig.username
            this.password = this@DatabaseConfig.password

            this@DatabaseConfig.driverClassName?.let {
                this.driverClassName = it
            }

            // Pool settings
            this.maximumPoolSize = poolConfig.maximumPoolSize
            this.minimumIdle = poolConfig.minimumIdle
            this.idleTimeout = poolConfig.idleTimeout.inWholeMilliseconds
            this.connectionTimeout = poolConfig.connectionTimeout.inWholeMilliseconds
            this.maxLifetime = poolConfig.maxLifetime.inWholeMilliseconds
            this.keepaliveTime = poolConfig.keepaliveTime.inWholeMilliseconds

            // Pool name
            poolConfig.poolName?.let { this.poolName = it }

            // Validation
            this.connectionTestQuery = poolConfig.connectionTestQuery

            // Auto-commit
            this.isAutoCommit = poolConfig.autoCommit

            // Read-only
            this.isReadOnly = poolConfig.readOnly

            // Transaction isolation
            poolConfig.transactionIsolation?.let {
                this.transactionIsolation = it.name
            }
        }

        return HikariDataSource(hikariConfig)
    }
}

/**
 * HikariCP pool configuration.
 */
data class PoolConfig(
    val maximumPoolSize: Int = 10,
    val minimumIdle: Int = 5,
    val idleTimeout: kotlin.time.Duration = kotlin.time.Duration.parse("10m"),
    val connectionTimeout: kotlin.time.Duration = kotlin.time.Duration.parse("30s"),
    val maxLifetime: kotlin.time.Duration = kotlin.time.Duration.parse("30m"),
    val keepaliveTime: kotlin.time.Duration = kotlin.time.Duration.parse("0s"),
    val poolName: String? = null,
    val connectionTestQuery: String? = null,
    val autoCommit: Boolean = true,
    val readOnly: Boolean = false,
    val transactionIsolation: TransactionIsolation? = null
)

/**
 * Transaction isolation levels.
 */
enum class TransactionIsolation {
    TRANSACTION_NONE,
    TRANSACTION_READ_UNCOMMITTED,
    TRANSACTION_READ_COMMITTED,
    TRANSACTION_REPEATABLE_READ,
    TRANSACTION_SERIALIZABLE;

    fun toJdbcValue(): Int = when (this) {
        TRANSACTION_NONE -> java.sql.Connection.TRANSACTION_NONE
        TRANSACTION_READ_UNCOMMITTED -> java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
        TRANSACTION_READ_COMMITTED -> java.sql.Connection.TRANSACTION_READ_COMMITTED
        TRANSACTION_REPEATABLE_READ -> java.sql.Connection.TRANSACTION_REPEATABLE_READ
        TRANSACTION_SERIALIZABLE -> java.sql.Connection.TRANSACTION_SERIALIZABLE
    }
}

/**
 * DSL builder for DatabaseConfig.
 */
class DatabaseConfigBuilder {
    var jdbcUrl: String = ""
    var username: String = ""
    var password: String = ""
    var driverClassName: String? = null

    private var poolConfig: PoolConfig = PoolConfig()

    /**
     * Configure connection pool settings.
     */
    fun pool(block: PoolConfigBuilder.() -> Unit) {
        poolConfig = PoolConfigBuilder().apply(block).build()
    }

    fun build(): DatabaseConfig = DatabaseConfig(
        jdbcUrl = jdbcUrl,
        username = username,
        password = password,
        driverClassName = driverClassName,
        poolConfig = poolConfig
    )
}

/**
 * DSL builder for PoolConfig.
 */
class PoolConfigBuilder {
    var maximumPoolSize: Int = 10
    var minimumIdle: Int = 5
    var idleTimeout: kotlin.time.Duration = kotlin.time.Duration.parse("10m")
    var connectionTimeout: kotlin.time.Duration = kotlin.time.Duration.parse("30s")
    var maxLifetime: kotlin.time.Duration = kotlin.time.Duration.parse("30m")
    var keepaliveTime: kotlin.time.Duration = kotlin.time.Duration.parse("0s")
    var poolName: String? = null
    var connectionTestQuery: String? = null
    var autoCommit: Boolean = true
    var readOnly: Boolean = false
    var transactionIsolation: TransactionIsolation? = null

    fun build(): PoolConfig = PoolConfig(
        maximumPoolSize = maximumPoolSize,
        minimumIdle = minimumIdle,
        idleTimeout = idleTimeout,
        connectionTimeout = connectionTimeout,
        maxLifetime = maxLifetime,
        keepaliveTime = keepaliveTime,
        poolName = poolName,
        connectionTestQuery = connectionTestQuery,
        autoCommit = autoCommit,
        readOnly = readOnly,
        transactionIsolation = transactionIsolation
    )
}

/**
 * Create a database configuration using DSL.
 */
fun databaseConfig(block: DatabaseConfigBuilder.() -> Unit): DatabaseConfig =
    DatabaseConfigBuilder().apply(block).build()
