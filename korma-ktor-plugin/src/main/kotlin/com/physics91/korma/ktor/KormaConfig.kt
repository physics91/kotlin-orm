package com.physics91.korma.ktor

import com.physics91.korma.dialect.h2.H2Dialect
import com.physics91.korma.jdbc.DatabaseConfig
import com.physics91.korma.jdbc.PoolConfig
import com.physics91.korma.sql.SqlDialect
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Configuration for Korma Ktor plugin.
 *
 * Example usage in Ktor:
 * ```kotlin
 * install(Korma) {
 *     // Option 1: Using JDBC URL
 *     jdbcUrl = "jdbc:h2:mem:test"
 *     username = "sa"
 *     password = ""
 *     dialect = H2Dialect
 *
 *     // Option 2: Using DataSource
 *     dataSource = myDataSource
 *     dialect = PostgreSqlDialect
 *
 *     // Optional settings
 *     showSql = true
 *     formatSql = true
 *     maxPoolSize = 10
 * }
 * ```
 */
class KormaConfig {

    /**
     * JDBC connection URL.
     */
    var jdbcUrl: String? = null

    /**
     * Database driver class name (optional, auto-detected from URL).
     */
    var driverClassName: String? = null

    /**
     * Database username.
     */
    var username: String? = null

    /**
     * Database password.
     */
    var password: String? = null

    /**
     * Pre-configured DataSource (alternative to JDBC URL configuration).
     */
    var dataSource: DataSource? = null

    /**
     * SQL dialect for the database.
     * If not specified, will attempt auto-detection.
     */
    var dialect: SqlDialect? = null

    /**
     * Whether to log SQL statements.
     */
    var showSql: Boolean = false

    /**
     * Whether to format logged SQL statements.
     */
    var formatSql: Boolean = false

    /**
     * Maximum connection pool size.
     * Default is 20, which is suitable for moderate production workloads.
     * Tune based on your database capacity and expected concurrent requests.
     */
    var maxPoolSize: Int = 20

    /**
     * Minimum idle connections in pool.
     * Default is 5 (25% of maxPoolSize).
     * Higher values reduce connection acquisition latency under load.
     */
    var minIdle: Int = 5

    /**
     * Connection timeout.
     */
    var connectionTimeout: Duration = 30.seconds

    /**
     * Idle timeout.
     */
    var idleTimeout: Duration = 10.minutes

    /**
     * Maximum connection lifetime.
     */
    var maxLifetime: Duration = 30.minutes

    /**
     * Connection pool name.
     */
    var poolName: String = "KormaPool"

    /**
     * Whether to auto-create tables on startup.
     */
    var autoCreateTables: Boolean = false

    /**
     * Build DatabaseConfig from this configuration.
     */
    internal fun buildDatabaseConfig(): DatabaseConfig? {
        val url = jdbcUrl ?: return null

        val poolConfig = PoolConfig(
            maximumPoolSize = maxPoolSize,
            minimumIdle = minIdle,
            connectionTimeout = connectionTimeout,
            idleTimeout = idleTimeout,
            maxLifetime = maxLifetime,
            poolName = poolName
        )

        return DatabaseConfig(
            jdbcUrl = url,
            username = username ?: "",
            password = password ?: "",
            driverClassName = driverClassName ?: detectDriver(url),
            poolConfig = poolConfig
        )
    }

    /**
     * Resolve the SQL dialect.
     */
    internal fun resolveDialect(): SqlDialect {
        // Use explicitly configured dialect
        dialect?.let { return it }

        // Try to detect from JDBC URL
        val url = jdbcUrl?.lowercase()
        if (url != null) {
            return when {
                url.contains("h2") -> H2Dialect
                url.contains("postgresql") || url.contains("postgres") -> loadDialect("com.physics91.korma.dialect.postgresql.PostgreSqlDialect")
                url.contains("mysql") || url.contains("mariadb") -> loadDialect("com.physics91.korma.dialect.mysql.MySqlDialect")
                url.contains("sqlite") -> loadDialect("com.physics91.korma.dialect.sqlite.SqliteDialect")
                else -> H2Dialect
            }
        }

        // Default to H2
        return H2Dialect
    }

    private fun detectDriver(url: String): String {
        return when {
            url.contains("h2") -> "org.h2.Driver"
            url.contains("postgresql") || url.contains("postgres") -> "org.postgresql.Driver"
            url.contains("mysql") -> "com.mysql.cj.jdbc.Driver"
            url.contains("mariadb") -> "org.mariadb.jdbc.Driver"
            url.contains("sqlite") -> "org.sqlite.JDBC"
            else -> throw IllegalArgumentException("Cannot detect driver for URL: $url")
        }
    }

    private fun loadDialect(className: String): SqlDialect {
        return try {
            val clazz = Class.forName(className)
            val field = clazz.getDeclaredField("INSTANCE")
            field.get(null) as SqlDialect
        } catch (e: ClassNotFoundException) {
            throw IllegalStateException(
                "Dialect class not found: $className. " +
                "Make sure the corresponding dialect module is on the classpath."
            )
        } catch (e: Exception) {
            throw IllegalStateException("Failed to load dialect: $className", e)
        }
    }
}
