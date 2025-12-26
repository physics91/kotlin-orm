package com.physics91.korma.test.containers

import com.physics91.korma.sql.SqlDialect
import com.physics91.korma.jdbc.DatabaseConfig
import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.jdbc.PoolConfig
import org.testcontainers.containers.JdbcDatabaseContainer
import kotlin.time.Duration.Companion.seconds

/**
 * Configuration holder for Korma test containers.
 * Provides dialect and R2DBC configuration for a specific database type.
 */
data class KormaContainerConfig(
    val dialect: SqlDialect,
    val r2dbcDriver: String,
    val r2dbcPort: Int
)

/**
 * Default pool configuration for test containers.
 * Optimized for test scenarios with smaller pool sizes.
 */
val DEFAULT_TEST_POOL_CONFIG = PoolConfig(
    maximumPoolSize = 5,
    minimumIdle = 1,
    connectionTimeout = 10.seconds,
    idleTimeout = 300.seconds
)

/**
 * Creates a JDBC DatabaseConfig from a JdbcDatabaseContainer (DRY: single implementation).
 */
fun <C : JdbcDatabaseContainer<C>> JdbcDatabaseContainer<C>.toKormaDatabaseConfig(): DatabaseConfig =
    DatabaseConfig(
        jdbcUrl = jdbcUrl,
        username = username,
        password = password,
        driverClassName = driverClassName,
        poolConfig = DEFAULT_TEST_POOL_CONFIG
    )

/**
 * Creates a JdbcDatabase instance from a JdbcDatabaseContainer.
 */
fun <C : JdbcDatabaseContainer<C>> JdbcDatabaseContainer<C>.toKormaJdbcDatabase(dialect: SqlDialect): JdbcDatabase =
    JdbcDatabase(
        config = toKormaDatabaseConfig(),
        dialect = dialect
    )

/**
 * Returns the R2DBC connection URL for a container.
 */
fun <C : JdbcDatabaseContainer<C>> JdbcDatabaseContainer<C>.getKormaR2dbcUrl(
    driver: String,
    port: Int
): String = "r2dbc:${driver}://${host}:${getMappedPort(port)}/${databaseName}"

/**
 * Returns R2DBC connection options map for a container.
 */
fun <C : JdbcDatabaseContainer<C>> JdbcDatabaseContainer<C>.getKormaR2dbcOptions(
    driver: String,
    port: Int
): Map<String, String> = mapOf(
    "driver" to driver,
    "host" to host,
    "port" to getMappedPort(port).toString(),
    "database" to databaseName,
    "user" to username,
    "password" to password
)
