package com.physics91.korma.r2dbc

import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import java.time.Duration

/**
 * R2DBC connection factory wrapper with connection pooling support.
 */
class R2dbcConnectionFactory private constructor(
    private val connectionPool: ConnectionPool
) : AutoCloseable {

    /**
     * Acquire a connection from the pool.
     */
    suspend fun getConnection(): Connection {
        return connectionPool.create().awaitFirst()
    }

    /**
     * Get pool metrics.
     */
    fun getMetrics(): PoolMetrics? {
        val metrics = connectionPool.metrics
        return metrics.map { m ->
            PoolMetrics(
                acquiredSize = m.acquiredSize(),
                allocatedSize = m.allocatedSize(),
                idleSize = m.idleSize(),
                pendingAcquireSize = m.pendingAcquireSize(),
                maxAllocatedSize = m.getMaxAllocatedSize(),
                maxPendingAcquireSize = m.getMaxPendingAcquireSize()
            )
        }.orElse(null)
    }

    override fun close() {
        connectionPool.dispose()
    }

    /**
     * Pool metrics data class.
     */
    data class PoolMetrics(
        val acquiredSize: Int,
        val allocatedSize: Int,
        val idleSize: Int,
        val pendingAcquireSize: Int,
        val maxAllocatedSize: Int,
        val maxPendingAcquireSize: Int
    )

    companion object {
        /**
         * Create a connection factory from a URL.
         */
        fun create(url: String, config: PoolConfig = PoolConfig()): R2dbcConnectionFactory {
            val connectionFactory = ConnectionFactories.get(url)
            return create(connectionFactory, config)
        }

        /**
         * Create a connection factory with options.
         */
        fun create(options: ConnectionFactoryOptions, config: PoolConfig = PoolConfig()): R2dbcConnectionFactory {
            val connectionFactory = ConnectionFactories.get(options)
            return create(connectionFactory, config)
        }

        /**
         * Create a connection factory from an existing factory.
         */
        fun create(connectionFactory: ConnectionFactory, config: PoolConfig = PoolConfig()): R2dbcConnectionFactory {
            val poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(config.maxIdleTime)
                .maxLifeTime(config.maxLifeTime)
                .maxAcquireTime(config.maxAcquireTime)
                .maxCreateConnectionTime(config.maxCreateConnectionTime)
                .initialSize(config.initialSize)
                .maxSize(config.maxSize)
                .acquireRetry(config.acquireRetry)
                .build()

            val pool = ConnectionPool(poolConfig)
            return R2dbcConnectionFactory(pool)
        }

        /**
         * Create options builder for common databases.
         */
        fun options(): ConnectionOptionsBuilder = ConnectionOptionsBuilder()
    }

    /**
     * Pool configuration.
     */
    data class PoolConfig(
        val initialSize: Int = 10,
        val maxSize: Int = 20,
        val maxIdleTime: Duration = Duration.ofMinutes(30),
        val maxLifeTime: Duration = Duration.ofHours(1),
        val maxAcquireTime: Duration = Duration.ofSeconds(30),
        val maxCreateConnectionTime: Duration = Duration.ofSeconds(30),
        val acquireRetry: Int = 3
    )
}

/**
 * Builder for connection factory options.
 */
class ConnectionOptionsBuilder {
    private var driver: String = "h2"
    private var protocol: String? = null
    private var host: String = "localhost"
    private var port: Int? = null
    private var database: String = ""
    private var user: String? = null
    private var password: String? = null
    private val options = mutableMapOf<String, String>()

    fun driver(driver: String) = apply { this.driver = driver }
    fun protocol(protocol: String) = apply { this.protocol = protocol }
    fun host(host: String) = apply { this.host = host }
    fun port(port: Int) = apply { this.port = port }
    fun database(database: String) = apply { this.database = database }
    fun user(user: String) = apply { this.user = user }
    fun password(password: String) = apply { this.password = password }
    fun option(key: String, value: String) = apply { this.options[key] = value }

    // Convenience methods for common databases
    fun h2() = apply { driver = "h2" }
    fun h2Mem(name: String) = apply {
        driver = "h2"
        protocol = "mem"
        database = name
    }
    fun h2File(path: String) = apply {
        driver = "h2"
        protocol = "file"
        database = path
    }

    fun postgresql() = apply {
        driver = "postgresql"
        port = 5432
    }

    fun mysql() = apply {
        driver = "mysql"
        port = 3306
    }

    fun build(): ConnectionFactoryOptions {
        val builder = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, driver)
            .option(ConnectionFactoryOptions.HOST, host)
            .option(ConnectionFactoryOptions.DATABASE, database)

        protocol?.let { builder.option(ConnectionFactoryOptions.PROTOCOL, it) }
        port?.let { builder.option(ConnectionFactoryOptions.PORT, it) }
        user?.let { builder.option(ConnectionFactoryOptions.USER, it) }
        password?.let { builder.option(ConnectionFactoryOptions.PASSWORD, it) }

        options.forEach { (key, value) ->
            val option: io.r2dbc.spi.Option<String> = io.r2dbc.spi.Option.valueOf(key)
            builder.option(option, value)
        }

        return builder.build()
    }
}
