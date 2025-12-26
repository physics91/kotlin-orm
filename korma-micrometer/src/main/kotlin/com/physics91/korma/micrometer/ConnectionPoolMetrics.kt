package com.physics91.korma.micrometer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.MeterBinder
import java.util.function.Supplier

/**
 * Metrics collector for database connection pool statistics.
 *
 * Provides visibility into:
 * - Active connections
 * - Idle connections
 * - Total connections
 * - Pending connection requests
 * - Connection acquisition time
 *
 * Usage:
 * ```kotlin
 * val poolMetrics = ConnectionPoolMetrics(
 *     registry = meterRegistry,
 *     poolName = "main",
 *     statsProvider = HikariPoolStatsProvider(hikariDataSource)
 * )
 * ```
 */
class ConnectionPoolMetrics(
    private val registry: MeterRegistry,
    private val poolName: String = "default",
    private val statsProvider: PoolStatsProvider,
    private val prefix: String = "korma.pool"
) : MeterBinder {

    override fun bindTo(registry: MeterRegistry) {
        val tags = listOf(Tag.of("pool", poolName))

        Gauge.builder("$prefix.active", statsProvider::activeConnections)
            .description("Number of active connections")
            .tags(tags)
            .register(registry)

        Gauge.builder("$prefix.idle", statsProvider::idleConnections)
            .description("Number of idle connections")
            .tags(tags)
            .register(registry)

        Gauge.builder("$prefix.total", statsProvider::totalConnections)
            .description("Total number of connections in the pool")
            .tags(tags)
            .register(registry)

        Gauge.builder("$prefix.pending", statsProvider::pendingRequests)
            .description("Number of threads waiting for a connection")
            .tags(tags)
            .register(registry)

        Gauge.builder("$prefix.max", statsProvider::maxConnections)
            .description("Maximum number of connections in the pool")
            .tags(tags)
            .register(registry)

        Gauge.builder("$prefix.min", statsProvider::minConnections)
            .description("Minimum number of idle connections in the pool")
            .tags(tags)
            .register(registry)

        // Utilization percentage
        Gauge.builder("$prefix.utilization") {
            val max = statsProvider.maxConnections()
            val active = statsProvider.activeConnections()
            if (max > 0) (active.toDouble() / max) * 100 else 0.0
        }
            .description("Pool utilization percentage")
            .tags(tags)
            .baseUnit("percent")
            .register(registry)
    }

    init {
        bindTo(registry)
    }
}

/**
 * Interface for providing connection pool statistics.
 */
interface PoolStatsProvider {
    fun activeConnections(): Int
    fun idleConnections(): Int
    fun totalConnections(): Int
    fun pendingRequests(): Int
    fun maxConnections(): Int
    fun minConnections(): Int
}

/**
 * Stats provider implementation for HikariCP.
 *
 * Usage:
 * ```kotlin
 * val hikariDataSource = HikariDataSource(config)
 * val statsProvider = HikariPoolStatsProvider(hikariDataSource)
 * ```
 */
class HikariPoolStatsProvider(
    private val activeConnectionsSupplier: Supplier<Int>,
    private val idleConnectionsSupplier: Supplier<Int>,
    private val totalConnectionsSupplier: Supplier<Int>,
    private val pendingRequestsSupplier: Supplier<Int>,
    private val maxConnectionsSupplier: Supplier<Int>,
    private val minConnectionsSupplier: Supplier<Int>
) : PoolStatsProvider {

    override fun activeConnections(): Int = activeConnectionsSupplier.get()
    override fun idleConnections(): Int = idleConnectionsSupplier.get()
    override fun totalConnections(): Int = totalConnectionsSupplier.get()
    override fun pendingRequests(): Int = pendingRequestsSupplier.get()
    override fun maxConnections(): Int = maxConnectionsSupplier.get()
    override fun minConnections(): Int = minConnectionsSupplier.get()

    companion object {
        /**
         * Creates a HikariPoolStatsProvider using reflection to avoid
         * compile-time dependency on HikariCP.
         */
        fun fromHikariDataSource(dataSource: Any): HikariPoolStatsProvider {
            val dsClass = dataSource::class.java

            val getPoolMethod = try {
                dsClass.getMethod("getHikariPoolMXBean")
            } catch (e: NoSuchMethodException) {
                throw IllegalArgumentException("DataSource must be a HikariDataSource")
            }

            val getConfigMethod = dsClass.getMethod("getHikariConfigMXBean")

            return HikariPoolStatsProvider(
                activeConnectionsSupplier = {
                    val pool = getPoolMethod.invoke(dataSource)
                    if (pool != null) {
                        pool::class.java.getMethod("getActiveConnections").invoke(pool) as Int
                    } else 0
                },
                idleConnectionsSupplier = {
                    val pool = getPoolMethod.invoke(dataSource)
                    if (pool != null) {
                        pool::class.java.getMethod("getIdleConnections").invoke(pool) as Int
                    } else 0
                },
                totalConnectionsSupplier = {
                    val pool = getPoolMethod.invoke(dataSource)
                    if (pool != null) {
                        pool::class.java.getMethod("getTotalConnections").invoke(pool) as Int
                    } else 0
                },
                pendingRequestsSupplier = {
                    val pool = getPoolMethod.invoke(dataSource)
                    if (pool != null) {
                        pool::class.java.getMethod("getThreadsAwaitingConnection").invoke(pool) as Int
                    } else 0
                },
                maxConnectionsSupplier = {
                    val config = getConfigMethod.invoke(dataSource)
                    config::class.java.getMethod("getMaximumPoolSize").invoke(config) as Int
                },
                minConnectionsSupplier = {
                    val config = getConfigMethod.invoke(dataSource)
                    config::class.java.getMethod("getMinimumIdle").invoke(config) as Int
                }
            )
        }
    }
}

/**
 * Simple in-memory stats provider for testing.
 */
class SimplePoolStatsProvider(
    var active: Int = 0,
    var idle: Int = 5,
    var total: Int = 5,
    var pending: Int = 0,
    var max: Int = 10,
    var min: Int = 1
) : PoolStatsProvider {
    override fun activeConnections(): Int = active
    override fun idleConnections(): Int = idle
    override fun totalConnections(): Int = total
    override fun pendingRequests(): Int = pending
    override fun maxConnections(): Int = max
    override fun minConnections(): Int = min
}
