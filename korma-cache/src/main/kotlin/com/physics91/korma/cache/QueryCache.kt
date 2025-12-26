package com.physics91.korma.cache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.security.MessageDigest
import kotlin.time.Duration

/**
 * Cache for query results.
 * Automatically invalidates when related tables are modified.
 *
 * Thread-safety: This implementation is thread-safe and coroutine-safe.
 */
class QueryCache(
    private val delegate: Cache<QueryKey, QueryResult>,
    private val defaultTtl: Duration = Duration.INFINITE
) {
    private val logger = LoggerFactory.getLogger(QueryCache::class.java)

    /** Mapping of table names to query keys that depend on them */
    private val tableDependencies = mutableMapOf<String, MutableSet<QueryKey>>()
    private val dependencyMutex = Mutex()

    /**
     * Get cached query result.
     */
    suspend fun get(key: QueryKey): QueryResult? {
        return delegate.get(key)
    }

    /**
     * Get cached result or execute query and cache the result.
     */
    suspend fun getOrPut(
        sql: String,
        params: List<Any?>,
        tables: Set<String>,
        loader: suspend () -> List<Any>
    ): List<Any> {
        val key = QueryKey.of(sql, params)

        val cached = delegate.get(key)
        if (cached != null) {
            logger.trace("Query cache hit: {}", sql.take(50))
            return cached.rows
        }

        logger.trace("Query cache miss: {}", sql.take(50))
        val result = loader()

        // Cache the result
        put(key, QueryResult(result, tables), defaultTtl)

        return result
    }

    /**
     * Put a query result in the cache.
     */
    suspend fun put(key: QueryKey, result: QueryResult, ttl: Duration = defaultTtl) {
        delegate.put(key, result, ttl)

        // Register table dependencies
        dependencyMutex.withLock {
            result.tables.forEach { table ->
                tableDependencies.getOrPut(table) { mutableSetOf() }.add(key)
            }
        }
    }

    /**
     * Invalidate all queries that depend on the given table.
     * Call this when the table is modified (INSERT, UPDATE, DELETE).
     */
    suspend fun invalidateTable(tableName: String) {
        val keysToInvalidate = dependencyMutex.withLock {
            tableDependencies.remove(tableName) ?: emptySet()
        }

        if (keysToInvalidate.isNotEmpty()) {
            logger.debug("Invalidating {} queries for table: {}", keysToInvalidate.size, tableName)
            delegate.removeAll(keysToInvalidate)
        }
    }

    /**
     * Invalidate all queries that depend on any of the given tables.
     */
    suspend fun invalidateTables(tableNames: Set<String>) {
        tableNames.forEach { invalidateTable(it) }
    }

    /**
     * Clear all cached queries.
     */
    suspend fun clear() {
        delegate.clear()
        dependencyMutex.withLock {
            tableDependencies.clear()
        }
    }

    /**
     * Clean stale dependencies - removes references to keys that are no longer in the cache.
     * Call this periodically to prevent memory leaks.
     */
    suspend fun cleanStaleDependencies() {
        dependencyMutex.withLock {
            val tablesToRemove = mutableListOf<String>()

            for ((tableName, keys) in tableDependencies) {
                // Collect keys to remove
                val keysToRemove = mutableSetOf<QueryKey>()
                for (key in keys) {
                    if (!delegate.containsKey(key)) {
                        keysToRemove.add(key)
                    }
                }
                keys.removeAll(keysToRemove)

                // Mark empty tables for removal
                if (keys.isEmpty()) {
                    tablesToRemove.add(tableName)
                }
            }

            // Remove empty tables
            tablesToRemove.forEach { tableDependencies.remove(it) }
        }
    }

    /**
     * Get the number of tracked table dependencies.
     */
    suspend fun dependencyCount(): Int {
        return dependencyMutex.withLock {
            tableDependencies.values.sumOf { it.size }
        }
    }

    /**
     * Get cache statistics.
     */
    fun stats(): CacheStats? {
        return (delegate as? StatsCache<*, *>)?.stats()
    }
}

/**
 * Key for query cache entries.
 */
data class QueryKey(
    /** Hash of the SQL query */
    val sqlHash: String,
    /** Hash of the parameters */
    val paramsHash: String
) {
    companion object {
        /**
         * Create a query key from SQL and parameters.
         * Uses new MessageDigest instance for each call to ensure coroutine safety.
         */
        fun of(sql: String, params: List<Any?>): QueryKey {
            val md = MessageDigest.getInstance("SHA-256")

            val sqlHash = md.digest(sql.toByteArray()).toHexString()
            md.reset()

            val paramsString = params.joinToString(",") { it?.toString() ?: "null" }
            val paramsHash = md.digest(paramsString.toByteArray()).toHexString()

            return QueryKey(sqlHash, paramsHash)
        }

        private fun ByteArray.toHexString(): String {
            return joinToString("") { "%02x".format(it) }
        }
    }
}

/**
 * Cached query result.
 */
data class QueryResult(
    /** The query result rows */
    val rows: List<Any>,
    /** Tables that this query depends on */
    val tables: Set<String>,
    /** When the result was cached */
    val cachedAt: Long = System.currentTimeMillis()
)

/**
 * Query cache configuration.
 */
data class QueryCacheConfig(
    /** Maximum number of queries to cache */
    val maxSize: Long = 1000,
    /** Default TTL for cached queries */
    val defaultTtl: Duration = Duration.parse("5m"),
    /** Whether to enable query caching */
    val enabled: Boolean = true,
    /** Tables to exclude from caching */
    val excludedTables: Set<String> = emptySet()
)
