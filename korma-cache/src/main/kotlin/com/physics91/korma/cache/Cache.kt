package com.physics91.korma.cache

import kotlin.time.Duration

/**
 * Core cache interface for Korma ORM.
 * Provides a unified API for different cache implementations.
 *
 * @param K Key type
 * @param V Value type
 */
interface Cache<K : Any, V : Any> {
    /** Cache name for identification */
    val name: String

    /**
     * Get a value by key.
     * @return The cached value or null if not found
     */
    suspend fun get(key: K): V?

    /**
     * Get a value by key, or compute and cache it if not found.
     * @param key The cache key
     * @param loader Function to compute the value if not cached
     * @return The cached or computed value
     */
    suspend fun getOrPut(key: K, loader: suspend () -> V): V

    /**
     * Put a value in the cache.
     * @param key The cache key
     * @param value The value to cache
     */
    suspend fun put(key: K, value: V)

    /**
     * Put a value in the cache with a specific TTL.
     * @param key The cache key
     * @param value The value to cache
     * @param ttl Time to live
     */
    suspend fun put(key: K, value: V, ttl: Duration)

    /**
     * Check if a key exists in the cache.
     */
    suspend fun containsKey(key: K): Boolean

    /**
     * Remove a value from the cache.
     * @return The removed value or null if not found
     */
    suspend fun remove(key: K): V?

    /**
     * Remove all entries from the cache.
     */
    suspend fun clear()

    /**
     * Get the number of entries in the cache.
     */
    suspend fun size(): Long

    /**
     * Get all keys in the cache.
     */
    suspend fun keys(): Set<K>

    /**
     * Get multiple values by keys.
     * @return Map of key to value for found entries
     */
    suspend fun getAll(keys: Set<K>): Map<K, V>

    /**
     * Put multiple values in the cache.
     */
    suspend fun putAll(entries: Map<K, V>)

    /**
     * Remove multiple values from the cache.
     */
    suspend fun removeAll(keys: Set<K>)
}

/**
 * Cache statistics for monitoring.
 */
data class CacheStats(
    val hits: Long,
    val misses: Long,
    val evictions: Long,
    val size: Long,
    val averageLoadTime: Double = 0.0
) {
    val hitRate: Double
        get() = if (hits + misses > 0) hits.toDouble() / (hits + misses) else 0.0

    val missRate: Double
        get() = if (hits + misses > 0) misses.toDouble() / (hits + misses) else 0.0
}

/**
 * Cache with statistics support.
 */
interface StatsCache<K : Any, V : Any> : Cache<K, V> {
    /**
     * Get cache statistics.
     */
    fun stats(): CacheStats

    /**
     * Reset statistics.
     */
    fun resetStats()
}

/**
 * Cache configuration.
 */
data class CacheConfig(
    /** Maximum number of entries */
    val maxSize: Long = 10_000,

    /** Default time to live */
    val defaultTtl: Duration = Duration.INFINITE,

    /** Time to idle (evict after no access) */
    val timeToIdle: Duration? = null,

    /** Enable statistics recording */
    val recordStats: Boolean = true,

    /** Weak keys (allow GC to collect keys) */
    val weakKeys: Boolean = false,

    /** Weak values (allow GC to collect values) */
    val weakValues: Boolean = false,

    /** Soft values (allow GC to collect values under memory pressure) */
    val softValues: Boolean = false
)

/**
 * Cache eviction listener.
 */
fun interface CacheEvictionListener<K : Any, V : Any> {
    /**
     * Called when an entry is evicted.
     * @param key The evicted key
     * @param value The evicted value
     * @param cause The cause of eviction
     */
    fun onEviction(key: K, value: V?, cause: EvictionCause)
}

/**
 * Cause of cache eviction.
 */
enum class EvictionCause {
    /** Evicted due to size limit */
    SIZE,
    /** Evicted due to expiration */
    EXPIRED,
    /** Explicitly removed */
    EXPLICIT,
    /** Replaced with new value */
    REPLACED,
    /** Collected by GC (weak/soft reference) */
    COLLECTED
}
