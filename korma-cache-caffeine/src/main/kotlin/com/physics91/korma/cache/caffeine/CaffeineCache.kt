package com.physics91.korma.cache.caffeine

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Cache as CaffeineNativeCache
import com.github.benmanes.caffeine.cache.stats.CacheStats as CaffeineStats
import com.physics91.korma.cache.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

/**
 * High-performance cache implementation using Caffeine.
 * Recommended for production L1 (in-memory) caching.
 *
 * Features:
 * - Automatic eviction based on size and time
 * - Statistics recording
 * - Async loading support
 * - Thread-safe operations
 */
class CaffeineCache<K : Any, V : Any>(
    override val name: String,
    private val config: CaffeineCacheConfig = CaffeineCacheConfig()
) : StatsCache<K, V> {

    private val logger = LoggerFactory.getLogger(CaffeineCache::class.java)

    private val cache: CaffeineNativeCache<K, CacheEntry<V>> = buildCache()

    private fun buildCache(): CaffeineNativeCache<K, CacheEntry<V>> {
        return Caffeine.newBuilder()
            .maximumSize(config.maxSize)
            .apply {
                if (config.expireAfterWrite != Duration.INFINITE) {
                    expireAfterWrite(config.expireAfterWrite.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                }
                if (config.expireAfterAccess != Duration.INFINITE) {
                    expireAfterAccess(config.expireAfterAccess.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                }
                if (config.recordStats) {
                    recordStats()
                }
                if (config.weakKeys) {
                    weakKeys()
                }
                if (config.weakValues) {
                    weakValues()
                }
                if (config.softValues) {
                    softValues()
                }
            }
            .removalListener<K, CacheEntry<V>> { key, _, cause ->
                logger.trace("Cache entry removed: key={}, cause={}", key, cause)
            }
            .build()
    }

    override suspend fun get(key: K): V? {
        return withContext(Dispatchers.IO) {
            val entry = cache.getIfPresent(key)
            if (entry != null && !entry.isExpired()) {
                entry.value
            } else {
                if (entry != null) {
                    cache.invalidate(key)
                }
                null
            }
        }
    }

    override suspend fun getOrPut(key: K, loader: suspend () -> V): V {
        get(key)?.let { return it }

        val value = loader()
        put(key, value)
        return value
    }

    override suspend fun put(key: K, value: V) {
        put(key, value, config.defaultTtl)
    }

    override suspend fun put(key: K, value: V, ttl: Duration) {
        withContext(Dispatchers.IO) {
            val expiresAt = if (ttl == Duration.INFINITE) {
                Long.MAX_VALUE
            } else {
                System.currentTimeMillis() + ttl.inWholeMilliseconds
            }
            cache.put(key, CacheEntry(value, expiresAt))
        }
    }

    override suspend fun containsKey(key: K): Boolean {
        return withContext(Dispatchers.IO) {
            val entry = cache.getIfPresent(key)
            if (entry != null && entry.isExpired()) {
                cache.invalidate(key)
                false
            } else {
                entry != null
            }
        }
    }

    override suspend fun remove(key: K): V? {
        return withContext(Dispatchers.IO) {
            val entry = cache.getIfPresent(key)
            cache.invalidate(key)
            entry?.value
        }
    }

    override suspend fun clear() {
        withContext(Dispatchers.IO) {
            cache.invalidateAll()
            cache.cleanUp()
        }
    }

    override suspend fun size(): Long {
        return withContext(Dispatchers.IO) {
            cache.estimatedSize()
        }
    }

    override suspend fun keys(): Set<K> {
        return withContext(Dispatchers.IO) {
            cache.asMap().keys.toSet()
        }
    }

    override suspend fun getAll(keys: Set<K>): Map<K, V> {
        return withContext(Dispatchers.IO) {
            cache.getAllPresent(keys)
                .filterValues { !it.isExpired() }
                .mapValues { it.value.value }
        }
    }

    override suspend fun putAll(entries: Map<K, V>) {
        withContext(Dispatchers.IO) {
            val expiresAt = if (config.defaultTtl == Duration.INFINITE) {
                Long.MAX_VALUE
            } else {
                System.currentTimeMillis() + config.defaultTtl.inWholeMilliseconds
            }
            val cacheEntries = entries.mapValues { CacheEntry(it.value, expiresAt) }
            cache.putAll(cacheEntries)
        }
    }

    override suspend fun removeAll(keys: Set<K>) {
        withContext(Dispatchers.IO) {
            cache.invalidateAll(keys)
        }
    }

    override fun stats(): CacheStats {
        val caffeineStats = cache.stats()
        return CacheStats(
            hits = caffeineStats.hitCount(),
            misses = caffeineStats.missCount(),
            evictions = caffeineStats.evictionCount(),
            size = cache.estimatedSize()
        )
    }

    override fun resetStats() {
        // Caffeine doesn't support resetting stats directly
        // Stats are cumulative since cache creation
        logger.warn("Caffeine cache doesn't support stats reset. Stats are cumulative.")
    }

    /**
     * Get detailed Caffeine statistics.
     */
    fun caffeineStats(): CaffeineStats = cache.stats()

    /**
     * Performs any pending maintenance operations.
     */
    suspend fun cleanUp() {
        withContext(Dispatchers.IO) {
            cache.cleanUp()
        }
    }

    /**
     * Returns the underlying Caffeine cache for advanced operations.
     * Note: The cache contains wrapped entries, not raw values.
     */
    @Suppress("UNCHECKED_CAST")
    fun nativeCache(): CaffeineNativeCache<K, *> = cache as CaffeineNativeCache<K, *>

    /**
     * Cache entry wrapper with TTL support.
     */
    data class CacheEntry<V>(
        val value: V,
        val expiresAt: Long,
        val createdAt: Long = System.currentTimeMillis()
    ) {
        fun isExpired(): Boolean = System.currentTimeMillis() > expiresAt
    }
}

/**
 * Configuration for Caffeine cache.
 */
data class CaffeineCacheConfig(
    /** Maximum number of entries */
    val maxSize: Long = 10_000,

    /** Default TTL for entries without explicit TTL */
    val defaultTtl: Duration = Duration.INFINITE,

    /** Expire entries after write (global policy) */
    val expireAfterWrite: Duration = Duration.INFINITE,

    /** Expire entries after last access */
    val expireAfterAccess: Duration = Duration.INFINITE,

    /** Whether to record statistics */
    val recordStats: Boolean = true,

    /** Use weak references for keys */
    val weakKeys: Boolean = false,

    /** Use weak references for values */
    val weakValues: Boolean = false,

    /** Use soft references for values (allows GC under memory pressure) */
    val softValues: Boolean = false,

    /** Initial capacity hint */
    val initialCapacity: Int = 16
) {
    companion object {
        /**
         * Create config from generic CacheConfig.
         */
        fun from(config: CacheConfig): CaffeineCacheConfig {
            return CaffeineCacheConfig(
                maxSize = config.maxSize,
                defaultTtl = config.defaultTtl,
                recordStats = config.recordStats
            )
        }

        /**
         * Config optimized for session cache (short-lived, high throughput).
         */
        fun forSessionCache(): CaffeineCacheConfig {
            return CaffeineCacheConfig(
                maxSize = 1_000,
                expireAfterAccess = Duration.parse("30m"),
                recordStats = true,
                weakValues = true
            )
        }

        /**
         * Config optimized for query cache (medium TTL, larger size).
         */
        fun forQueryCache(): CaffeineCacheConfig {
            return CaffeineCacheConfig(
                maxSize = 5_000,
                expireAfterWrite = Duration.parse("5m"),
                recordStats = true
            )
        }

        /**
         * Config optimized for entity cache (longer TTL, soft references).
         */
        fun forEntityCache(): CaffeineCacheConfig {
            return CaffeineCacheConfig(
                maxSize = 10_000,
                expireAfterWrite = Duration.parse("1h"),
                recordStats = true,
                softValues = true
            )
        }
    }
}

/**
 * Factory for creating Caffeine caches.
 */
class CaffeineCacheFactory(
    private val defaultConfig: CaffeineCacheConfig = CaffeineCacheConfig()
) : CacheFactory {

    override fun <K : Any, V : Any> createCache(name: String, config: CacheConfig): Cache<K, V> {
        val caffeineConfig = CaffeineCacheConfig.from(config)
        return CaffeineCache(name, caffeineConfig)
    }

    /**
     * Create a cache with Caffeine-specific configuration.
     */
    fun <K : Any, V : Any> createCache(name: String, config: CaffeineCacheConfig): CaffeineCache<K, V> {
        return CaffeineCache(name, config)
    }

    /**
     * Create a cache with default configuration.
     */
    fun <K : Any, V : Any> createCache(name: String): CaffeineCache<K, V> {
        return CaffeineCache(name, defaultConfig)
    }
}
