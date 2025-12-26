package com.physics91.korma.cache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Manages multiple cache instances.
 * Provides a centralized way to create, access, and manage caches.
 */
interface CacheManager {
    /**
     * Get or create a cache with the given name.
     */
    fun <K : Any, V : Any> getCache(name: String): Cache<K, V>

    /**
     * Get or create a cache with the given name and configuration.
     */
    fun <K : Any, V : Any> getCache(name: String, config: CacheConfig): Cache<K, V>

    /**
     * Get all cache names.
     */
    fun getCacheNames(): Set<String>

    /**
     * Remove a cache.
     */
    suspend fun removeCache(name: String)

    /**
     * Clear all caches.
     */
    suspend fun clearAll()

    /**
     * Close the cache manager and release resources.
     */
    suspend fun close()
}

/**
 * Factory for creating cache instances.
 */
interface CacheFactory {
    /**
     * Create a new cache instance.
     */
    fun <K : Any, V : Any> createCache(name: String, config: CacheConfig): Cache<K, V>
}

/**
 * Default cache manager implementation.
 *
 * Thread-safety: This implementation is thread-safe.
 * Uses ConcurrentHashMap.computeIfAbsent for atomic cache creation
 * and Mutex for coroutine-safe suspend operations.
 */
class DefaultCacheManager(
    private val factory: CacheFactory,
    private val defaultConfig: CacheConfig = CacheConfig()
) : CacheManager {
    private val logger = LoggerFactory.getLogger(DefaultCacheManager::class.java)
    private val caches = ConcurrentHashMap<String, Cache<*, *>>()
    private val mutex = Mutex()  // Single synchronization mechanism for suspend functions

    @Suppress("UNCHECKED_CAST")
    override fun <K : Any, V : Any> getCache(name: String): Cache<K, V> {
        return getCache(name, defaultConfig)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K : Any, V : Any> getCache(name: String, config: CacheConfig): Cache<K, V> {
        // Use computeIfAbsent for atomic, lock-free cache creation
        return caches.computeIfAbsent(name) {
            logger.debug("Creating cache: {}", name)
            factory.createCache<K, V>(name, config)
        } as Cache<K, V>
    }

    override fun getCacheNames(): Set<String> = caches.keys.toSet()

    override suspend fun removeCache(name: String) {
        mutex.withLock {
            caches.remove(name)?.also {
                logger.debug("Removed cache: {}", name)
            }
        }
    }

    override suspend fun clearAll() {
        mutex.withLock {
            caches.values.forEach { cache ->
                cache.clear()
            }
            logger.debug("Cleared all caches")
        }
    }

    override suspend fun close() {
        mutex.withLock {
            caches.clear()
            logger.debug("Cache manager closed")
        }
    }
}

/**
 * Cache region for grouping related cache entries.
 */
class CacheRegion<K : Any, V : Any>(
    private val cache: Cache<K, V>,
    private val prefix: String
) {
    private fun prefixKey(key: K): K {
        @Suppress("UNCHECKED_CAST")
        return when (key) {
            is String -> "$prefix:$key" as K
            else -> key
        }
    }

    suspend fun get(key: K): V? = cache.get(prefixKey(key))

    suspend fun put(key: K, value: V) = cache.put(prefixKey(key), value)

    suspend fun remove(key: K): V? = cache.remove(prefixKey(key))
}
