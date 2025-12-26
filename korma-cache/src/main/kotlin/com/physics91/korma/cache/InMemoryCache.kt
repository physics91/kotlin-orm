package com.physics91.korma.cache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/**
 * Simple in-memory cache implementation.
 * Primarily for testing and simple use cases.
 * For production, use CaffeineCache or RedisCache.
 *
 * Thread-safety: This implementation is thread-safe.
 * Statistics are approximate and may have slight inaccuracies under high concurrency.
 */
class InMemoryCache<K : Any, V : Any>(
    override val name: String,
    private val config: CacheConfig = CacheConfig()
) : StatsCache<K, V> {

    private val cache = ConcurrentHashMap<K, CacheEntry<V>>()
    private val mutex = Mutex()

    // Thread-safe statistics
    private val hits = AtomicLong(0)
    private val misses = AtomicLong(0)
    private val evictions = AtomicLong(0)

    override suspend fun get(key: K): V? {
        val entry = cache[key]

        return if (entry != null && !entry.isExpired()) {
            hits.incrementAndGet()
            entry.value
        } else {
            if (entry != null) {
                cache.remove(key)
                evictions.incrementAndGet()
            }
            misses.incrementAndGet()
            null
        }
    }

    override suspend fun getOrPut(key: K, loader: suspend () -> V): V {
        // Fast path: check without lock
        get(key)?.let { return it }

        // Slow path: acquire lock and double-check
        return mutex.withLock {
            // Double-check after acquiring lock
            cache[key]?.takeIf { !it.isExpired() }?.let {
                hits.incrementAndGet()
                return@withLock it.value
            }

            // Load value while holding lock to prevent duplicate loading
            val value = loader()
            val expiresAt = if (config.defaultTtl == Duration.INFINITE) {
                Long.MAX_VALUE
            } else {
                System.currentTimeMillis() + config.defaultTtl.inWholeMilliseconds
            }
            cache[key] = CacheEntry(value, expiresAt)
            misses.incrementAndGet()
            value
        }
    }

    override suspend fun put(key: K, value: V) {
        put(key, value, config.defaultTtl)
    }

    override suspend fun put(key: K, value: V, ttl: Duration) {
        ensureCapacity()

        val expiresAt = if (ttl == Duration.INFINITE) {
            Long.MAX_VALUE
        } else {
            System.currentTimeMillis() + ttl.inWholeMilliseconds
        }

        cache[key] = CacheEntry(value, expiresAt)
    }

    override suspend fun containsKey(key: K): Boolean {
        val entry = cache[key] ?: return false
        if (entry.isExpired()) {
            cache.remove(key)
            return false
        }
        return true
    }

    override suspend fun remove(key: K): V? {
        return cache.remove(key)?.value
    }

    override suspend fun clear() {
        cache.clear()
    }

    override suspend fun size(): Long = cache.size.toLong()

    override suspend fun keys(): Set<K> {
        // Clean expired entries first
        cleanExpired()
        return cache.keys.toSet()
    }

    override suspend fun getAll(keys: Set<K>): Map<K, V> {
        return keys.mapNotNull { key ->
            get(key)?.let { key to it }
        }.toMap()
    }

    override suspend fun putAll(entries: Map<K, V>) {
        entries.forEach { (k, v) -> put(k, v) }
    }

    override suspend fun removeAll(keys: Set<K>) {
        keys.forEach { cache.remove(it) }
    }

    override fun stats(): CacheStats {
        return CacheStats(
            hits = hits.get(),
            misses = misses.get(),
            evictions = evictions.get(),
            size = cache.size.toLong()
        )
    }

    override fun resetStats() {
        hits.set(0)
        misses.set(0)
        evictions.set(0)
    }

    private suspend fun ensureCapacity() {
        if (cache.size >= config.maxSize) {
            mutex.withLock {
                if (cache.size >= config.maxSize) {
                    // Simple eviction: remove oldest entries
                    val toRemove = cache.entries
                        .sortedBy { it.value.createdAt }
                        .take((config.maxSize / 10).toInt().coerceAtLeast(1))
                        .map { it.key }

                    toRemove.forEach {
                        cache.remove(it)
                        evictions.incrementAndGet()
                    }
                }
            }
        }
    }

    private fun cleanExpired() {
        val now = System.currentTimeMillis()
        cache.entries.removeIf { entry ->
            if (entry.value.expiresAt <= now) {
                evictions.incrementAndGet()
                true
            } else {
                false
            }
        }
    }

    private data class CacheEntry<V>(
        val value: V,
        val expiresAt: Long,
        val createdAt: Long = System.currentTimeMillis()
    ) {
        fun isExpired(): Boolean = System.currentTimeMillis() > expiresAt
    }
}

/**
 * Factory for creating in-memory caches.
 */
class InMemoryCacheFactory : CacheFactory {
    override fun <K : Any, V : Any> createCache(name: String, config: CacheConfig): Cache<K, V> {
        return InMemoryCache(name, config)
    }
}
