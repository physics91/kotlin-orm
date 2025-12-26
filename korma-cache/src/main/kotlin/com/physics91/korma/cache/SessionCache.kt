package com.physics91.korma.cache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/**
 * Session-scoped first-level cache.
 * Provides identity mapping for entities within a database session.
 *
 * This cache ensures that:
 * - The same entity is not loaded twice in the same session
 * - Changes to entities are tracked
 * - Memory is released when the session closes
 *
 * Thread-safety: This implementation is thread-safe.
 */
class SessionCache : Cache<EntityKey, Any> {
    override val name: String = "session"

    private val cache = mutableMapOf<EntityKey, CacheEntry>()
    private val mutex = Mutex()

    // Thread-safe statistics
    private val hits = AtomicLong(0)
    private val misses = AtomicLong(0)

    override suspend fun get(key: EntityKey): Any? {
        return mutex.withLock {
            cache[key]?.let {
                hits.incrementAndGet()
                it.value
            } ?: run {
                misses.incrementAndGet()
                null
            }
        }
    }

    override suspend fun getOrPut(key: EntityKey, loader: suspend () -> Any): Any {
        return mutex.withLock {
            cache[key]?.let {
                hits.incrementAndGet()
                it.value
            } ?: run {
                // Execute loader inside lock to prevent duplicate loading
                val value = loader()
                cache[key] = CacheEntry(value)
                misses.incrementAndGet()
                value
            }
        }
    }

    override suspend fun put(key: EntityKey, value: Any) {
        mutex.withLock {
            cache[key] = CacheEntry(value)
        }
    }

    override suspend fun put(key: EntityKey, value: Any, ttl: Duration) {
        // Session cache doesn't support TTL - entries live for session duration
        put(key, value)
    }

    override suspend fun containsKey(key: EntityKey): Boolean {
        return mutex.withLock { cache.containsKey(key) }
    }

    override suspend fun remove(key: EntityKey): Any? {
        return mutex.withLock {
            cache.remove(key)?.value
        }
    }

    override suspend fun clear() {
        mutex.withLock {
            cache.clear()
            hits.set(0)
            misses.set(0)
        }
    }

    override suspend fun size(): Long {
        return mutex.withLock { cache.size.toLong() }
    }

    override suspend fun keys(): Set<EntityKey> {
        return mutex.withLock { cache.keys.toSet() }
    }

    override suspend fun getAll(keys: Set<EntityKey>): Map<EntityKey, Any> {
        return mutex.withLock {
            keys.mapNotNull { key ->
                cache[key]?.let { entry ->
                    hits.incrementAndGet()
                    key to entry.value
                } ?: run {
                    misses.incrementAndGet()
                    null
                }
            }.toMap()
        }
    }

    override suspend fun putAll(entries: Map<EntityKey, Any>) {
        mutex.withLock {
            entries.forEach { (key, value) ->
                cache[key] = CacheEntry(value)
            }
        }
    }

    override suspend fun removeAll(keys: Set<EntityKey>) {
        mutex.withLock {
            keys.forEach { cache.remove(it) }
        }
    }

    /**
     * Get all entities of a specific type.
     */
    suspend fun <T : Any> getByType(entityType: String): List<T> {
        return mutex.withLock {
            cache.entries
                .filter { it.key.entityType == entityType }
                .map {
                    @Suppress("UNCHECKED_CAST")
                    it.value.value as T
                }
        }
    }

    /**
     * Remove all entities of a specific type.
     */
    suspend fun removeByType(entityType: String) {
        mutex.withLock {
            cache.keys
                .filter { it.entityType == entityType }
                .forEach { cache.remove(it) }
        }
    }

    /**
     * Get cache statistics.
     */
    fun stats(): CacheStats {
        return CacheStats(
            hits = hits.get(),
            misses = misses.get(),
            evictions = 0,
            size = cache.size.toLong()
        )
    }

    private data class CacheEntry(
        val value: Any,
        val timestamp: Long = System.currentTimeMillis()
    )
}

/**
 * Key for entity cache entries.
 */
data class EntityKey(
    /** Entity type (table name or class name) */
    val entityType: String,
    /** Primary key value(s) */
    val id: Any
) {
    companion object {
        /**
         * Create an entity key from table name and ID.
         */
        fun of(tableName: String, id: Any): EntityKey = EntityKey(tableName, id)

        /**
         * Create an entity key from entity class and ID.
         */
        inline fun <reified T : Any> of(id: Any): EntityKey =
            EntityKey(T::class.simpleName ?: T::class.java.name, id)
    }
}

/**
 * Composite key for entities with multiple primary key columns.
 */
data class CompositeId(
    val values: Map<String, Any>
) {
    constructor(vararg pairs: Pair<String, Any>) : this(pairs.toMap())

    override fun toString(): String {
        return values.entries.joinToString(",") { "${it.key}=${it.value}" }
    }
}
