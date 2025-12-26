@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package com.physics91.korma.cache.redis

import com.physics91.korma.cache.*
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlin.time.Duration

/**
 * Distributed cache implementation using Redis.
 * Recommended for L2 caching in clustered environments.
 *
 * Features:
 * - Distributed caching across multiple instances
 * - TTL support at Redis level
 * - Serialization via kotlinx-serialization
 * - Connection pooling via Lettuce
 */
class RedisCache<K : Any, V : Any>(
    override val name: String,
    private val connection: StatefulRedisConnection<String, String>,
    private val config: RedisCacheConfig = RedisCacheConfig(),
    private val keySerializer: (K) -> String,
    private val valueSerializer: (V) -> String,
    private val valueDeserializer: (String) -> V
) : StatsCache<K, V> {

    private val logger = LoggerFactory.getLogger(RedisCache::class.java)
    private val commands: RedisCoroutinesCommands<String, String> = connection.coroutines()

    // Statistics (local tracking, not distributed)
    private var hits = 0L
    private var misses = 0L

    private fun keyPrefix(): String = "${config.keyPrefix}:$name:"

    private fun fullKey(key: K): String = "${keyPrefix()}${keySerializer(key)}"

    override suspend fun get(key: K): V? {
        val fullKey = fullKey(key)
        val value = commands.get(fullKey)

        return if (value != null) {
            hits++
            try {
                valueDeserializer(value)
            } catch (e: Exception) {
                logger.warn("Failed to deserialize cached value for key: {}", fullKey, e)
                commands.del(fullKey)
                misses++
                null
            }
        } else {
            misses++
            null
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
        val fullKey = fullKey(key)
        val serialized = valueSerializer(value)

        if (ttl == Duration.INFINITE) {
            commands.set(fullKey, serialized)
        } else {
            commands.setex(fullKey, ttl.inWholeSeconds, serialized)
        }
    }

    override suspend fun containsKey(key: K): Boolean {
        return (commands.exists(fullKey(key)) ?: 0L) > 0
    }

    override suspend fun remove(key: K): V? {
        val fullKey = fullKey(key)
        val existing = get(key)
        commands.del(fullKey)
        return existing
    }

    override suspend fun clear() {
        val pattern = "${keyPrefix()}*"
        val keys = commands.keys(pattern).toList()
        if (keys.isNotEmpty()) {
            commands.del(*keys.toTypedArray())
        }
    }

    override suspend fun size(): Long {
        val pattern = "${keyPrefix()}*"
        return commands.keys(pattern).toList().size.toLong()
    }

    override suspend fun keys(): Set<K> {
        // Note: This is expensive for large datasets
        // Consider using SCAN in production
        logger.warn("keys() on Redis cache can be expensive. Consider using pattern-based operations.")
        val pattern = "${keyPrefix()}*"
        val prefix = keyPrefix()
        return commands.keys(pattern)
            .toList()
            .map { it.removePrefix(prefix) }
            .mapNotNull {
                @Suppress("UNCHECKED_CAST")
                it as? K
            }
            .toSet()
    }

    override suspend fun getAll(keys: Set<K>): Map<K, V> {
        if (keys.isEmpty()) return emptyMap()

        val fullKeys = keys.map { fullKey(it) }
        val values = commands.mget(*fullKeys.toTypedArray()).toList()

        val result = mutableMapOf<K, V>()
        keys.zip(values) { key, keyValue ->
            keyValue.value?.let { value ->
                try {
                    result[key] = valueDeserializer(value)
                    hits++
                } catch (e: Exception) {
                    logger.warn("Failed to deserialize value for key: {}", key, e)
                    misses++
                }
            } ?: run {
                misses++
            }
        }
        return result
    }

    override suspend fun putAll(entries: Map<K, V>) {
        if (entries.isEmpty()) return

        val serialized = entries.map { (k, v) ->
            fullKey(k) to valueSerializer(v)
        }.toMap()

        commands.mset(serialized)

        // Set TTL for each key if not infinite
        if (config.defaultTtl != Duration.INFINITE) {
            serialized.keys.forEach { key ->
                commands.expire(key, config.defaultTtl.inWholeSeconds)
            }
        }
    }

    override suspend fun removeAll(keys: Set<K>) {
        if (keys.isEmpty()) return
        val fullKeys = keys.map { fullKey(it) }
        commands.del(*fullKeys.toTypedArray())
    }

    override fun stats(): CacheStats {
        return CacheStats(
            hits = hits,
            misses = misses,
            evictions = 0, // Redis doesn't expose eviction count easily
            size = -1 // Would need to call size() which is expensive
        )
    }

    override fun resetStats() {
        hits = 0
        misses = 0
    }

    /**
     * Get TTL for a specific key.
     */
    suspend fun ttl(key: K): Long {
        return commands.ttl(fullKey(key)) ?: -1
    }

    /**
     * Set expiration for an existing key.
     */
    suspend fun expire(key: K, ttl: Duration): Boolean {
        return commands.expire(fullKey(key), ttl.inWholeSeconds) ?: false
    }

    /**
     * Remove expiration from a key.
     */
    suspend fun persist(key: K): Boolean {
        return commands.persist(fullKey(key)) ?: false
    }
}

/**
 * Redis cache configuration.
 */
data class RedisCacheConfig(
    /** Default TTL for entries */
    val defaultTtl: Duration = Duration.parse("1h"),

    /** Key prefix for namespacing */
    val keyPrefix: String = "korma",

    /** Whether to record stats locally */
    val recordStats: Boolean = true,

    /** Connection timeout */
    val connectionTimeout: Duration = Duration.parse("10s"),

    /** Command timeout */
    val commandTimeout: Duration = Duration.parse("5s")
) {
    companion object {
        /**
         * Create config from generic CacheConfig.
         */
        fun from(config: CacheConfig): RedisCacheConfig {
            return RedisCacheConfig(
                defaultTtl = config.defaultTtl,
                recordStats = config.recordStats
            )
        }
    }
}

/**
 * Factory for creating Redis caches.
 */
class RedisCacheFactory(
    private val redisClient: RedisClient,
    @PublishedApi internal val defaultConfig: RedisCacheConfig = RedisCacheConfig(),
    @PublishedApi internal val json: Json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }
) : CacheFactory, AutoCloseable {

    private val logger = LoggerFactory.getLogger(RedisCacheFactory::class.java)
    @PublishedApi internal val connections = mutableMapOf<String, StatefulRedisConnection<String, String>>()

    companion object {
        /**
         * Create factory with Redis URI.
         */
        fun create(uri: String): RedisCacheFactory {
            val client = RedisClient.create(uri)
            return RedisCacheFactory(client)
        }

        /**
         * Create factory with host and port.
         */
        fun create(host: String = "localhost", port: Int = 6379, password: String? = null): RedisCacheFactory {
            val uriBuilder = RedisURI.Builder.redis(host, port)
            password?.let { uriBuilder.withPassword(it.toCharArray()) }
            val client = RedisClient.create(uriBuilder.build())
            return RedisCacheFactory(client)
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K : Any, V : Any> createCache(name: String, config: CacheConfig): Cache<K, V> {
        val redisConfig = RedisCacheConfig.from(config)
        return createStringCache(name, redisConfig) as Cache<K, V>
    }

    /**
     * Create a cache with string keys and JSON-serialized values.
     */
    inline fun <reified V : Any> createJsonCache(
        name: String,
        config: RedisCacheConfig = defaultConfig
    ): RedisCache<String, V> {
        val connection = getOrCreateConnection(name)
        val serializer = json.serializersModule.serializer<V>()

        return RedisCache(
            name = name,
            connection = connection,
            config = config,
            keySerializer = { it },
            valueSerializer = { json.encodeToString(serializer, it) },
            valueDeserializer = { json.decodeFromString(serializer, it) }
        )
    }

    /**
     * Create a cache with string keys and string values.
     */
    fun createStringCache(
        name: String,
        config: RedisCacheConfig = defaultConfig
    ): RedisCache<String, String> {
        val connection = getOrCreateConnection(name)

        return RedisCache(
            name = name,
            connection = connection,
            config = config,
            keySerializer = { it },
            valueSerializer = { it },
            valueDeserializer = { it }
        )
    }

    /**
     * Create a cache with custom serializers.
     */
    fun <K : Any, V : Any> createCache(
        name: String,
        config: RedisCacheConfig = defaultConfig,
        keySerializer: (K) -> String,
        valueSerializer: (V) -> String,
        valueDeserializer: (String) -> V
    ): RedisCache<K, V> {
        val connection = getOrCreateConnection(name)

        return RedisCache(
            name = name,
            connection = connection,
            config = config,
            keySerializer = keySerializer,
            valueSerializer = valueSerializer,
            valueDeserializer = valueDeserializer
        )
    }

    @PublishedApi
    internal fun getOrCreateConnection(name: String): StatefulRedisConnection<String, String> {
        return connections.getOrPut(name) {
            logger.debug("Creating Redis connection for cache: {}", name)
            redisClient.connect()
        }
    }

    override fun close() {
        logger.debug("Closing Redis cache factory")
        connections.values.forEach { it.close() }
        connections.clear()
        redisClient.shutdown()
    }
}

/**
 * Redis pub/sub based cache invalidation.
 * Uses a dedicated coroutine scope to handle invalidation events without blocking.
 */
class RedisCacheInvalidator(
    private val redisClient: RedisClient,
    private val channel: String = "korma:cache:invalidation",
    private val dispatcher: kotlinx.coroutines.CoroutineDispatcher = kotlinx.coroutines.Dispatchers.Default
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(RedisCacheInvalidator::class.java)
    private val pubConnection = redisClient.connectPubSub()
    private val subConnection = redisClient.connectPubSub()
    private val listeners = mutableListOf<InvalidationListener>()

    // Use SupervisorJob to prevent listener failures from canceling other listeners
    private val scope = kotlinx.coroutines.CoroutineScope(
        kotlinx.coroutines.SupervisorJob() + dispatcher
    )

    init {
        // Subscribe to invalidation channel
        subConnection.addListener(object : io.lettuce.core.pubsub.RedisPubSubAdapter<String, String>() {
            override fun message(channel: String, message: String) {
                handleInvalidationMessage(message)
            }
        })
        subConnection.sync().subscribe(channel)
    }

    /**
     * Publish an invalidation event.
     */
    fun publishInvalidation(event: InvalidationEvent) {
        val message = "${event.tableName}|${event.operation}|${event.affectedIds?.joinToString(",") ?: ""}"
        pubConnection.sync().publish(channel, message)
        logger.debug("Published invalidation: {}", message)
    }

    /**
     * Add a listener for invalidation events.
     */
    fun addListener(listener: InvalidationListener) {
        listeners.add(listener)
    }

    /**
     * Remove a listener.
     */
    fun removeListener(listener: InvalidationListener) {
        listeners.remove(listener)
    }

    private fun handleInvalidationMessage(message: String) {
        try {
            val parts = message.split("|")
            val tableName = parts[0]
            val operation = ModificationOperation.valueOf(parts[1])
            val affectedIds = if (parts.size > 2 && parts[2].isNotEmpty()) {
                parts[2].split(",").toSet()
            } else {
                null
            }

            val event = InvalidationEvent(tableName, operation, affectedIds)

            // Launch in coroutine scope instead of blocking
            listeners.forEach { listener ->
                scope.launch {
                    try {
                        listener.onInvalidation(event)
                    } catch (e: Exception) {
                        logger.error("Error in invalidation listener", e)
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to parse invalidation message: {}", message, e)
        }
    }

    override fun close() {
        scope.cancel()
        pubConnection.close()
        subConnection.close()
    }
}
