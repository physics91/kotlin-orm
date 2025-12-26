package com.physics91.korma.cache.caffeine

import com.physics91.korma.cache.CacheConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class CaffeineCacheTest {

    private lateinit var cache: CaffeineCache<String, String>

    @BeforeEach
    fun setup() {
        cache = CaffeineCache(
            name = "test",
            config = CaffeineCacheConfig(
                maxSize = 100,
                recordStats = true
            )
        )
    }

    @Test
    fun `put and get value`() = runBlocking {
        cache.put("key1", "value1")

        val result = cache.get("key1")

        assertEquals("value1", result)
    }

    @Test
    fun `get returns null for missing key`() = runBlocking {
        val result = cache.get("missing")

        assertNull(result)
    }

    @Test
    fun `getOrPut loads value when missing`() = runBlocking {
        var loadCount = 0

        val result = cache.getOrPut("key1") {
            loadCount++
            "loaded"
        }

        assertEquals("loaded", result)
        assertEquals(1, loadCount)
    }

    @Test
    fun `getOrPut returns cached value when present`() = runBlocking {
        cache.put("key1", "cached")
        var loadCount = 0

        val result = cache.getOrPut("key1") {
            loadCount++
            "loaded"
        }

        assertEquals("cached", result)
        assertEquals(0, loadCount)
    }

    @Test
    fun `remove deletes entry`() = runBlocking {
        cache.put("key1", "value1")

        val removed = cache.remove("key1")

        assertEquals("value1", removed)
        assertNull(cache.get("key1"))
    }

    @Test
    fun `clear removes all entries`() = runBlocking {
        cache.put("key1", "value1")
        cache.put("key2", "value2")

        cache.clear()

        assertEquals(0, cache.size())
    }

    @Test
    fun `containsKey returns correct result`() = runBlocking {
        cache.put("key1", "value1")

        assertTrue(cache.containsKey("key1"))
        assertFalse(cache.containsKey("key2"))
    }

    @Test
    fun `keys returns all keys`() = runBlocking {
        cache.put("key1", "value1")
        cache.put("key2", "value2")

        val keys = cache.keys()

        assertEquals(setOf("key1", "key2"), keys)
    }

    @Test
    fun `getAll returns matching entries`() = runBlocking {
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")

        val result = cache.getAll(setOf("key1", "key3", "missing"))

        assertEquals(2, result.size)
        assertEquals("value1", result["key1"])
        assertEquals("value3", result["key3"])
    }

    @Test
    fun `putAll adds multiple entries`() = runBlocking {
        cache.putAll(mapOf(
            "key1" to "value1",
            "key2" to "value2"
        ))

        assertEquals("value1", cache.get("key1"))
        assertEquals("value2", cache.get("key2"))
    }

    @Test
    fun `removeAll deletes multiple entries`() = runBlocking {
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")

        cache.removeAll(setOf("key1", "key3"))

        assertNull(cache.get("key1"))
        assertEquals("value2", cache.get("key2"))
        assertNull(cache.get("key3"))
    }

    @Test
    fun `ttl expires entries`() = runBlocking {
        cache.put("key1", "value1", 100.milliseconds)

        assertEquals("value1", cache.get("key1"))

        delay(150)

        assertNull(cache.get("key1"))
    }

    @Test
    fun `stats tracks operations`() = runBlocking {
        cache.put("key1", "value1")

        cache.get("key1") // hit
        cache.get("key1") // hit
        cache.get("missing") // miss

        val stats = cache.stats()
        assertTrue(stats.hits >= 2)
        assertTrue(stats.misses >= 1)
    }

    @Test
    fun `caffeineStats returns detailed stats`() = runBlocking {
        cache.put("key1", "value1")
        cache.get("key1")

        val stats = cache.caffeineStats()

        assertNotNull(stats)
        assertTrue(stats.hitCount() >= 1)
    }

    @Test
    fun `expireAfterWrite evicts entries`() = runBlocking {
        val shortLivedCache = CaffeineCache<String, String>(
            name = "short",
            config = CaffeineCacheConfig(
                expireAfterWrite = 100.milliseconds,
                recordStats = true
            )
        )

        shortLivedCache.put("key1", "value1")
        assertEquals("value1", shortLivedCache.get("key1"))

        delay(150)
        shortLivedCache.cleanUp()

        assertNull(shortLivedCache.get("key1"))
    }

    @Test
    fun `expireAfterAccess extends lifetime on access`() = runBlocking {
        val accessCache = CaffeineCache<String, String>(
            name = "access",
            config = CaffeineCacheConfig(
                expireAfterAccess = 200.milliseconds,
                recordStats = true
            )
        )

        accessCache.put("key1", "value1")

        delay(100)
        accessCache.get("key1") // refresh access time

        delay(100)
        // Should still be present since we accessed it
        assertEquals("value1", accessCache.get("key1"))

        delay(250)
        accessCache.cleanUp()
        assertNull(accessCache.get("key1"))
    }

    @Test
    fun `maxSize evicts oldest entries`() = runBlocking {
        val smallCache = CaffeineCache<Int, String>(
            name = "small",
            config = CaffeineCacheConfig(maxSize = 3)
        )

        for (i in 1..5) {
            smallCache.put(i, "value$i")
        }

        smallCache.cleanUp()
        assertTrue(smallCache.size() <= 3)
    }

    @Test
    fun `CaffeineCacheConfig presets`() {
        val sessionConfig = CaffeineCacheConfig.forSessionCache()
        assertEquals(1_000, sessionConfig.maxSize)
        assertTrue(sessionConfig.weakValues)

        val queryConfig = CaffeineCacheConfig.forQueryCache()
        assertEquals(5_000, queryConfig.maxSize)

        val entityConfig = CaffeineCacheConfig.forEntityCache()
        assertEquals(10_000, entityConfig.maxSize)
        assertTrue(entityConfig.softValues)
    }

    @Test
    fun `CaffeineCacheFactory creates caches`() = runBlocking {
        val factory = CaffeineCacheFactory()

        val cache1 = factory.createCache<String, String>("cache1")
        val cache2 = factory.createCache<String, Int>("cache2", CacheConfig(maxSize = 50))

        cache1.put("key", "value")
        cache2.put("key", 42)

        assertEquals("value", cache1.get("key"))
        assertEquals(42, cache2.get("key"))
    }

    @Test
    fun `CaffeineCacheConfig from generic config`() {
        val genericConfig = CacheConfig(
            maxSize = 500,
            recordStats = false
        )

        val caffeineConfig = CaffeineCacheConfig.from(genericConfig)

        assertEquals(500, caffeineConfig.maxSize)
        assertFalse(caffeineConfig.recordStats)
    }
}
