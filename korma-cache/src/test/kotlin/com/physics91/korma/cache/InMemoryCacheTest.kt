package com.physics91.korma.cache

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class InMemoryCacheTest {

    private lateinit var cache: InMemoryCache<String, String>

    @BeforeEach
    fun setup() {
        cache = InMemoryCache(
            name = "test",
            config = CacheConfig(maxSize = 100)
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
        assertEquals("loaded", cache.get("key1"))
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
        assertNull(cache.get("key1"))
        assertNull(cache.get("key2"))
    }

    @Test
    fun `containsKey returns correct result`() = runBlocking {
        cache.put("key1", "value1")

        assertTrue(cache.containsKey("key1"))
        assertFalse(cache.containsKey("key2"))
    }

    @Test
    fun `size returns correct count`() = runBlocking {
        assertEquals(0, cache.size())

        cache.put("key1", "value1")
        assertEquals(1, cache.size())

        cache.put("key2", "value2")
        assertEquals(2, cache.size())

        cache.remove("key1")
        assertEquals(1, cache.size())
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

        assertEquals(mapOf("key1" to "value1", "key3" to "value3"), result)
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
    fun `stats tracks hits and misses`() = runBlocking {
        cache.resetStats()
        cache.put("key1", "value1")

        cache.get("key1") // hit
        cache.get("key1") // hit
        cache.get("missing") // miss

        val stats = cache.stats()
        assertEquals(2, stats.hits)
        assertEquals(1, stats.misses)
    }

    @Test
    fun `eviction when capacity exceeded`() = runBlocking {
        val smallCache = InMemoryCache<String, String>(
            name = "small",
            config = CacheConfig(maxSize = 5)
        )

        // Add 6 entries to trigger eviction
        for (i in 1..6) {
            smallCache.put("key$i", "value$i")
        }

        assertTrue(smallCache.size() <= 5)
    }
}
