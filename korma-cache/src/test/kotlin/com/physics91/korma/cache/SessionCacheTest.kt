package com.physics91.korma.cache

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SessionCacheTest {

    private lateinit var sessionCache: SessionCache

    @BeforeEach
    fun setup() {
        sessionCache = SessionCache()
    }

    @Test
    fun `put and get entity`() = runBlocking {
        val key = EntityKey.of("users", 1L)
        val entity = mapOf("id" to 1L, "name" to "John")

        sessionCache.put(key, entity)

        val result = sessionCache.get(key)
        assertEquals(entity, result)
    }

    @Test
    fun `get returns null for missing entity`() = runBlocking {
        val key = EntityKey.of("users", 999L)

        val result = sessionCache.get(key)

        assertNull(result)
    }

    @Test
    fun `getOrPut loads entity when missing`() = runBlocking {
        val key = EntityKey.of("users", 1L)
        var loadCount = 0

        val result = sessionCache.getOrPut(key) {
            loadCount++
            mapOf("id" to 1L, "name" to "Loaded")
        }

        assertEquals(1, loadCount)
        assertEquals("Loaded", (result as Map<*, *>)["name"])
    }

    @Test
    fun `getOrPut returns cached entity when present`() = runBlocking {
        val key = EntityKey.of("users", 1L)
        val entity = mapOf("id" to 1L, "name" to "Cached")
        sessionCache.put(key, entity)
        var loadCount = 0

        val result = sessionCache.getOrPut(key) {
            loadCount++
            mapOf("id" to 1L, "name" to "Loaded")
        }

        assertEquals(0, loadCount)
        assertEquals("Cached", (result as Map<*, *>)["name"])
    }

    @Test
    fun `remove entity from cache`() = runBlocking {
        val key = EntityKey.of("users", 1L)
        val entity = mapOf("id" to 1L, "name" to "John")
        sessionCache.put(key, entity)

        val removed = sessionCache.remove(key)

        assertEquals(entity, removed)
        assertNull(sessionCache.get(key))
    }

    @Test
    fun `getByType returns entities of specific type`() = runBlocking {
        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L, "name" to "User1"))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L, "name" to "User2"))
        sessionCache.put(EntityKey.of("orders", 1L), mapOf("id" to 1L, "total" to 100))

        val users: List<Map<*, *>> = sessionCache.getByType("users")

        assertEquals(2, users.size)
        assertTrue(users.all { it.containsKey("name") })
    }

    @Test
    fun `removeByType removes entities of specific type`() = runBlocking {
        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L, "name" to "User1"))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L, "name" to "User2"))
        sessionCache.put(EntityKey.of("orders", 1L), mapOf("id" to 1L, "total" to 100))

        sessionCache.removeByType("users")

        assertNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNull(sessionCache.get(EntityKey.of("users", 2L)))
        assertNotNull(sessionCache.get(EntityKey.of("orders", 1L)))
    }

    @Test
    fun `clear removes all entities`() = runBlocking {
        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        sessionCache.put(EntityKey.of("orders", 1L), mapOf("id" to 1L))

        sessionCache.clear()

        assertEquals(0, sessionCache.size())
    }

    @Test
    fun `stats tracks hits and misses`() = runBlocking {
        val key = EntityKey.of("users", 1L)
        sessionCache.put(key, mapOf("id" to 1L))

        sessionCache.get(key) // hit
        sessionCache.get(key) // hit
        sessionCache.get(EntityKey.of("users", 999L)) // miss

        val stats = sessionCache.stats()
        assertEquals(2, stats.hits)
        assertEquals(1, stats.misses)
    }

    @Test
    fun `getAll returns matching entities`() = runBlocking {
        val key1 = EntityKey.of("users", 1L)
        val key2 = EntityKey.of("users", 2L)
        val key3 = EntityKey.of("users", 3L)

        sessionCache.put(key1, mapOf("id" to 1L))
        sessionCache.put(key2, mapOf("id" to 2L))

        val result = sessionCache.getAll(setOf(key1, key2, key3))

        assertEquals(2, result.size)
        assertTrue(result.containsKey(key1))
        assertTrue(result.containsKey(key2))
        assertFalse(result.containsKey(key3))
    }

    @Test
    fun `putAll adds multiple entities`() = runBlocking {
        val entries = mapOf(
            EntityKey.of("users", 1L) to mapOf("id" to 1L),
            EntityKey.of("users", 2L) to mapOf("id" to 2L)
        )

        sessionCache.putAll(entries)

        assertEquals(2, sessionCache.size())
        assertNotNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNotNull(sessionCache.get(EntityKey.of("users", 2L)))
    }

    @Test
    fun `EntityKey equals and hashCode`() {
        val key1 = EntityKey.of("users", 1L)
        val key2 = EntityKey.of("users", 1L)
        val key3 = EntityKey.of("users", 2L)
        val key4 = EntityKey.of("orders", 1L)

        assertEquals(key1, key2)
        assertEquals(key1.hashCode(), key2.hashCode())
        assertNotEquals(key1, key3)
        assertNotEquals(key1, key4)
    }

    @Test
    fun `CompositeId for multi-column keys`() {
        val compositeId = CompositeId("userId" to 1L, "roleId" to 2L)
        val key = EntityKey.of("user_roles", compositeId)

        assertEquals("user_roles", key.entityType)
        assertEquals(compositeId, key.id)

        val id = key.id as CompositeId
        assertEquals(1L, id.values["userId"])
        assertEquals(2L, id.values["roleId"])
    }
}
