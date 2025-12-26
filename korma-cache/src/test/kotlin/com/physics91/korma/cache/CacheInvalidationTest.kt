package com.physics91.korma.cache

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class CacheInvalidationTest {

    private lateinit var sessionCache: SessionCache
    private lateinit var delegateCache: InMemoryCache<QueryKey, QueryResult>
    private lateinit var queryCache: QueryCache

    @BeforeEach
    fun setup() {
        sessionCache = SessionCache()
        delegateCache = InMemoryCache("query", CacheConfig(maxSize = 1000))
        queryCache = QueryCache(delegateCache)
    }

    @Test
    fun `FullInvalidationStrategy clears all entries for table`() = runBlocking {
        val strategy = FullInvalidationStrategy(sessionCache, queryCache)

        // Add entities
        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L))
        sessionCache.put(EntityKey.of("orders", 1L), mapOf("id" to 1L))

        // Add query
        queryCache.getOrPut("SELECT * FROM users", emptyList(), setOf("users")) { listOf("result") }

        // Invalidate users
        strategy.onTableModified("users", ModificationOperation.UPDATE, null)

        // Users should be removed
        assertNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNull(sessionCache.get(EntityKey.of("users", 2L)))

        // Orders should remain
        assertNotNull(sessionCache.get(EntityKey.of("orders", 1L)))
    }

    @Test
    fun `TargetedInvalidationStrategy removes only affected entities on UPDATE`() = runBlocking {
        val strategy = TargetedInvalidationStrategy(sessionCache, queryCache)

        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L))
        sessionCache.put(EntityKey.of("users", 3L), mapOf("id" to 3L))

        strategy.onTableModified("users", ModificationOperation.UPDATE, setOf(1L, 2L))

        assertNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNull(sessionCache.get(EntityKey.of("users", 2L)))
        assertNotNull(sessionCache.get(EntityKey.of("users", 3L)))
    }

    @Test
    fun `TargetedInvalidationStrategy removes all on UPDATE without affectedIds`() = runBlocking {
        val strategy = TargetedInvalidationStrategy(sessionCache, queryCache)

        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L))

        strategy.onTableModified("users", ModificationOperation.UPDATE, null)

        assertNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNull(sessionCache.get(EntityKey.of("users", 2L)))
    }

    @Test
    fun `TargetedInvalidationStrategy handles INSERT`() = runBlocking {
        val strategy = TargetedInvalidationStrategy(sessionCache, queryCache)

        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        queryCache.getOrPut("SELECT * FROM users", emptyList(), setOf("users")) { listOf("result") }

        strategy.onTableModified("users", ModificationOperation.INSERT, setOf(2L))

        // Existing entity should remain
        assertNotNull(sessionCache.get(EntityKey.of("users", 1L)))

        // But query cache should be invalidated (new data added)
        var reloaded = false
        queryCache.getOrPut("SELECT * FROM users", emptyList(), setOf("users")) {
            reloaded = true
            listOf("new result")
        }
        assertTrue(reloaded)
    }

    @Test
    fun `TargetedInvalidationStrategy handles DELETE`() = runBlocking {
        val strategy = TargetedInvalidationStrategy(sessionCache, queryCache)

        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L))

        strategy.onTableModified("users", ModificationOperation.DELETE, setOf(1L))

        assertNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNotNull(sessionCache.get(EntityKey.of("users", 2L)))
    }

    @Test
    fun `TargetedInvalidationStrategy handles TRUNCATE`() = runBlocking {
        val strategy = TargetedInvalidationStrategy(sessionCache, queryCache)

        sessionCache.put(EntityKey.of("users", 1L), mapOf("id" to 1L))
        sessionCache.put(EntityKey.of("users", 2L), mapOf("id" to 2L))

        strategy.onTableModified("users", ModificationOperation.TRUNCATE, null)

        assertNull(sessionCache.get(EntityKey.of("users", 1L)))
        assertNull(sessionCache.get(EntityKey.of("users", 2L)))
    }

    @Test
    fun `VersionedInvalidationStrategy increments version`() = runBlocking {
        val strategy = VersionedInvalidationStrategy(sessionCache, queryCache)

        assertEquals(0, strategy.getTableVersion("users"))

        strategy.onTableModified("users", ModificationOperation.INSERT, null)
        assertEquals(1, strategy.getTableVersion("users"))

        strategy.onTableModified("users", ModificationOperation.UPDATE, null)
        assertEquals(2, strategy.getTableVersion("users"))
    }

    @Test
    fun `CacheInvalidationManager notifies strategy`() = runBlocking {
        var notified = false
        val strategy = object : CacheInvalidationStrategy {
            override suspend fun onTableModified(
                tableName: String,
                operation: ModificationOperation,
                affectedIds: Set<Any>?
            ) {
                notified = true
                assertEquals("users", tableName)
                assertEquals(ModificationOperation.UPDATE, operation)
            }

            override suspend fun shouldInvalidate(key: Any, tableName: String) = true
        }

        val manager = CacheInvalidationManager(strategy)

        manager.notifyModification("users", ModificationOperation.UPDATE)

        assertTrue(notified)
    }

    @Test
    fun `CacheInvalidationManager notifies listeners`() = runBlocking {
        val strategy = FullInvalidationStrategy(sessionCache, queryCache)
        val manager = CacheInvalidationManager(strategy)

        var receivedEvent: InvalidationEvent? = null
        manager.addListener { event ->
            receivedEvent = event
        }

        manager.notifyModification("users", ModificationOperation.DELETE, setOf(1L, 2L))

        assertNotNull(receivedEvent)
        assertEquals("users", receivedEvent?.tableName)
        assertEquals(ModificationOperation.DELETE, receivedEvent?.operation)
        assertEquals(setOf(1L, 2L), receivedEvent?.affectedIds)
    }

    @Test
    fun `CacheInvalidationManager can remove listener`() = runBlocking {
        val strategy = FullInvalidationStrategy(sessionCache, queryCache)
        val manager = CacheInvalidationManager(strategy)

        var callCount = 0
        val listener = InvalidationListener { callCount++ }

        manager.addListener(listener)
        manager.notifyModification("users", ModificationOperation.INSERT)
        assertEquals(1, callCount)

        manager.removeListener(listener)
        manager.notifyModification("users", ModificationOperation.INSERT)
        assertEquals(1, callCount)
    }

    @Test
    fun `InvalidationEvent has timestamp`() {
        val event = InvalidationEvent("users", ModificationOperation.UPDATE, setOf(1L))

        assertNotNull(event.timestamp)
        assertEquals("users", event.tableName)
        assertEquals(ModificationOperation.UPDATE, event.operation)
        assertEquals(setOf(1L), event.affectedIds)
    }
}
