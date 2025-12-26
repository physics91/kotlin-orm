package com.physics91.korma.cache

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.minutes

class QueryCacheTest {

    private lateinit var delegateCache: InMemoryCache<QueryKey, QueryResult>
    private lateinit var queryCache: QueryCache

    @BeforeEach
    fun setup() {
        delegateCache = InMemoryCache(
            name = "query",
            config = CacheConfig(maxSize = 1000)
        )
        queryCache = QueryCache(delegateCache, 5.minutes)
    }

    @Test
    fun `cache query result`() = runBlocking {
        val sql = "SELECT * FROM users WHERE id = ?"
        val params = listOf(1L)
        val tables = setOf("users")
        var loadCount = 0

        val result1 = queryCache.getOrPut(sql, params, tables) {
            loadCount++
            listOf(mapOf("id" to 1L, "name" to "John"))
        }

        val result2 = queryCache.getOrPut(sql, params, tables) {
            loadCount++
            listOf(mapOf("id" to 1L, "name" to "John"))
        }

        assertEquals(1, loadCount)
        assertEquals(result1, result2)
    }

    @Test
    fun `different params create different cache entries`() = runBlocking {
        val sql = "SELECT * FROM users WHERE id = ?"
        val tables = setOf("users")
        var loadCount = 0

        queryCache.getOrPut(sql, listOf(1L), tables) {
            loadCount++
            listOf(mapOf("id" to 1L))
        }

        queryCache.getOrPut(sql, listOf(2L), tables) {
            loadCount++
            listOf(mapOf("id" to 2L))
        }

        assertEquals(2, loadCount)
    }

    @Test
    fun `invalidate table removes related queries`() = runBlocking {
        val sql1 = "SELECT * FROM users"
        val sql2 = "SELECT * FROM orders"
        val sql3 = "SELECT * FROM users WHERE id = 1"

        queryCache.getOrPut(sql1, emptyList(), setOf("users")) { listOf("user1") }
        queryCache.getOrPut(sql2, emptyList(), setOf("orders")) { listOf("order1") }
        queryCache.getOrPut(sql3, emptyList(), setOf("users")) { listOf("user2") }

        queryCache.invalidateTable("users")

        // Users queries should be reloaded
        var usersLoaded = false
        var ordersLoaded = false

        queryCache.getOrPut(sql1, emptyList(), setOf("users")) {
            usersLoaded = true
            listOf("user1-reloaded")
        }

        queryCache.getOrPut(sql2, emptyList(), setOf("orders")) {
            ordersLoaded = true
            listOf("order1-reloaded")
        }

        assertTrue(usersLoaded)
        assertFalse(ordersLoaded)
    }

    @Test
    fun `invalidate multiple tables`() = runBlocking {
        queryCache.getOrPut("SELECT * FROM users", emptyList(), setOf("users")) { listOf("u") }
        queryCache.getOrPut("SELECT * FROM orders", emptyList(), setOf("orders")) { listOf("o") }
        queryCache.getOrPut("SELECT * FROM products", emptyList(), setOf("products")) { listOf("p") }

        queryCache.invalidateTables(setOf("users", "orders"))

        var usersLoaded = false
        var ordersLoaded = false
        var productsLoaded = false

        queryCache.getOrPut("SELECT * FROM users", emptyList(), setOf("users")) {
            usersLoaded = true
            listOf()
        }
        queryCache.getOrPut("SELECT * FROM orders", emptyList(), setOf("orders")) {
            ordersLoaded = true
            listOf()
        }
        queryCache.getOrPut("SELECT * FROM products", emptyList(), setOf("products")) {
            productsLoaded = true
            listOf()
        }

        assertTrue(usersLoaded)
        assertTrue(ordersLoaded)
        assertFalse(productsLoaded)
    }

    @Test
    fun `join queries track multiple table dependencies`() = runBlocking {
        val joinSql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        val tables = setOf("users", "orders")

        queryCache.getOrPut(joinSql, emptyList(), tables) { listOf("join-result") }

        // Invalidating either table should invalidate the join query
        queryCache.invalidateTable("orders")

        var reloaded = false
        queryCache.getOrPut(joinSql, emptyList(), tables) {
            reloaded = true
            listOf("join-result-reloaded")
        }

        assertTrue(reloaded)
    }

    @Test
    fun `clear removes all cached queries`() = runBlocking {
        queryCache.getOrPut("SELECT 1", emptyList(), setOf("t1")) { listOf(1) }
        queryCache.getOrPut("SELECT 2", emptyList(), setOf("t2")) { listOf(2) }

        queryCache.clear()

        var loadCount = 0
        queryCache.getOrPut("SELECT 1", emptyList(), setOf("t1")) {
            loadCount++
            listOf(1)
        }
        queryCache.getOrPut("SELECT 2", emptyList(), setOf("t2")) {
            loadCount++
            listOf(2)
        }

        assertEquals(2, loadCount)
    }

    @Test
    fun `QueryKey generation is consistent`() {
        val key1 = QueryKey.of("SELECT * FROM users", listOf(1, "test"))
        val key2 = QueryKey.of("SELECT * FROM users", listOf(1, "test"))
        val key3 = QueryKey.of("SELECT * FROM users", listOf(2, "test"))

        assertEquals(key1, key2)
        assertEquals(key1.hashCode(), key2.hashCode())
        assertNotEquals(key1, key3)
    }

    @Test
    fun `stats returns delegate stats`() = runBlocking {
        delegateCache.resetStats()

        queryCache.getOrPut("SELECT 1", emptyList(), emptySet()) { listOf(1) }
        queryCache.getOrPut("SELECT 1", emptyList(), emptySet()) { listOf(1) } // hit

        val stats = queryCache.stats()
        assertNotNull(stats)
        assertEquals(1, stats?.hits)
    }
}
