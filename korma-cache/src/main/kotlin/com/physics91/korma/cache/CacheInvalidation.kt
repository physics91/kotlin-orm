package com.physics91.korma.cache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Instant

/**
 * Strategy for cache invalidation.
 */
interface CacheInvalidationStrategy {
    /**
     * Called when a table is modified.
     * @param tableName The modified table
     * @param operation The type of modification
     * @param affectedIds The IDs of affected entities (if known)
     */
    suspend fun onTableModified(
        tableName: String,
        operation: ModificationOperation,
        affectedIds: Set<Any>? = null
    )

    /**
     * Called to check if an entry should be invalidated.
     */
    suspend fun shouldInvalidate(key: Any, tableName: String): Boolean
}

/**
 * Type of table modification.
 */
enum class ModificationOperation {
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE
}

/**
 * Simple invalidation strategy that clears all related cache entries.
 */
class FullInvalidationStrategy(
    private val sessionCache: SessionCache,
    private val queryCache: QueryCache?
) : CacheInvalidationStrategy {
    private val logger = LoggerFactory.getLogger(FullInvalidationStrategy::class.java)

    override suspend fun onTableModified(
        tableName: String,
        operation: ModificationOperation,
        affectedIds: Set<Any>?
    ) {
        logger.debug("Full invalidation for table: {} ({})", tableName, operation)

        // Invalidate session cache entries for this table
        sessionCache.removeByType(tableName)

        // Invalidate all queries that touch this table
        queryCache?.invalidateTable(tableName)
    }

    override suspend fun shouldInvalidate(key: Any, tableName: String): Boolean = true
}

/**
 * Targeted invalidation strategy that only invalidates specific entries.
 */
class TargetedInvalidationStrategy(
    private val sessionCache: SessionCache,
    private val queryCache: QueryCache?
) : CacheInvalidationStrategy {
    private val logger = LoggerFactory.getLogger(TargetedInvalidationStrategy::class.java)

    override suspend fun onTableModified(
        tableName: String,
        operation: ModificationOperation,
        affectedIds: Set<Any>?
    ) {
        when (operation) {
            ModificationOperation.INSERT -> {
                // New entries don't affect existing cache, but invalidate queries
                queryCache?.invalidateTable(tableName)
            }
            ModificationOperation.UPDATE -> {
                // Only invalidate specific entities and queries
                if (affectedIds != null) {
                    affectedIds.forEach { id ->
                        sessionCache.remove(EntityKey.of(tableName, id))
                    }
                } else {
                    // Unknown IDs - invalidate all
                    sessionCache.removeByType(tableName)
                }
                queryCache?.invalidateTable(tableName)
            }
            ModificationOperation.DELETE -> {
                // Remove specific entities
                if (affectedIds != null) {
                    affectedIds.forEach { id ->
                        sessionCache.remove(EntityKey.of(tableName, id))
                    }
                } else {
                    sessionCache.removeByType(tableName)
                }
                queryCache?.invalidateTable(tableName)
            }
            ModificationOperation.TRUNCATE -> {
                // Remove all
                sessionCache.removeByType(tableName)
                queryCache?.invalidateTable(tableName)
            }
        }
    }

    override suspend fun shouldInvalidate(key: Any, tableName: String): Boolean {
        return key is EntityKey && key.entityType == tableName
    }
}

/**
 * Time-based invalidation with version tracking.
 */
class VersionedInvalidationStrategy(
    private val sessionCache: SessionCache,
    private val queryCache: QueryCache?
) : CacheInvalidationStrategy {
    private val logger = LoggerFactory.getLogger(VersionedInvalidationStrategy::class.java)

    /** Table modification versions */
    private val tableVersions = mutableMapOf<String, Long>()
    private val mutex = Mutex()

    override suspend fun onTableModified(
        tableName: String,
        operation: ModificationOperation,
        affectedIds: Set<Any>?
    ) {
        val newVersion = mutex.withLock {
            val version = (tableVersions[tableName] ?: 0) + 1
            tableVersions[tableName] = version
            version
        }

        logger.debug("Table {} version updated to {} ({})", tableName, newVersion, operation)

        // Invalidate query cache
        queryCache?.invalidateTable(tableName)

        // Session cache will use shouldInvalidate for lazy invalidation
    }

    override suspend fun shouldInvalidate(key: Any, tableName: String): Boolean {
        // Version check can be implemented with cached entry metadata
        return true
    }

    /**
     * Get the current version of a table.
     */
    suspend fun getTableVersion(tableName: String): Long {
        return mutex.withLock { tableVersions[tableName] ?: 0 }
    }
}

/**
 * Cache invalidation event.
 */
data class InvalidationEvent(
    val tableName: String,
    val operation: ModificationOperation,
    val affectedIds: Set<Any>?,
    val timestamp: Instant = Instant.now()
)

/**
 * Listener for invalidation events.
 */
fun interface InvalidationListener {
    suspend fun onInvalidation(event: InvalidationEvent)
}

/**
 * Cache invalidation manager.
 */
class CacheInvalidationManager(
    private val strategy: CacheInvalidationStrategy
) {
    private val logger = LoggerFactory.getLogger(CacheInvalidationManager::class.java)
    private val listeners = mutableListOf<InvalidationListener>()
    private val mutex = Mutex()

    /**
     * Notify of a table modification.
     */
    suspend fun notifyModification(
        tableName: String,
        operation: ModificationOperation,
        affectedIds: Set<Any>? = null
    ) {
        logger.debug("Notifying modification: {} {} {}", tableName, operation, affectedIds?.size ?: "all")

        strategy.onTableModified(tableName, operation, affectedIds)

        val event = InvalidationEvent(tableName, operation, affectedIds)
        mutex.withLock {
            listeners.forEach { it.onInvalidation(event) }
        }
    }

    /**
     * Add an invalidation listener.
     */
    fun addListener(listener: InvalidationListener) {
        listeners.add(listener)
    }

    /**
     * Remove an invalidation listener.
     */
    fun removeListener(listener: InvalidationListener) {
        listeners.remove(listener)
    }
}
