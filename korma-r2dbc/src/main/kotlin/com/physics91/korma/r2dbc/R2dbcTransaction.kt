package com.physics91.korma.r2dbc

import com.physics91.korma.sql.SqlDialect
import io.r2dbc.spi.Connection
import io.r2dbc.spi.IsolationLevel
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory

/**
 * Interface for transaction-like operations.
 * This allows both real transactions and non-transactional wrappers to share the same API.
 */
interface TransactionScope {
    /**
     * The executor for database operations.
     */
    val executor: R2dbcExecutor

    /**
     * The SQL dialect for this scope.
     */
    val dialect: SqlDialect?

    /**
     * Check if this is a real transaction.
     */
    fun isTransactional(): Boolean

    /**
     * Check if the transaction is still active.
     */
    fun isActive(): Boolean

    /**
     * Create a savepoint.
     */
    suspend fun savepoint(name: String): String

    /**
     * Rollback to a savepoint.
     */
    suspend fun rollbackToSavepoint(name: String)

    /**
     * Release a savepoint.
     */
    suspend fun releaseSavepoint(name: String)
}

/**
 * R2DBC transaction wrapper with coroutine support.
 */
open class R2dbcTransaction internal constructor(
    internal val connection: Connection,
    private val isolationLevel: IsolationLevel? = null,
    override val dialect: SqlDialect? = null
) : TransactionScope {
    private val logger = LoggerFactory.getLogger(R2dbcTransaction::class.java)
    private var active = true
    private val savepointNames = mutableListOf<String>()

    /**
     * Get the executor for this transaction.
     */
    override val executor: R2dbcExecutor by lazy { R2dbcExecutor(connection) }

    override open fun isTransactional(): Boolean = true

    /**
     * Check if the transaction is still active.
     */
    override open fun isActive(): Boolean = active

    /**
     * Create a savepoint.
     */
    override open suspend fun savepoint(name: String): String {
        checkActive()
        connection.createSavepoint(name).awaitFirstOrNull()
        savepointNames.add(name)
        logger.debug("Created savepoint: $name")
        return name
    }

    /**
     * Rollback to a savepoint.
     */
    override open suspend fun rollbackToSavepoint(name: String) {
        checkActive()
        connection.rollbackTransactionToSavepoint(name).awaitFirstOrNull()
        logger.debug("Rolled back to savepoint: $name")
    }

    /**
     * Release a savepoint.
     */
    override open suspend fun releaseSavepoint(name: String) {
        checkActive()
        connection.releaseSavepoint(name).awaitFirstOrNull()
        savepointNames.remove(name)
        logger.debug("Released savepoint: $name")
    }

    /**
     * Commit the transaction.
     */
    internal suspend fun commit() {
        if (!active) return
        connection.commitTransaction().awaitFirstOrNull()
        active = false
        logger.debug("Transaction committed")
    }

    /**
     * Rollback the transaction.
     */
    internal suspend fun rollback() {
        if (!active) return
        connection.rollbackTransaction().awaitFirstOrNull()
        active = false
        logger.debug("Transaction rolled back")
    }

    /**
     * Close the connection.
     */
    internal suspend fun close() {
        connection.close().awaitFirstOrNull()
    }

    private fun checkActive() {
        if (!active) {
            throw IllegalStateException("Transaction is no longer active")
        }
    }
}

/**
 * Non-transactional wrapper for operations that run without a transaction.
 * Used for SUPPORTS, NOT_SUPPORTED, and NEVER propagation modes.
 */
internal class NonTransactionalScope(
    override val executor: R2dbcExecutor,
    override val dialect: SqlDialect?
) : TransactionScope {

    override fun isTransactional(): Boolean = false

    override fun isActive(): Boolean = false

    override suspend fun savepoint(name: String): String {
        throw UnsupportedOperationException("Savepoints not supported in non-transactional mode")
    }

    override suspend fun rollbackToSavepoint(name: String) {
        throw UnsupportedOperationException("Savepoints not supported in non-transactional mode")
    }

    override suspend fun releaseSavepoint(name: String) {
        throw UnsupportedOperationException("Savepoints not supported in non-transactional mode")
    }
}

/**
 * Transaction isolation levels.
 */
enum class TransactionIsolation(val r2dbcLevel: IsolationLevel) {
    READ_UNCOMMITTED(IsolationLevel.READ_UNCOMMITTED),
    READ_COMMITTED(IsolationLevel.READ_COMMITTED),
    REPEATABLE_READ(IsolationLevel.REPEATABLE_READ),
    SERIALIZABLE(IsolationLevel.SERIALIZABLE)
}

/**
 * Transaction propagation behavior.
 */
enum class TransactionPropagation {
    /** Use existing transaction or create new one */
    REQUIRED,
    /** Always create a new transaction */
    REQUIRES_NEW,
    /** Use existing transaction or run without one */
    SUPPORTS,
    /** Run without transaction, suspend existing one */
    NOT_SUPPORTED,
    /** Require existing transaction */
    MANDATORY,
    /** Never use transaction, throw if one exists */
    NEVER
}
