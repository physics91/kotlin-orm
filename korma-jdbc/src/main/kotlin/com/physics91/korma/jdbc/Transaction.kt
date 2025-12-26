package com.physics91.korma.jdbc

import com.physics91.korma.transaction.TransactionManager as TransactionManagerInterface
import java.sql.Connection
import java.sql.Savepoint

/**
 * Represents an active database transaction.
 *
 * Provides access to the underlying connection and supports savepoints.
 */
class Transaction(
    val connection: Connection,
    private val database: JdbcDatabase
) {
    private var completed = false
    private val savepoints = mutableListOf<Savepoint>()

    /**
     * Current transaction nesting level.
     */
    var nestingLevel: Int = 0

    /**
     * Create a savepoint within this transaction.
     */
    fun savepoint(name: String? = null): Savepoint {
        val savepoint = if (name != null) {
            connection.setSavepoint(name)
        } else {
            connection.setSavepoint()
        }
        savepoints.add(savepoint)
        return savepoint
    }

    /**
     * Rollback to a specific savepoint.
     */
    fun rollbackTo(savepoint: Savepoint) {
        connection.rollback(savepoint)
        // Remove this savepoint and all subsequent ones
        val index = savepoints.indexOf(savepoint)
        if (index >= 0) {
            savepoints.subList(index, savepoints.size).clear()
        }
    }

    /**
     * Release a savepoint.
     */
    fun releaseSavepoint(savepoint: Savepoint) {
        connection.releaseSavepoint(savepoint)
        savepoints.remove(savepoint)
    }

    /**
     * Commit this transaction.
     */
    fun commit() {
        if (!completed) {
            connection.commit()
            completed = true
        }
    }

    /**
     * Rollback this transaction.
     */
    fun rollback() {
        if (!completed) {
            connection.rollback()
            completed = true
        }
    }

    /**
     * Execute within a nested transaction (savepoint).
     */
    fun <T> nested(block: Transaction.() -> T): T {
        val savepoint = savepoint()
        return try {
            nestingLevel++
            val result = block()
            releaseSavepoint(savepoint)
            result
        } catch (e: Exception) {
            rollbackTo(savepoint)
            throw e
        } finally {
            nestingLevel--
        }
    }
}

/**
 * Transaction manager for handling database transactions.
 *
 * Implements the TransactionManager interface from korma-core.
 */
class TransactionManager(private val database: JdbcDatabase) : TransactionManagerInterface {

    private val threadLocalTransaction = ThreadLocal<Transaction?>()
    private val threadLocalConnection = ThreadLocal<Connection?>()

    /**
     * Get the current transaction for this thread, if any.
     */
    fun currentTransaction(): Transaction? = threadLocalTransaction.get()

    /**
     * Check if there's an active transaction.
     */
    fun hasActiveTransaction(): Boolean = currentTransaction() != null

    /**
     * Check if a transaction is currently active.
     */
    override fun isTransactionActive(): Boolean = hasActiveTransaction()

    /**
     * Get a connection for the current context.
     */
    override fun getConnection(): Connection {
        // If there's an active transaction, return its connection
        val transaction = currentTransaction()
        if (transaction != null) {
            return transaction.connection
        }

        // Check if there's a thread-bound connection
        val existing = threadLocalConnection.get()
        if (existing != null) {
            return existing
        }

        // Get a new connection
        val connection = database.dataSource.connection
        threadLocalConnection.set(connection)
        return connection
    }

    /**
     * Release a connection back to the pool.
     */
    override fun releaseConnection(connection: Connection) {
        // Don't release if bound to a transaction
        val transaction = currentTransaction()
        if (transaction != null && transaction.connection === connection) {
            return
        }

        // Remove from thread-local and close
        val threadBound = threadLocalConnection.get()
        if (threadBound === connection) {
            threadLocalConnection.remove()
        }
        connection.close()
    }

    /**
     * Execute a block within a transaction (interface method).
     */
    override fun <T> transaction(block: () -> T): T {
        return transactionWithContext { block() }
    }

    /**
     * Execute a block within a transaction with Transaction context.
     *
     * If there's already an active transaction, participates in it.
     * Otherwise, starts a new transaction.
     */
    fun <T> transactionWithContext(block: Transaction.() -> T): T {
        val existing = currentTransaction()

        return if (existing != null) {
            // Join existing transaction
            existing.block()
        } else {
            // Start new transaction
            newTransaction(block)
        }
    }

    /**
     * Always start a new transaction (REQUIRES_NEW semantics).
     */
    fun <T> newTransaction(block: Transaction.() -> T): T {
        val existing = currentTransaction()
        val connection = database.dataSource.connection.apply {
            autoCommit = false
        }

        val transaction = Transaction(connection, database)
        threadLocalTransaction.set(transaction)

        return try {
            val result = transaction.block()
            transaction.commit()
            result
        } catch (e: Exception) {
            transaction.rollback()
            throw e
        } finally {
            if (existing != null) {
                threadLocalTransaction.set(existing)
            } else {
                threadLocalTransaction.remove()
            }
            connection.close()
        }
    }

    /**
     * Execute a block with a specific isolation level.
     */
    fun <T> transaction(
        isolation: TransactionIsolation,
        block: Transaction.() -> T
    ): T {
        val existing = currentTransaction()
        if (existing != null) {
            return existing.block()
        }

        val connection = database.dataSource.connection.apply {
            autoCommit = false
            transactionIsolation = isolation.toJdbcValue()
        }

        val transaction = Transaction(connection, database)
        threadLocalTransaction.set(transaction)

        return try {
            val result = transaction.block()
            transaction.commit()
            result
        } catch (e: Exception) {
            transaction.rollback()
            throw e
        } finally {
            threadLocalTransaction.remove()
            connection.close()
        }
    }

    /**
     * Execute read-only operations.
     */
    fun <T> readOnly(block: Transaction.() -> T): T {
        val existing = currentTransaction()
        if (existing != null) {
            return existing.block()
        }

        val connection = database.dataSource.connection.apply {
            autoCommit = true
            isReadOnly = true
        }

        val transaction = Transaction(connection, database)
        threadLocalTransaction.set(transaction)

        return try {
            transaction.block()
        } finally {
            threadLocalTransaction.remove()
            connection.close()
        }
    }
}
