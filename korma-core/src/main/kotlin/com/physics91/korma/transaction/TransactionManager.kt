package com.physics91.korma.transaction

import java.sql.Connection

/**
 * Transaction manager interface for managing database transactions.
 *
 * This interface defines the contract for transaction management across
 * different implementations (JDBC, Spring, etc.).
 */
interface TransactionManager {

    /**
     * Get a connection for the current context.
     *
     * If a transaction is active, returns the connection bound to that transaction.
     * Otherwise, returns a new connection from the underlying data source.
     *
     * @return A database connection
     */
    fun getConnection(): Connection

    /**
     * Release a connection back to the pool.
     *
     * If the connection is bound to an active transaction, it will not be closed
     * until the transaction completes.
     *
     * @param connection The connection to release
     */
    fun releaseConnection(connection: Connection)

    /**
     * Execute a block within a transaction.
     *
     * If a transaction is already active, the block participates in the existing transaction.
     * Otherwise, a new transaction is started.
     *
     * @param block The code to execute within the transaction
     * @return The result of the block
     * @throws Exception if the block throws or transaction management fails
     */
    fun <T> transaction(block: () -> T): T

    /**
     * Check if a transaction is currently active in the current context.
     *
     * @return true if a transaction is active, false otherwise
     */
    fun isTransactionActive(): Boolean
}
