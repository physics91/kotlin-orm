package com.physics91.korma.r2dbc

import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Coroutine context element for transaction propagation.
 */
class TransactionContext(
    val transaction: R2dbcTransaction
) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<TransactionContext>
}

/**
 * Marker context element indicating transaction is suspended.
 */
class SuspendedTransactionContext(
    val suspendedTransaction: R2dbcTransaction
) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<SuspendedTransactionContext>
}

/**
 * Get the current transaction from coroutine context.
 */
val CoroutineContext.currentTransaction: R2dbcTransaction?
    get() = this[TransactionContext]?.transaction

/**
 * Check if there's an active transaction in the current context.
 */
val CoroutineContext.hasTransaction: Boolean
    get() = currentTransaction?.isActive() == true

/**
 * Check if there's a suspended transaction in the current context.
 */
val CoroutineContext.hasSuspendedTransaction: Boolean
    get() = this[SuspendedTransactionContext]?.suspendedTransaction?.isActive() == true

/**
 * Execute within the current transaction context.
 *
 * Supports all standard transaction propagation modes:
 * - REQUIRED: Join existing transaction or create new one
 * - REQUIRES_NEW: Always create a new transaction
 * - SUPPORTS: Use existing transaction or run without one
 * - MANDATORY: Require existing transaction
 * - NOT_SUPPORTED: Suspend existing transaction and run without one
 * - NEVER: Fail if transaction exists, run without one otherwise
 */
suspend fun <T> R2dbcDatabase.suspendTransaction(
    isolation: TransactionIsolation = TransactionIsolation.READ_COMMITTED,
    propagation: TransactionPropagation = TransactionPropagation.REQUIRED,
    block: suspend R2dbcTransaction.() -> T
): T {
    val currentTx = kotlin.coroutines.coroutineContext.currentTransaction

    return when (propagation) {
        TransactionPropagation.REQUIRED -> {
            if (currentTx?.isActive() == true) {
                // Join existing transaction
                currentTx.block()
            } else {
                // Create new transaction
                transaction(isolation) {
                    withContext(TransactionContext(this)) {
                        block()
                    }
                }
            }
        }

        TransactionPropagation.REQUIRES_NEW -> {
            // Always create a new transaction, suspending the existing one if present
            if (currentTx?.isActive() == true) {
                // Suspend current transaction context (but don't actually suspend the db tx)
                val suspendedContext = SuspendedTransactionContext(currentTx)
                // Remove current transaction from context and add suspended marker
                withContext(suspendedContext) {
                    transaction(isolation) {
                        withContext(TransactionContext(this)) {
                            block()
                        }
                    }
                }
            } else {
                transaction(isolation) {
                    withContext(TransactionContext(this)) {
                        block()
                    }
                }
            }
        }

        TransactionPropagation.SUPPORTS -> {
            if (currentTx?.isActive() == true) {
                // Use existing transaction
                currentTx.block()
            } else {
                // Run without transaction (auto-commit mode)
                executeNonTransactional(block)
            }
        }

        TransactionPropagation.MANDATORY -> {
            currentTx?.takeIf { it.isActive() }?.block()
                ?: throw IllegalStateException("No active transaction found for MANDATORY propagation")
        }

        TransactionPropagation.NOT_SUPPORTED -> {
            if (currentTx?.isActive() == true) {
                // Suspend the current transaction and run without transaction
                val suspendedContext = SuspendedTransactionContext(currentTx)
                withContext(suspendedContext) {
                    executeNonTransactional(block)
                }
            } else {
                // No transaction to suspend, just run without transaction
                executeNonTransactional(block)
            }
        }

        TransactionPropagation.NEVER -> {
            if (currentTx?.isActive() == true) {
                throw IllegalStateException("Existing transaction found for NEVER propagation")
            }
            // Run without transaction
            executeNonTransactional(block)
        }
    }
}

/**
 * Execute a block without a transaction.
 */
private suspend fun <T> R2dbcDatabase.executeNonTransactional(
    block: suspend R2dbcTransaction.() -> T
): T {
    return withRawConnection { connection, _ ->
        // Create a non-transactional wrapper using the connection
        val wrapper = NonTransactionalTransaction(connection, dialect)
        wrapper.block()
    }
}

/**
 * Non-transactional R2dbcTransaction wrapper.
 * This is internal and used only for non-transactional propagation modes.
 */
private class NonTransactionalTransaction(
    connection: io.r2dbc.spi.Connection,
    dialect: com.physics91.korma.sql.SqlDialect
) : R2dbcTransaction(connection, null, dialect) {
    // Override behavior for non-transactional mode

    override fun isActive(): Boolean = false
    override fun isTransactional(): Boolean = false

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
 * Nested transaction support using savepoints.
 */
suspend fun <T> R2dbcTransaction.nested(
    savepointName: String,
    block: suspend R2dbcTransaction.() -> T
): T {
    val sp = savepoint(savepointName)
    return try {
        val result = block()
        releaseSavepoint(sp)
        result
    } catch (e: Exception) {
        rollbackToSavepoint(sp)
        throw e
    }
}

/**
 * Retry a transaction on failure.
 */
suspend fun <T> R2dbcDatabase.transactionWithRetry(
    maxRetries: Int = 3,
    retryOn: Set<Class<out Throwable>> = setOf(Exception::class.java),
    isolation: TransactionIsolation = TransactionIsolation.READ_COMMITTED,
    block: suspend R2dbcTransaction.() -> T
): T {
    var lastException: Throwable? = null

    repeat(maxRetries) { attempt ->
        try {
            return transaction(isolation, block)
        } catch (e: Throwable) {
            if (retryOn.any { it.isInstance(e) }) {
                lastException = e
                // Could add exponential backoff here
            } else {
                throw e
            }
        }
    }

    throw lastException ?: IllegalStateException("Transaction failed after $maxRetries retries")
}
