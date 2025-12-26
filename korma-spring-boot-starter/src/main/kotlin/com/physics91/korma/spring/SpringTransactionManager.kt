package com.physics91.korma.spring

import com.physics91.korma.transaction.TransactionManager
import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.DefaultTransactionDefinition
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.sql.Connection
import javax.sql.DataSource

/**
 * Spring-integrated transaction manager for Korma.
 *
 * This transaction manager integrates with Spring's transaction infrastructure,
 * allowing Korma to participate in Spring-managed transactions.
 *
 * Features:
 * - Full integration with @Transactional annotation
 * - Transaction propagation support
 * - Connection binding via Spring's DataSourceUtils
 *
 * Example usage:
 * ```kotlin
 * @Service
 * class UserService(private val kormaTemplate: KormaTemplate) {
 *
 *     @Transactional
 *     fun createUser(name: String, email: String): Long {
 *         return kormaTemplate.insertReturning(Users) {
 *             it[Users.name] = name
 *             it[Users.email] = email
 *         }
 *     }
 * }
 * ```
 */
class SpringTransactionManager(
    private val dataSource: DataSource,
    private val platformTransactionManager: PlatformTransactionManager
) : TransactionManager {

    /**
     * Get the current connection bound to the transaction.
     * Uses Spring's DataSourceUtils for proper connection management.
     */
    override fun getConnection(): Connection {
        return DataSourceUtils.getConnection(dataSource)
    }

    /**
     * Release a connection back to the pool.
     * Uses Spring's DataSourceUtils for proper connection management.
     */
    override fun releaseConnection(connection: Connection) {
        DataSourceUtils.releaseConnection(connection, dataSource)
    }

    /**
     * Execute a block within a transaction.
     * Integrates with Spring's transaction management.
     */
    override fun <T> transaction(block: () -> T): T {
        val definition = DefaultTransactionDefinition().apply {
            propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRED
        }

        val status = platformTransactionManager.getTransaction(definition)
        return try {
            val result = block()
            platformTransactionManager.commit(status)
            result
        } catch (e: Exception) {
            platformTransactionManager.rollback(status)
            throw e
        }
    }

    /**
     * Execute a block within a new transaction (REQUIRES_NEW).
     */
    fun <T> requiresNewTransaction(block: () -> T): T {
        val definition = DefaultTransactionDefinition().apply {
            propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW
        }

        val status = platformTransactionManager.getTransaction(definition)
        return try {
            val result = block()
            platformTransactionManager.commit(status)
            result
        } catch (e: Exception) {
            platformTransactionManager.rollback(status)
            throw e
        }
    }

    /**
     * Execute a block within an existing transaction or throw if none exists.
     */
    fun <T> mandatory(block: () -> T): T {
        val definition = DefaultTransactionDefinition().apply {
            propagationBehavior = TransactionDefinition.PROPAGATION_MANDATORY
        }

        val status = platformTransactionManager.getTransaction(definition)
        return try {
            val result = block()
            platformTransactionManager.commit(status)
            result
        } catch (e: Exception) {
            platformTransactionManager.rollback(status)
            throw e
        }
    }

    /**
     * Check if a transaction is currently active.
     */
    override fun isTransactionActive(): Boolean {
        return TransactionSynchronizationManager.isActualTransactionActive()
    }

    /**
     * Get the current transaction status if available.
     */
    fun getCurrentTransactionStatus(): TransactionStatus? {
        return if (isTransactionActive()) {
            // This is a simplified check - in practice you'd track status
            null
        } else null
    }
}
