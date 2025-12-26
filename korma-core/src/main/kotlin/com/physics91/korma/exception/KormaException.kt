package com.physics91.korma.exception

/**
 * Base exception for all Korma-related exceptions.
 *
 * This sealed class hierarchy provides structured exception handling
 * with specific exception types for different error scenarios.
 *
 * Usage:
 * ```kotlin
 * try {
 *     database.execute(query)
 * } catch (e: KormaException) {
 *     when (e) {
 *         is QueryException -> handleQueryError(e)
 *         is TransactionException -> handleTransactionError(e)
 *         is ConnectionException -> handleConnectionError(e)
 *         is CacheException -> handleCacheError(e)
 *         is MigrationException -> handleMigrationError(e)
 *     }
 * }
 * ```
 */
sealed class KormaException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause) {

    /**
     * Unique error code for this exception type.
     */
    abstract val errorCode: String

    /**
     * Indicates whether this exception is recoverable.
     */
    abstract val isRecoverable: Boolean
}

// ============================================================================
// Query Exceptions
// ============================================================================

/**
 * Base exception for query-related errors.
 */
sealed class QueryException(
    message: String,
    cause: Throwable? = null,
    val sql: String? = null,
    val params: List<Any?>? = null
) : KormaException(message, cause) {

    override fun toString(): String = buildString {
        append(this@QueryException::class.simpleName)
        append(": ")
        append(message)
        sql?.let { append("\n  SQL: $it") }
        params?.let { append("\n  Params: $it") }
        cause?.let { append("\n  Cause: ${it.message}") }
    }
}

/**
 * Exception thrown when a query cannot be built correctly.
 *
 * This typically indicates a programming error in the DSL usage.
 */
class QueryBuildException(
    message: String,
    cause: Throwable? = null,
    sql: String? = null,
    params: List<Any?>? = null
) : QueryException(message, cause, sql, params) {
    override val errorCode: String = "KORMA-Q001"
    override val isRecoverable: Boolean = false
}

/**
 * Exception thrown when query execution fails.
 *
 * This may be caused by:
 * - SQL syntax errors
 * - Constraint violations
 * - Data type mismatches
 */
class QueryExecutionException(
    message: String,
    cause: Throwable? = null,
    sql: String? = null,
    params: List<Any?>? = null,
    val sqlState: String? = null,
    val vendorCode: Int? = null
) : QueryException(message, cause, sql, params) {
    override val errorCode: String = "KORMA-Q002"
    override val isRecoverable: Boolean = false

    override fun toString(): String = buildString {
        append(super.toString())
        sqlState?.let { append("\n  SQLState: $it") }
        vendorCode?.let { append("\n  VendorCode: $it") }
    }
}

/**
 * Exception thrown when a query times out.
 */
class QueryTimeoutException(
    message: String,
    cause: Throwable? = null,
    sql: String? = null,
    params: List<Any?>? = null,
    val timeoutMillis: Long? = null
) : QueryException(message, cause, sql, params) {
    override val errorCode: String = "KORMA-Q003"
    override val isRecoverable: Boolean = true

    override fun toString(): String = buildString {
        append(super.toString())
        timeoutMillis?.let { append("\n  Timeout: ${it}ms") }
    }
}

/**
 * Exception thrown when a required result is not found.
 */
class NoResultException(
    message: String,
    sql: String? = null,
    params: List<Any?>? = null
) : QueryException(message, null, sql, params) {
    override val errorCode: String = "KORMA-Q004"
    override val isRecoverable: Boolean = false
}

/**
 * Exception thrown when multiple results are found but only one was expected.
 */
class NonUniqueResultException(
    message: String,
    sql: String? = null,
    params: List<Any?>? = null,
    val actualCount: Int? = null
) : QueryException(message, null, sql, params) {
    override val errorCode: String = "KORMA-Q005"
    override val isRecoverable: Boolean = false
}

// ============================================================================
// Transaction Exceptions
// ============================================================================

/**
 * Base exception for transaction-related errors.
 */
sealed class TransactionException(
    message: String,
    cause: Throwable? = null
) : KormaException(message, cause)

/**
 * Exception thrown when a transaction rollback fails.
 */
class TransactionRollbackException(
    message: String,
    cause: Throwable? = null,
    val rollbackCause: Throwable? = null
) : TransactionException(message, cause) {
    override val errorCode: String = "KORMA-T001"
    override val isRecoverable: Boolean = false
}

/**
 * Exception thrown when a deadlock is detected.
 *
 * This exception is recoverable - retrying the transaction may succeed.
 */
class DeadlockException(
    message: String,
    cause: Throwable? = null,
    val involvedTables: List<String>? = null
) : TransactionException(message, cause) {
    override val errorCode: String = "KORMA-T002"
    override val isRecoverable: Boolean = true
}

/**
 * Exception thrown when an optimistic lock fails.
 *
 * This indicates that the data was modified by another transaction
 * between read and write operations.
 */
class OptimisticLockException(
    message: String,
    cause: Throwable? = null,
    val tableName: String? = null,
    val expectedVersion: Long? = null,
    val actualVersion: Long? = null
) : TransactionException(message, cause) {
    override val errorCode: String = "KORMA-T003"
    override val isRecoverable: Boolean = true
}

/**
 * Exception thrown when a transaction cannot be started.
 */
class TransactionStartException(
    message: String,
    cause: Throwable? = null
) : TransactionException(message, cause) {
    override val errorCode: String = "KORMA-T004"
    override val isRecoverable: Boolean = true
}

/**
 * Exception thrown when a transaction commit fails.
 */
class TransactionCommitException(
    message: String,
    cause: Throwable? = null
) : TransactionException(message, cause) {
    override val errorCode: String = "KORMA-T005"
    override val isRecoverable: Boolean = false
}

// ============================================================================
// Connection Exceptions
// ============================================================================

/**
 * Base exception for connection-related errors.
 */
sealed class ConnectionException(
    message: String,
    cause: Throwable? = null
) : KormaException(message, cause)

/**
 * Exception thrown when a connection cannot be acquired.
 */
class ConnectionAcquisitionException(
    message: String,
    cause: Throwable? = null,
    val waitTimeMillis: Long? = null
) : ConnectionException(message, cause) {
    override val errorCode: String = "KORMA-C001"
    override val isRecoverable: Boolean = true
}

/**
 * Exception thrown when the connection pool is exhausted.
 */
class ConnectionPoolExhaustedException(
    message: String,
    cause: Throwable? = null,
    val activeConnections: Int? = null,
    val maxPoolSize: Int? = null
) : ConnectionException(message, cause) {
    override val errorCode: String = "KORMA-C002"
    override val isRecoverable: Boolean = true
}

/**
 * Exception thrown when a connection is lost.
 */
class ConnectionLostException(
    message: String,
    cause: Throwable? = null
) : ConnectionException(message, cause) {
    override val errorCode: String = "KORMA-C003"
    override val isRecoverable: Boolean = true
}

/**
 * Exception thrown when connection validation fails.
 */
class ConnectionValidationException(
    message: String,
    cause: Throwable? = null
) : ConnectionException(message, cause) {
    override val errorCode: String = "KORMA-C004"
    override val isRecoverable: Boolean = true
}

// ============================================================================
// Cache Exceptions
// ============================================================================

/**
 * Base exception for cache-related errors.
 */
sealed class CacheException(
    message: String,
    cause: Throwable? = null
) : KormaException(message, cause)

/**
 * Exception thrown when cache initialization fails.
 */
class CacheInitializationException(
    message: String,
    cause: Throwable? = null
) : CacheException(message, cause) {
    override val errorCode: String = "KORMA-CA001"
    override val isRecoverable: Boolean = false
}

/**
 * Exception thrown when a cache operation fails.
 */
class CacheOperationException(
    message: String,
    cause: Throwable? = null,
    val operation: String? = null,
    val key: Any? = null
) : CacheException(message, cause) {
    override val errorCode: String = "KORMA-CA002"
    override val isRecoverable: Boolean = true
}

// ============================================================================
// Migration Exceptions
// ============================================================================

/**
 * Base exception for migration-related errors.
 */
sealed class MigrationException(
    message: String,
    cause: Throwable? = null
) : KormaException(message, cause)

/**
 * Exception thrown when a migration script fails.
 */
class MigrationScriptException(
    message: String,
    cause: Throwable? = null,
    val scriptName: String? = null,
    val version: String? = null
) : MigrationException(message, cause) {
    override val errorCode: String = "KORMA-M001"
    override val isRecoverable: Boolean = false
}

/**
 * Exception thrown when migration validation fails.
 */
class MigrationValidationException(
    message: String,
    cause: Throwable? = null,
    val expectedChecksum: String? = null,
    val actualChecksum: String? = null
) : MigrationException(message, cause) {
    override val errorCode: String = "KORMA-M002"
    override val isRecoverable: Boolean = false
}

/**
 * Exception thrown when migrations are out of order.
 */
class MigrationOrderException(
    message: String,
    cause: Throwable? = null
) : MigrationException(message, cause) {
    override val errorCode: String = "KORMA-M003"
    override val isRecoverable: Boolean = false
}

// ============================================================================
// Configuration Exceptions
// ============================================================================

/**
 * Exception thrown when configuration is invalid.
 */
class ConfigurationException(
    message: String,
    cause: Throwable? = null,
    val configKey: String? = null
) : KormaException(message, cause) {
    override val errorCode: String = "KORMA-CFG001"
    override val isRecoverable: Boolean = false
}

// ============================================================================
// Mapping Exceptions
// ============================================================================

/**
 * Exception thrown when entity mapping fails.
 */
class MappingException(
    message: String,
    cause: Throwable? = null,
    val entityClass: String? = null,
    val columnName: String? = null
) : KormaException(message, cause) {
    override val errorCode: String = "KORMA-MAP001"
    override val isRecoverable: Boolean = false
}
