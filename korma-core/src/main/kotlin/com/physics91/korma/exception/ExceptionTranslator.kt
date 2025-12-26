package com.physics91.korma.exception

import java.sql.SQLException
import java.sql.SQLTimeoutException

/**
 * Translates database-specific exceptions into Korma exceptions.
 *
 * This translator analyzes SQL state codes and vendor-specific error codes
 * to provide meaningful, structured exceptions.
 *
 * Usage:
 * ```kotlin
 * try {
 *     connection.executeQuery(sql)
 * } catch (e: SQLException) {
 *     throw ExceptionTranslator.translate(e, sql, params)
 * }
 * ```
 */
object ExceptionTranslator {

    /**
     * Translates a SQLException into a KormaException.
     */
    fun translate(
        exception: SQLException,
        sql: String? = null,
        params: List<Any?>? = null
    ): KormaException {
        val sqlState = exception.sqlState
        val errorCode = exception.errorCode
        val message = exception.message ?: "Unknown SQL error"

        // Check for timeout first
        if (exception is SQLTimeoutException) {
            return QueryTimeoutException(
                message = message,
                cause = exception,
                sql = sql,
                params = params
            )
        }

        // Analyze SQLState for standard error classification
        return when {
            // Connection errors (08xxx)
            sqlState?.startsWith("08") == true -> translateConnectionError(exception, sqlState)

            // Integrity constraint violations (23xxx)
            sqlState?.startsWith("23") == true -> translateConstraintError(exception, sql, params)

            // Transaction errors (40xxx)
            sqlState?.startsWith("40") == true -> translateTransactionError(exception, sqlState)

            // Syntax/access errors (42xxx)
            sqlState?.startsWith("42") == true -> QueryBuildException(
                message = "SQL syntax or access error: $message",
                cause = exception,
                sql = sql,
                params = params
            )

            // Deadlock detection based on vendor codes
            isDeadlock(sqlState, errorCode) -> DeadlockException(
                message = "Deadlock detected: $message",
                cause = exception
            )

            // Lock timeout (vendor-specific)
            isLockTimeout(sqlState, errorCode) -> QueryTimeoutException(
                message = "Lock wait timeout: $message",
                cause = exception,
                sql = sql,
                params = params
            )

            // Default: generic query execution exception
            else -> QueryExecutionException(
                message = message,
                cause = exception,
                sql = sql,
                params = params,
                sqlState = sqlState,
                vendorCode = errorCode
            )
        }
    }

    /**
     * Translates a generic exception that occurred during database operations.
     */
    fun translate(
        exception: Throwable,
        sql: String? = null,
        params: List<Any?>? = null,
        context: String = "database operation"
    ): KormaException {
        return when (exception) {
            is KormaException -> exception
            is SQLException -> translate(exception, sql, params)
            else -> QueryExecutionException(
                message = "Error during $context: ${exception.message}",
                cause = exception,
                sql = sql,
                params = params
            )
        }
    }

    private fun translateConnectionError(exception: SQLException, sqlState: String): KormaException {
        val message = exception.message ?: "Connection error"

        return when (sqlState) {
            "08001" -> ConnectionAcquisitionException(
                message = "Unable to establish connection: $message",
                cause = exception
            )
            "08003" -> ConnectionLostException(
                message = "Connection does not exist: $message",
                cause = exception
            )
            "08004" -> ConnectionAcquisitionException(
                message = "Connection rejected: $message",
                cause = exception
            )
            "08006" -> ConnectionLostException(
                message = "Connection failure: $message",
                cause = exception
            )
            "08007" -> TransactionStartException(
                message = "Transaction resolution unknown: $message",
                cause = exception
            )
            else -> ConnectionLostException(
                message = "Connection error: $message",
                cause = exception
            )
        }
    }

    private fun translateConstraintError(
        exception: SQLException,
        sql: String?,
        params: List<Any?>?
    ): QueryExecutionException {
        val message = exception.message ?: "Constraint violation"
        val sqlState = exception.sqlState

        val constraintMessage = when (sqlState) {
            "23000" -> "Integrity constraint violation"
            "23001" -> "Restrict violation"
            "23502" -> "Not null violation"
            "23503" -> "Foreign key violation"
            "23505" -> "Unique constraint violation"
            "23514" -> "Check constraint violation"
            else -> "Constraint violation"
        }

        return QueryExecutionException(
            message = "$constraintMessage: $message",
            cause = exception,
            sql = sql,
            params = params,
            sqlState = sqlState,
            vendorCode = exception.errorCode
        )
    }

    private fun translateTransactionError(exception: SQLException, sqlState: String): TransactionException {
        val message = exception.message ?: "Transaction error"

        return when (sqlState) {
            "40001" -> DeadlockException(
                message = "Serialization failure (deadlock): $message",
                cause = exception
            )
            "40P01" -> DeadlockException(
                message = "PostgreSQL deadlock detected: $message",
                cause = exception
            )
            "40002" -> TransactionRollbackException(
                message = "Integrity constraint violation during transaction: $message",
                cause = exception
            )
            "40003" -> TransactionRollbackException(
                message = "Statement completion unknown: $message",
                cause = exception
            )
            else -> TransactionRollbackException(
                message = "Transaction rollback: $message",
                cause = exception
            )
        }
    }

    private fun isDeadlock(sqlState: String?, errorCode: Int): Boolean {
        // PostgreSQL deadlock
        if (sqlState == "40P01") return true

        // MySQL deadlock (error code 1213)
        if (errorCode == 1213) return true

        // SQL Server deadlock (error code 1205)
        if (errorCode == 1205) return true

        // Oracle deadlock (error code 60)
        if (errorCode == 60) return true

        return false
    }

    private fun isLockTimeout(sqlState: String?, errorCode: Int): Boolean {
        // PostgreSQL lock timeout
        if (sqlState == "55P03") return true

        // MySQL lock wait timeout (error code 1205)
        if (errorCode == 1205) return true

        // SQL Server lock timeout (error code 1222)
        if (errorCode == 1222) return true

        return false
    }
}

/**
 * Extension function to wrap database operations with exception translation.
 */
inline fun <T> translateExceptions(
    sql: String? = null,
    params: List<Any?>? = null,
    context: String = "database operation",
    block: () -> T
): T {
    return try {
        block()
    } catch (e: KormaException) {
        throw e
    } catch (e: SQLException) {
        throw ExceptionTranslator.translate(e, sql, params)
    } catch (e: Exception) {
        throw ExceptionTranslator.translate(e, sql, params, context)
    }
}
