package com.physics91.korma.ktor

import com.physics91.korma.jdbc.DatabaseContext
import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.jdbc.TransactionIsolation
import io.ktor.server.application.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Execute a block within a database transaction using coroutines.
 *
 * This function wraps the database transaction in a coroutine context,
 * making it safe to use within Ktor's suspend handlers.
 *
 * Example:
 * ```kotlin
 * get("/users/{id}") {
 *     val user = call.dbTransaction {
 *         val id = call.parameters["id"]!!.toLong()
 *         from(Users).select().where { Users.id eq id }.firstOrNull()
 *     }
 *     if (user != null) {
 *         call.respond(user)
 *     } else {
 *         call.respond(HttpStatusCode.NotFound)
 *     }
 * }
 * ```
 *
 * @param block The block to execute within the transaction
 * @return The result of the block
 */
suspend fun <T> ApplicationCall.dbTransaction(
    block: DatabaseContext.() -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.transaction(block)
}

/**
 * Execute a block within a database transaction with a specific isolation level.
 *
 * @param isolation The transaction isolation level
 * @param block The block to execute within the transaction
 * @return The result of the block
 */
suspend fun <T> ApplicationCall.dbTransaction(
    isolation: TransactionIsolation,
    block: DatabaseContext.() -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.transaction(isolation, block)
}

/**
 * Execute a read-only database operation using coroutines.
 *
 * This is optimized for SELECT queries and doesn't require transaction overhead.
 *
 * Example:
 * ```kotlin
 * get("/users") {
 *     val users = call.dbReadOnly {
 *         from(Users).select().fetch()
 *     }
 *     call.respond(users)
 * }
 * ```
 *
 * @param block The block to execute
 * @return The result of the block
 */
suspend fun <T> ApplicationCall.dbReadOnly(
    block: DatabaseContext.() -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.readOnly(block)
}

/**
 * Execute a new database transaction (REQUIRES_NEW semantics).
 *
 * This always starts a new transaction, suspending any existing transaction.
 *
 * @param block The block to execute within the new transaction
 * @return The result of the block
 */
suspend fun <T> ApplicationCall.dbNewTransaction(
    block: DatabaseContext.() -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.newTransaction(block)
}

/**
 * Application-level transaction helper.
 *
 * @param block The block to execute within the transaction
 * @return The result of the block
 */
suspend fun <T> Application.dbTransaction(
    block: DatabaseContext.() -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.transaction(block)
}

/**
 * Application-level read-only operation helper.
 *
 * @param block The block to execute
 * @return The result of the block
 */
suspend fun <T> Application.dbReadOnly(
    block: DatabaseContext.() -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.readOnly(block)
}

/**
 * Execute a direct database operation without transaction.
 *
 * Use this for simple queries that don't need transaction semantics.
 *
 * @param block The block to execute with direct connection
 * @return The result of the block
 */
suspend fun <T> ApplicationCall.useConnection(
    block: (java.sql.Connection) -> T
): T = withContext(Dispatchers.IO) {
    kormaDatabase.useConnection(block)
}
