package com.physics91.korma.r2dbc

import com.physics91.korma.dsl.*
import com.physics91.korma.exception.QueryExecutionException
import com.physics91.korma.resilience.RetryPolicy
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect
import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory

/**
 * DSL-integrated R2DBC operations for type-safe database access.
 *
 * Usage:
 * ```kotlin
 * database.suspendTransaction {
 *     // Type-safe DSL operations
 *     val user = selectOne(Users) {
 *         select(Users.id, Users.name)
 *         where { Users.id eq 1L }
 *     }
 *
 *     insert(Users) {
 *         set(Users.name, "John")
 *         set(Users.email, "john@example.com")
 *     }
 *
 *     update(Users) {
 *         set(Users.name, "Jane")
 *         where { Users.id eq 1L }
 *     }
 *
 *     delete(Users) {
 *         where { Users.id eq 1L }
 *     }
 * }
 * ```
 */

// ============== Transaction DSL Extensions ==============

/**
 * Execute a SELECT query within a transaction using DSL.
 */
suspend fun <T> R2dbcTransaction.select(
    table: Table,
    mapper: (Row) -> T,
    block: SelectBuilder.() -> Unit
): List<T> {
    val builder = SelectBuilder(table).apply(block)
    val sql = builder.build(getDialect())
    return executor.queryList(sql.sql, sql.params, mapper)
}

/**
 * Execute a SELECT query and return a single result.
 */
suspend fun <T> R2dbcTransaction.selectOne(
    table: Table,
    mapper: (Row) -> T,
    block: SelectBuilder.() -> Unit
): T? {
    val builder = SelectBuilder(table).apply(block)
    val sql = builder.build(getDialect())
    return executor.queryOne(sql.sql, sql.params, mapper)
}

/**
 * Execute a SELECT query and return results as Flow.
 */
fun <T> R2dbcTransaction.selectFlow(
    table: Table,
    mapper: (Row) -> T,
    block: SelectBuilder.() -> Unit
): Flow<T> {
    val builder = SelectBuilder(table).apply(block)
    val sql = builder.build(getDialect())
    return executor.query(sql.sql, sql.params, mapper)
}

/**
 * Execute an INSERT within a transaction using DSL.
 */
suspend fun R2dbcTransaction.insert(
    table: Table,
    block: InsertBuilder.() -> Unit
): Long {
    val builder = InsertBuilder(table).apply(block)
    val sql = builder.build(getDialect())
    return executor.execute(sql.sql, sql.params)
}

/**
 * Execute an INSERT with RETURNING within a transaction.
 */
suspend fun <T> R2dbcTransaction.insertReturning(
    table: Table,
    returningColumn: Column<T>,
    block: InsertBuilder.() -> Unit
): T? {
    val builder = InsertBuilder(table).apply {
        block()
        returning(returningColumn)
    }
    val sql = builder.build(getDialect())
    @Suppress("UNCHECKED_CAST")
    return executor.executeReturning(sql.sql, sql.params) { row ->
        row.get(returningColumn.name, Any::class.java) as T
    }
}

/**
 * Execute an UPDATE within a transaction using DSL.
 */
suspend fun R2dbcTransaction.update(
    table: Table,
    block: UpdateBuilder.() -> Unit
): Long {
    val builder = UpdateBuilder(table).apply(block)
    val sql = builder.build(getDialect())
    return executor.execute(sql.sql, sql.params)
}

/**
 * Execute a DELETE within a transaction using DSL.
 */
suspend fun R2dbcTransaction.delete(
    table: Table,
    block: DeleteBuilder.() -> Unit
): Long {
    val builder = DeleteBuilder(table).apply(block)
    val sql = builder.build(getDialect())
    return executor.execute(sql.sql, sql.params)
}

/**
 * Execute a batch INSERT within a transaction.
 */
suspend fun <T> R2dbcTransaction.batchInsert(
    table: Table,
    items: List<T>,
    block: InsertBuilder.(T) -> Unit
): Long {
    if (items.isEmpty()) return 0L

    val batchBuilder = BatchInsertBuilder(table)
    batchBuilder.addRows(items) { block(it) }

    val sqls = batchBuilder.build(getDialect())
    var total = 0L
    sqls.forEach { sql ->
        total += executor.execute(sql.sql, sql.params)
    }
    return total
}

// ============== Database DSL Extensions (Auto-commit) ==============

/**
 * Execute a SELECT query using DSL (auto-commit mode).
 */
suspend fun <T> R2dbcDatabase.select(
    table: Table,
    mapper: (Row) -> T,
    block: SelectBuilder.() -> Unit
): List<T> {
    val builder = SelectBuilder(table).apply(block)
    return select(builder, mapper)
}

/**
 * Execute a SELECT query and return a single result (auto-commit mode).
 */
suspend fun <T> R2dbcDatabase.selectOne(
    table: Table,
    mapper: (Row) -> T,
    block: SelectBuilder.() -> Unit
): T? {
    val results = select(table, mapper, block)
    return results.firstOrNull()
}

/**
 * Execute an INSERT using DSL (auto-commit mode).
 */
suspend fun R2dbcDatabase.insert(
    table: Table,
    block: InsertBuilder.() -> Unit
): Long {
    val builder = InsertBuilder(table).apply(block)
    return insert(builder)
}

/**
 * Execute an UPDATE using DSL (auto-commit mode).
 */
suspend fun R2dbcDatabase.update(
    table: Table,
    block: UpdateBuilder.() -> Unit
): Long {
    val builder = UpdateBuilder(table).apply(block)
    return update(builder)
}

/**
 * Execute a DELETE using DSL (auto-commit mode).
 */
suspend fun R2dbcDatabase.delete(
    table: Table,
    block: DeleteBuilder.() -> Unit
): Long {
    val builder = DeleteBuilder(table).apply(block)
    return delete(builder)
}

// ============== Enhanced Batch Operations ==============

/**
 * Batch update executor with chunking support.
 */
class R2dbcBatchOperations(
    private val database: R2dbcDatabase,
    private val dialect: SqlDialect,
    private val chunkSize: Int = 1000
) {
    private val logger = LoggerFactory.getLogger(R2dbcBatchOperations::class.java)

    /**
     * Execute batch insert with chunking.
     */
    suspend fun <T> batchInsert(
        table: Table,
        items: List<T>,
        block: InsertBuilder.(T) -> Unit
    ): BatchResult {
        if (items.isEmpty()) return BatchResult.empty()

        var totalAffected = 0L
        var successCount = 0
        var failureCount = 0
        val errors = mutableListOf<BatchError>()

        items.chunked(chunkSize).forEachIndexed { chunkIndex, chunk ->
            try {
                val affected = database.batchInsert(table, chunk, block)
                totalAffected += affected
                successCount += chunk.size
                logger.debug("Batch insert chunk {} completed: {} rows", chunkIndex, affected)
            } catch (e: Exception) {
                failureCount += chunk.size
                errors.add(BatchError(chunkIndex, e.message ?: "Unknown error", e))
                logger.warn("Batch insert chunk {} failed: {}", chunkIndex, e.message)
            }
        }

        return BatchResult(totalAffected, successCount, failureCount, errors)
    }

    /**
     * Execute batch insert from a Flow with backpressure support.
     */
    fun <T> batchInsertFlow(
        table: Table,
        itemsFlow: Flow<T>,
        block: InsertBuilder.(T) -> Unit
    ): Flow<BatchChunkResult> = flow {
        val batch = mutableListOf<T>()
        var chunkIndex = 0

        itemsFlow.collect { item ->
            batch.add(item)
            if (batch.size >= chunkSize) {
                val result = processBatchChunk(table, batch.toList(), chunkIndex, block)
                emit(result)
                batch.clear()
                chunkIndex++
            }
        }

        if (batch.isNotEmpty()) {
            val result = processBatchChunk(table, batch.toList(), chunkIndex, block)
            emit(result)
        }
    }

    private suspend fun <T> processBatchChunk(
        table: Table,
        chunk: List<T>,
        chunkIndex: Int,
        block: InsertBuilder.(T) -> Unit
    ): BatchChunkResult {
        return try {
            val affected = database.batchInsert(table, chunk, block)
            BatchChunkResult(chunkIndex, chunk.size, affected, null)
        } catch (e: Exception) {
            BatchChunkResult(chunkIndex, chunk.size, 0L, e)
        }
    }

    /**
     * Execute batch update with chunking.
     */
    suspend fun <T> batchUpdate(
        items: List<T>,
        updateFn: suspend R2dbcDatabase.(T) -> Long
    ): BatchResult {
        if (items.isEmpty()) return BatchResult.empty()

        var totalAffected = 0L
        var successCount = 0
        var failureCount = 0
        val errors = mutableListOf<BatchError>()

        items.forEachIndexed { index, item ->
            try {
                val affected = database.updateFn(item)
                totalAffected += affected
                successCount++
            } catch (e: Exception) {
                failureCount++
                errors.add(BatchError(index, e.message ?: "Unknown error", e))
            }
        }

        return BatchResult(totalAffected, successCount, failureCount, errors)
    }

    /**
     * Execute batch delete with chunking.
     */
    suspend fun <T> batchDelete(
        items: List<T>,
        deleteFn: suspend R2dbcDatabase.(T) -> Long
    ): BatchResult {
        if (items.isEmpty()) return BatchResult.empty()

        var totalAffected = 0L
        var successCount = 0
        var failureCount = 0
        val errors = mutableListOf<BatchError>()

        items.forEachIndexed { index, item ->
            try {
                val affected = database.deleteFn(item)
                totalAffected += affected
                successCount++
            } catch (e: Exception) {
                failureCount++
                errors.add(BatchError(index, e.message ?: "Unknown error", e))
            }
        }

        return BatchResult(totalAffected, successCount, failureCount, errors)
    }
}

/**
 * Result of a batch operation.
 */
data class BatchResult(
    val totalAffected: Long,
    val successCount: Int,
    val failureCount: Int,
    val errors: List<BatchError>
) {
    val isFullySuccessful: Boolean get() = failureCount == 0
    val hasPartialSuccess: Boolean get() = successCount > 0 && failureCount > 0
    val isFullyFailed: Boolean get() = successCount == 0 && failureCount > 0

    companion object {
        fun empty(): BatchResult = BatchResult(0L, 0, 0, emptyList())
    }
}

/**
 * Result of a single batch chunk.
 */
data class BatchChunkResult(
    val chunkIndex: Int,
    val itemCount: Int,
    val affectedRows: Long,
    val error: Throwable?
) {
    val isSuccessful: Boolean get() = error == null
}

/**
 * Error information for a failed batch item.
 */
data class BatchError(
    val index: Int,
    val message: String,
    val cause: Throwable?
)

// ============== Retry-Aware Operations ==============

/**
 * Execute a query with retry support.
 */
suspend fun <T> R2dbcDatabase.selectWithRetry(
    builder: SelectBuilder,
    mapper: (Row) -> T,
    retryPolicy: RetryPolicy = RetryPolicy.exponentialBackoff()
): List<T> {
    return retryPolicy.executeSuspend {
        select(builder, mapper)
    }
}

/**
 * Execute an insert with retry support.
 */
suspend fun R2dbcDatabase.insertWithRetry(
    builder: InsertBuilder,
    retryPolicy: RetryPolicy = RetryPolicy.exponentialBackoff()
): Long {
    return retryPolicy.executeSuspend {
        insert(builder)
    }
}

/**
 * Execute an update with retry support.
 */
suspend fun R2dbcDatabase.updateWithRetry(
    builder: UpdateBuilder,
    retryPolicy: RetryPolicy = RetryPolicy.exponentialBackoff()
): Long {
    return retryPolicy.executeSuspend {
        update(builder)
    }
}

/**
 * Execute a delete with retry support.
 */
suspend fun R2dbcDatabase.deleteWithRetry(
    builder: DeleteBuilder,
    retryPolicy: RetryPolicy = RetryPolicy.exponentialBackoff()
): Long {
    return retryPolicy.executeSuspend {
        delete(builder)
    }
}

// ============== Internal Helpers ==============

/**
 * Get dialect from transaction context.
 */
private fun R2dbcTransaction.getDialect(): SqlDialect {
    return dialect ?: throw IllegalStateException(
        "Transaction was not created with a dialect. Use R2dbcDatabase.transaction() instead."
    )
}

/**
 * Extension to create batch operations helper.
 */
fun R2dbcDatabase.batchOperations(chunkSize: Int = 1000): R2dbcBatchOperations {
    return R2dbcBatchOperations(this, dialect, chunkSize)
}
