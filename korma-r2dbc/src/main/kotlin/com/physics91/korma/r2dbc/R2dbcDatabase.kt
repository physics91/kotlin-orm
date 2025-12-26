package com.physics91.korma.r2dbc

import com.physics91.korma.dsl.*
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect
import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory

/**
 * Main entry point for R2DBC database operations with coroutine support.
 */
class R2dbcDatabase(
    private val connectionFactory: R2dbcConnectionFactory,
    val dialect: SqlDialect
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(R2dbcDatabase::class.java)

    /**
     * Execute a block within a transaction.
     */
    suspend fun <T> transaction(
        isolation: TransactionIsolation = TransactionIsolation.READ_COMMITTED,
        block: suspend R2dbcTransaction.() -> T
    ): T {
        val connection = connectionFactory.getConnection()
        try {
            // Set isolation level if specified
            connection.setTransactionIsolationLevel(isolation.r2dbcLevel).awaitFirstOrNull()

            // Begin transaction
            connection.beginTransaction().awaitFirstOrNull()

            val tx = R2dbcTransaction(connection, isolation.r2dbcLevel, dialect)
            return try {
                val result = tx.block()
                tx.commit()
                result
            } catch (e: Exception) {
                tx.rollback()
                throw e
            }
        } finally {
            connection.close().awaitFirstOrNull()
        }
    }

    /**
     * Execute a block with a connection (auto-commit mode).
     */
    suspend fun <T> withConnection(block: suspend R2dbcExecutor.() -> T): T {
        val connection = connectionFactory.getConnection()
        try {
            val executor = R2dbcExecutor(connection)
            return executor.block()
        } finally {
            connection.close().awaitFirstOrNull()
        }
    }

    /**
     * Execute a block with raw connection access (internal use).
     */
    internal suspend fun <T> withRawConnection(
        block: suspend (connection: io.r2dbc.spi.Connection, executor: R2dbcExecutor) -> T
    ): T {
        val connection = connectionFactory.getConnection()
        try {
            val executor = R2dbcExecutor(connection)
            return block(connection, executor)
        } finally {
            connection.close().awaitFirstOrNull()
        }
    }

    // ============== Table Operations ==============

    /**
     * Create a table.
     */
    suspend fun createTable(table: Table, ifNotExists: Boolean = true) {
        val sql = dialect.createTableStatement(table, ifNotExists)
        withConnection {
            execute(sql)
        }
        logger.debug("Created table: ${table.tableName}")
    }

    /**
     * Drop a table.
     */
    suspend fun dropTable(table: Table, ifExists: Boolean = true) {
        val sql = dialect.dropTableStatement(table, ifExists)
        withConnection {
            execute(sql)
        }
        logger.debug("Dropped table: ${table.tableName}")
    }

    // ============== CRUD Operations ==============

    /**
     * Execute a SELECT query.
     */
    suspend fun <T> select(builder: SelectBuilder, mapper: (Row) -> T): List<T> {
        val sql = builder.build(dialect)
        return withConnection {
            queryList(sql.sql, sql.params, mapper)
        }
    }

    /**
     * Execute a SELECT query returning a Flow.
     */
    fun <T> selectFlow(builder: SelectBuilder, mapper: (Row) -> T): Flow<T> = flow {
        val sql = builder.build(dialect)
        withConnection {
            query(sql.sql, sql.params, mapper).collect { emit(it) }
        }
    }

    /**
     * Execute an INSERT.
     */
    suspend fun insert(builder: InsertBuilder): Long {
        val sql = builder.build(dialect)
        return withConnection {
            execute(sql.sql, sql.params)
        }
    }

    /**
     * Execute an INSERT with RETURNING.
     */
    suspend fun <T> insertReturning(builder: InsertBuilder, column: Column<T>): T? {
        builder.returning(column)
        val sql = builder.build(dialect)
        return withConnection {
            @Suppress("UNCHECKED_CAST")
            executeReturning(sql.sql, sql.params) { row ->
                row.get(column.name, Any::class.java) as T
            }
        }
    }

    /**
     * Execute an UPDATE.
     */
    suspend fun update(builder: UpdateBuilder): Long {
        val sql = builder.build(dialect)
        return withConnection {
            execute(sql.sql, sql.params)
        }
    }

    /**
     * Execute a DELETE.
     */
    suspend fun delete(builder: DeleteBuilder): Long {
        val sql = builder.build(dialect)
        return withConnection {
            execute(sql.sql, sql.params)
        }
    }

    // ============== Batch Operations ==============

    /**
     * Execute batch insert.
     */
    suspend fun <T> batchInsert(
        table: Table,
        items: List<T>,
        mapper: InsertBuilder.(T) -> Unit
    ): Long {
        if (items.isEmpty()) return 0L

        val batchBuilder = BatchInsertBuilder(table)
        batchBuilder.addRows(items) { mapper(it) }

        val sqls = batchBuilder.build(dialect)
        return withConnection {
            var total = 0L
            sqls.forEach { sql ->
                total += execute(sql.sql, sql.params)
            }
            total
        }
    }

    /**
     * Execute batch insert with Flow input.
     */
    fun <T> batchInsertFlow(
        table: Table,
        itemsFlow: Flow<T>,
        batchSize: Int = 1000,
        mapper: InsertBuilder.(T) -> Unit
    ): Flow<Long> = flow {
        val batch = mutableListOf<T>()

        itemsFlow.collect { item ->
            batch.add(item)
            if (batch.size >= batchSize) {
                emit(batchInsert(table, batch.toList(), mapper))
                batch.clear()
            }
        }

        if (batch.isNotEmpty()) {
            emit(batchInsert(table, batch.toList(), mapper))
        }
    }

    // ============== Raw SQL ==============

    /**
     * Execute raw SQL.
     */
    suspend fun execute(sql: String, vararg params: Any?): Long {
        return withConnection {
            execute(sql, params.toList())
        }
    }

    /**
     * Execute raw SQL query.
     */
    suspend fun <T> query(sql: String, vararg params: Any?, mapper: (Row) -> T): List<T> {
        return withConnection {
            queryList(sql, params.toList(), mapper)
        }
    }

    /**
     * Execute raw SQL query returning Flow.
     */
    fun <T> queryFlow(sql: String, vararg params: Any?, mapper: (Row) -> T): Flow<T> = flow {
        withConnection {
            query(sql, params.toList(), mapper).collect { emit(it) }
        }
    }

    /**
     * Get connection pool metrics.
     */
    fun getPoolMetrics(): R2dbcConnectionFactory.PoolMetrics? {
        return connectionFactory.getMetrics()
    }

    override fun close() {
        connectionFactory.close()
    }

    companion object {
        /**
         * Create a database from a URL.
         */
        fun create(
            url: String,
            dialect: SqlDialect,
            poolConfig: R2dbcConnectionFactory.PoolConfig = R2dbcConnectionFactory.PoolConfig()
        ): R2dbcDatabase {
            val factory = R2dbcConnectionFactory.create(url, poolConfig)
            return R2dbcDatabase(factory, dialect)
        }

        /**
         * Create a database with options builder.
         */
        fun create(
            options: ConnectionOptionsBuilder,
            dialect: SqlDialect,
            poolConfig: R2dbcConnectionFactory.PoolConfig = R2dbcConnectionFactory.PoolConfig()
        ): R2dbcDatabase {
            val factory = R2dbcConnectionFactory.create(options.build(), poolConfig)
            return R2dbcDatabase(factory, dialect)
        }
    }
}
