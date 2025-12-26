package com.physics91.korma.r2dbc

import com.physics91.korma.schema.Column
import com.physics91.korma.sql.PreparedSql
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

/**
 * Executes SQL queries using R2DBC with coroutine support.
 */
class R2dbcExecutor(private val connection: Connection) {

    /**
     * Execute a query and return results as a Flow.
     */
    fun <T> query(sql: String, params: List<Any?> = emptyList(), mapper: (Row) -> T): Flow<T> = flow {
        val statement = prepareStatement(sql, params)
        val resultFlux = Flux.from(statement.execute())

        resultFlux.asFlow().collect { result ->
            Flux.from(result.map { row, _ -> mapper(row) }).asFlow().collect { emit(it) }
        }
    }

    /**
     * Execute a query with PreparedSql.
     */
    fun <T> query(paramSql: PreparedSql, mapper: (Row) -> T): Flow<T> {
        return query(paramSql.sql, paramSql.params, mapper)
    }

    /**
     * Execute a query and return a single result.
     */
    suspend fun <T> queryOne(sql: String, params: List<Any?> = emptyList(), mapper: (Row) -> T): T? {
        val statement = prepareStatement(sql, params)
        val result = Flux.from(statement.execute()).awaitFirst()
        return Flux.from(result.map { row, _ -> mapper(row) }).awaitFirstOrNull()
    }

    /**
     * Execute a query and return a list of results.
     */
    suspend fun <T> queryList(sql: String, params: List<Any?> = emptyList(), mapper: (Row) -> T): List<T> {
        val list = mutableListOf<T>()
        query(sql, params, mapper).collect { list.add(it) }
        return list
    }

    /**
     * Execute an update/insert/delete and return affected row count.
     */
    suspend fun execute(sql: String, params: List<Any?> = emptyList()): Long {
        val statement = prepareStatement(sql, params)
        val result = Flux.from(statement.execute()).awaitFirst()
        return Flux.from(result.rowsUpdated).awaitFirst()
    }

    /**
     * Execute with PreparedSql.
     */
    suspend fun execute(paramSql: PreparedSql): Long {
        return execute(paramSql.sql, paramSql.params)
    }

    /**
     * Execute an insert and return generated keys.
     */
    suspend fun <T> executeReturning(
        sql: String,
        params: List<Any?> = emptyList(),
        keyMapper: (Row) -> T
    ): T? {
        val statement = prepareStatement(sql, params)
            .returnGeneratedValues()
        val result = Flux.from(statement.execute()).awaitFirst()
        return Flux.from(result.map { row, _ -> keyMapper(row) }).awaitFirstOrNull()
    }

    /**
     * Execute a batch of statements.
     */
    suspend fun executeBatch(sql: String, paramsList: List<List<Any?>>): Long {
        if (paramsList.isEmpty()) return 0L

        val statement = connection.createStatement(sql)
        paramsList.forEachIndexed { index, params ->
            params.forEachIndexed { paramIndex, value ->
                bindParam(statement, paramIndex, value)
            }
            if (index < paramsList.lastIndex) {
                statement.add()
            }
        }

        var totalUpdated = 0L
        Flux.from(statement.execute()).asFlow().collect { result ->
            totalUpdated += Flux.from(result.rowsUpdated).awaitFirst()
        }
        return totalUpdated
    }

    /**
     * Execute a batch with Flow of parameters (for large batches).
     */
    fun executeBatchFlow(sql: String, paramsFlow: Flow<List<Any?>>, batchSize: Int = 1000): Flow<Long> = flow {
        val batch = mutableListOf<List<Any?>>()

        paramsFlow.collect { params ->
            batch.add(params)
            if (batch.size >= batchSize) {
                emit(executeBatch(sql, batch))
                batch.clear()
            }
        }

        if (batch.isNotEmpty()) {
            emit(executeBatch(sql, batch))
        }
    }

    /**
     * Stream large result sets with backpressure support.
     */
    fun <T> queryStream(
        sql: String,
        params: List<Any?> = emptyList(),
        fetchSize: Int = 100,
        mapper: (Row) -> T
    ): Flow<T> = flow {
        val statement = prepareStatement(sql, params)
        statement.fetchSize(fetchSize)

        Flux.from(statement.execute()).asFlow().collect { result ->
            Flux.from(result.map { row, _ -> mapper(row) }).asFlow().collect { emit(it) }
        }
    }

    /**
     * Create a row mapper for a table's columns.
     */
    fun createRowMapper(columns: List<Column<*>>): (Row) -> Map<Column<*>, Any?> = { row ->
        columns.associateWith { column ->
            row.get(column.name, Any::class.java)
        }
    }

    private fun prepareStatement(sql: String, params: List<Any?>): Statement {
        val statement = connection.createStatement(sql)
        params.forEachIndexed { index, value ->
            bindParam(statement, index, value)
        }
        return statement
    }

    private fun bindParam(statement: Statement, index: Int, value: Any?) {
        when (value) {
            null -> statement.bindNull(index, Any::class.java)
            is String -> statement.bind(index, value)
            is Int -> statement.bind(index, value)
            is Long -> statement.bind(index, value)
            is Double -> statement.bind(index, value)
            is Float -> statement.bind(index, value)
            is Boolean -> statement.bind(index, value)
            is ByteArray -> statement.bind(index, value)
            is java.time.LocalDate -> statement.bind(index, value)
            is java.time.LocalTime -> statement.bind(index, value)
            is java.time.LocalDateTime -> statement.bind(index, value)
            is java.time.OffsetDateTime -> statement.bind(index, value)
            is java.time.Instant -> statement.bind(index, value)
            is java.util.UUID -> statement.bind(index, value)
            is java.math.BigDecimal -> statement.bind(index, value)
            else -> statement.bind(index, value)
        }
    }
}
