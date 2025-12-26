package com.physics91.korma.jdbc

import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect
import java.io.Closeable
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import org.slf4j.LoggerFactory

/**
 * A wrapper that manages both ResultSet and its underlying PreparedStatement lifecycle.
 * Closing this wrapper will properly close both resources.
 */
class ManagedResultSet(
    private val resultSet: ResultSet,
    private val statement: PreparedStatement
) : Closeable {

    /** Delegate to the underlying ResultSet */
    fun getResultSet(): ResultSet = resultSet

    /** Map results using the provided mapper */
    fun <T> map(mapper: (Row) -> T): List<T> = resultSet.map(mapper)

    /** Get first result or null */
    fun <T> firstOrNull(mapper: (Row) -> T): T? = resultSet.firstOrNull(mapper)

    /** Check if there are more rows */
    fun next(): Boolean = resultSet.next()

    /** Create a Row wrapper for current position */
    fun currentRow(): Row = Row(resultSet)

    override fun close() {
        try {
            resultSet.close()
        } finally {
            statement.close()
        }
    }
}

/**
 * Executes SQL queries against a JDBC connection.
 */
class JdbcExecutor(
    private val connection: Connection,
    private val dialect: SqlDialect
) {
    private val logger = LoggerFactory.getLogger(JdbcExecutor::class.java)

    /**
     * Execute a SELECT query and map results.
     */
    fun <T> query(sql: PreparedSql, mapper: (Row) -> T): List<T> {
        logger.debug("Executing query: {} with params: {}", sql.sql, sql.params)

        return connection.prepareStatement(sql.sql).use { stmt ->
            bindParameters(stmt, sql.params)
            stmt.executeQuery().use { rs ->
                rs.map(mapper)
            }
        }
    }

    /**
     * Execute a SELECT query and return a ManagedResultSet.
     * The ManagedResultSet manages both the ResultSet and PreparedStatement lifecycle.
     * Caller MUST close the returned ManagedResultSet to avoid resource leaks.
     *
     * Usage:
     * ```kotlin
     * executor.queryRaw(sql).use { managed ->
     *     managed.map { row -> ... }
     * }
     * ```
     */
    fun queryRaw(sql: PreparedSql): ManagedResultSet {
        logger.debug("Executing raw query: {} with params: {}", sql.sql, sql.params)

        val stmt = connection.prepareStatement(sql.sql)
        try {
            bindParameters(stmt, sql.params)
            val rs = stmt.executeQuery()
            return ManagedResultSet(rs, stmt)
        } catch (e: Exception) {
            stmt.close()
            throw e
        }
    }

    /**
     * Execute a SELECT query and get single result.
     */
    fun <T> querySingle(sql: PreparedSql, mapper: (Row) -> T): T? {
        logger.debug("Executing single query: {} with params: {}", sql.sql, sql.params)

        return connection.prepareStatement(sql.sql).use { stmt ->
            bindParameters(stmt, sql.params)
            stmt.executeQuery().use { rs ->
                rs.firstOrNull(mapper)
            }
        }
    }

    /**
     * Execute an INSERT/UPDATE/DELETE statement.
     * Returns the number of affected rows.
     */
    fun execute(sql: PreparedSql): Int {
        logger.debug("Executing: {} with params: {}", sql.sql, sql.params)

        return connection.prepareStatement(sql.sql).use { stmt ->
            bindParameters(stmt, sql.params)
            stmt.executeUpdate()
        }
    }

    /**
     * Execute an INSERT statement and return generated keys.
     */
    fun <T> executeWithGeneratedKeys(
        sql: PreparedSql,
        keyMapper: (Row) -> T
    ): T {
        logger.debug("Executing with generated keys: {} with params: {}", sql.sql, sql.params)

        return connection.prepareStatement(sql.sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
            bindParameters(stmt, sql.params)
            stmt.executeUpdate()
            stmt.generatedKeys.use { rs ->
                rs.first(keyMapper)
            }
        }
    }

    /**
     * Execute an INSERT statement and return all generated keys.
     */
    fun <T> executeWithAllGeneratedKeys(
        sql: PreparedSql,
        keyMapper: (Row) -> T
    ): List<T> {
        logger.debug("Executing with all generated keys: {} with params: {}", sql.sql, sql.params)

        return connection.prepareStatement(sql.sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
            bindParameters(stmt, sql.params)
            stmt.executeUpdate()
            stmt.generatedKeys.use { rs ->
                rs.map(keyMapper)
            }
        }
    }

    /**
     * Execute a batch of statements.
     */
    fun executeBatch(sqls: List<PreparedSql>): IntArray {
        if (sqls.isEmpty()) return intArrayOf()

        // Group by SQL (for parameterized batch)
        val grouped = sqls.groupBy { it.sql }

        val results = mutableListOf<Int>()

        for ((sql, batch) in grouped) {
            logger.debug("Executing batch: {} with {} items", sql, batch.size)

            connection.prepareStatement(sql).use { stmt ->
                for (item in batch) {
                    bindParameters(stmt, item.params)
                    stmt.addBatch()
                }
                results.addAll(stmt.executeBatch().toList())
            }
        }

        return results.toIntArray()
    }

    /**
     * Execute raw SQL (no parameters).
     */
    fun executeRaw(sql: String): Boolean {
        logger.debug("Executing raw SQL: {}", sql)

        return connection.createStatement().use { stmt ->
            stmt.execute(sql)
        }
    }

    /**
     * Execute DDL statements (CREATE, ALTER, DROP).
     */
    fun executeDdl(sql: String) {
        logger.debug("Executing DDL: {}", sql)

        connection.createStatement().use { stmt ->
            stmt.executeUpdate(sql)
        }
    }

    /**
     * Check if a table exists.
     */
    fun tableExists(tableName: String): Boolean {
        return connection.metaData.getTables(null, null, tableName, arrayOf("TABLE")).use { rs ->
            rs.next()
        }
    }

    /**
     * Bind parameters to a PreparedStatement.
     */
    private fun bindParameters(stmt: PreparedStatement, params: List<Any?>) {
        params.forEachIndexed { index, param ->
            val paramIndex = index + 1 // JDBC uses 1-based indexing

            when (param) {
                null -> stmt.setNull(paramIndex, java.sql.Types.NULL)
                is String -> stmt.setString(paramIndex, param)
                is Int -> stmt.setInt(paramIndex, param)
                is Long -> stmt.setLong(paramIndex, param)
                is Double -> stmt.setDouble(paramIndex, param)
                is Float -> stmt.setFloat(paramIndex, param)
                is Boolean -> stmt.setBoolean(paramIndex, param)
                is Short -> stmt.setShort(paramIndex, param)
                is Byte -> stmt.setByte(paramIndex, param)
                is ByteArray -> stmt.setBytes(paramIndex, param)
                is java.math.BigDecimal -> stmt.setBigDecimal(paramIndex, param)
                is java.sql.Date -> stmt.setDate(paramIndex, param)
                is java.sql.Time -> stmt.setTime(paramIndex, param)
                is java.sql.Timestamp -> stmt.setTimestamp(paramIndex, param)
                is java.time.Instant -> stmt.setTimestamp(paramIndex, java.sql.Timestamp.from(param))
                is java.time.LocalDate -> stmt.setDate(paramIndex, java.sql.Date.valueOf(param))
                is java.time.LocalTime -> stmt.setTime(paramIndex, java.sql.Time.valueOf(param))
                is java.time.LocalDateTime -> stmt.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(param))
                is java.util.UUID -> stmt.setObject(paramIndex, param)
                is Enum<*> -> stmt.setString(paramIndex, param.name)
                else -> stmt.setObject(paramIndex, param)
            }
        }
    }
}
