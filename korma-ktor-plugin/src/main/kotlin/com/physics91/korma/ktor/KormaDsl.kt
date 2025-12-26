package com.physics91.korma.ktor

import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.SqlDialect
import com.physics91.korma.sql.SqlLogger
import io.ktor.server.application.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/**
 * Korma DSL helper for Ktor applications.
 *
 * Provides convenient methods for database operations within Ktor handlers.
 *
 * Example:
 * ```kotlin
 * get("/users") {
 *     val users = call.korma.selectAll(Users) { row ->
 *         User(row[Users.id], row[Users.name])
 *     }
 *     call.respond(users)
 * }
 *
 * post("/users") {
 *     val user = call.receive<CreateUserRequest>()
 *     val id = call.korma.insert(Users) {
 *         it[Users.name] = user.name
 *         it[Users.email] = user.email
 *     }
 *     call.respond(mapOf("id" to id))
 * }
 * ```
 */
class KormaDsl internal constructor(
    private val database: JdbcDatabase,
    private val config: KormaConfig
) {
    val dialect: SqlDialect get() = database.dialect

    private val sqlLogger = SqlLogger(
        showSql = config.showSql,
        formatSql = config.formatSql,
        logger = LoggerFactory.getLogger(KormaDsl::class.java)
    )

    /**
     * Execute a raw SQL query and map results.
     */
    suspend fun <T> query(sql: String, vararg params: Any?, mapper: (Map<String, Any?>) -> T): List<T> =
        withContext(Dispatchers.IO) {
            sqlLogger.log(sql, params.toList())
            database.executeQuery(sql, params.toList(), mapper)
        }

    /**
     * Execute a raw SQL update/insert/delete.
     */
    suspend fun execute(sql: String, vararg params: Any?): Int =
        withContext(Dispatchers.IO) {
            sqlLogger.log(sql, params.toList())
            database.executeUpdate(sql, params.toList())
        }

    /**
     * Select all rows from a table.
     */
    suspend fun <T> selectAll(table: Table, mapper: (ResultRow) -> T): List<T> =
        withContext(Dispatchers.IO) {
            val sql = "SELECT * FROM ${dialect.quoteIdentifier(table.tableName)}"
            sqlLogger.log(sql, emptyList())
            database.executeQuery(sql, emptyList()) { row ->
                mapper(ResultRow(row, table))
            }
        }

    /**
     * Select rows with a WHERE clause.
     *
     * **Security Warning**: The [whereClause] is interpolated directly into SQL.
     * Always use parameterized placeholders (?) and pass values via [params].
     * Never concatenate user input directly into [whereClause].
     *
     * @param table the table to select from
     * @param whereClause the WHERE clause with ? placeholders (e.g., "id = ? AND status = ?")
     * @param params values to bind to the ? placeholders
     * @param mapper function to map each row to result type
     */
    suspend fun <T> selectWhere(
        table: Table,
        whereClause: String,
        vararg params: Any?,
        mapper: (ResultRow) -> T
    ): List<T> = withContext(Dispatchers.IO) {
        val sql = "SELECT * FROM ${dialect.quoteIdentifier(table.tableName)} WHERE $whereClause"
        sqlLogger.log(sql, params.toList())
        database.executeQuery(sql, params.toList()) { row ->
            mapper(ResultRow(row, table))
        }
    }

    /**
     * Find a single row by primary key.
     */
    suspend fun <T, ID> findById(
        table: Table,
        idColumn: Column<ID>,
        id: ID,
        mapper: (ResultRow) -> T
    ): T? = withContext(Dispatchers.IO) {
        val sql = "SELECT * FROM ${dialect.quoteIdentifier(table.tableName)} " +
                "WHERE ${dialect.quoteIdentifier(idColumn.name)} = ?"
        sqlLogger.log(sql, listOf(id))
        val results = database.executeQuery(sql, listOf(id)) { row ->
            mapper(ResultRow(row, table))
        }
        results.firstOrNull()
    }

    /**
     * Insert a row and return generated keys.
     */
    suspend fun insert(table: Table, block: InsertBuilder.(InsertBuilder) -> Unit): Long =
        withContext(Dispatchers.IO) {
            val builder = InsertBuilder(table, dialect)
            builder.block(builder)
            val (sql, params) = builder.build()
            sqlLogger.log(sql, params)
            database.executeInsert(sql, params)
        }

    /**
     * Update rows matching a condition.
     *
     * **Security Warning**: The [whereClause] is interpolated directly into SQL.
     * Always use parameterized placeholders (?) and pass values via [whereParams].
     * Never concatenate user input directly into [whereClause].
     *
     * @param table the table to update
     * @param whereClause the WHERE clause with ? placeholders
     * @param whereParams values to bind to the ? placeholders in WHERE clause
     * @param block builder for SET clause values
     */
    suspend fun update(
        table: Table,
        whereClause: String,
        vararg whereParams: Any?,
        block: UpdateBuilder.(UpdateBuilder) -> Unit
    ): Int = withContext(Dispatchers.IO) {
        val builder = UpdateBuilder(table, dialect)
        builder.block(builder)
        val (setSql, setParams) = builder.build()
        val sql = "UPDATE ${dialect.quoteIdentifier(table.tableName)} SET $setSql WHERE $whereClause"
        val params = setParams + whereParams.toList()
        sqlLogger.log(sql, params)
        database.executeUpdate(sql, params)
    }

    /**
     * Delete rows matching a condition.
     *
     * **Security Warning**: The [whereClause] is interpolated directly into SQL.
     * Always use parameterized placeholders (?) and pass values via [params].
     * Never concatenate user input directly into [whereClause].
     *
     * @param table the table to delete from
     * @param whereClause the WHERE clause with ? placeholders
     * @param params values to bind to the ? placeholders
     */
    suspend fun delete(table: Table, whereClause: String, vararg params: Any?): Int =
        withContext(Dispatchers.IO) {
            val sql = "DELETE FROM ${dialect.quoteIdentifier(table.tableName)} WHERE $whereClause"
            sqlLogger.log(sql, params.toList())
            database.executeUpdate(sql, params.toList())
        }

    /**
     * Delete all rows from a table.
     */
    suspend fun deleteAll(table: Table): Int =
        withContext(Dispatchers.IO) {
            val sql = "DELETE FROM ${dialect.quoteIdentifier(table.tableName)}"
            sqlLogger.log(sql, emptyList())
            database.executeUpdate(sql, emptyList())
        }

    /**
     * Count rows in a table.
     */
    suspend fun count(table: Table): Long =
        withContext(Dispatchers.IO) {
            val sql = "SELECT COUNT(*) FROM ${dialect.quoteIdentifier(table.tableName)}"
            sqlLogger.log(sql, emptyList())
            val results = database.executeQuery(sql, emptyList()) { row ->
                (row.values.first() as Number).toLong()
            }
            results.first()
        }

    /**
     * Count rows matching a condition.
     */
    suspend fun countWhere(table: Table, whereClause: String, vararg params: Any?): Long =
        withContext(Dispatchers.IO) {
            val sql = "SELECT COUNT(*) FROM ${dialect.quoteIdentifier(table.tableName)} WHERE $whereClause"
            sqlLogger.log(sql, params.toList())
            val results = database.executeQuery(sql, params.toList()) { row ->
                (row.values.first() as Number).toLong()
            }
            results.first()
        }

    /**
     * Check if any rows exist matching a condition.
     */
    suspend fun exists(table: Table, whereClause: String, vararg params: Any?): Boolean {
        return countWhere(table, whereClause, *params) > 0
    }

    /**
     * Execute DDL to create a table.
     */
    suspend fun createTable(table: Table, ifNotExists: Boolean = true): Int =
        withContext(Dispatchers.IO) {
            val sql = dialect.createTableStatement(table, ifNotExists)
            sqlLogger.log(sql, emptyList())
            database.executeUpdate(sql, emptyList())
        }

    /**
     * Execute DDL to drop a table.
     */
    suspend fun dropTable(table: Table, ifExists: Boolean = true): Int =
        withContext(Dispatchers.IO) {
            val sql = dialect.dropTableStatement(table, ifExists)
            sqlLogger.log(sql, emptyList())
            database.executeUpdate(sql, emptyList())
        }

}

/**
 * Wrapper for result row data with typed column access.
 *
 * Provides safe access to column values with proper null handling.
 */
class ResultRow(private val data: Map<String, Any?>, private val table: Table) {

    /**
     * Get the value of a column.
     *
     * @throws NoSuchElementException if the column is not present in the result
     * @throws ClassCastException if the value cannot be cast to the expected type
     */
    @Suppress("UNCHECKED_CAST")
    operator fun <T> get(column: Column<T>): T {
        val value = data[column.name]
            ?: if (column.name in data.keys) {
                // Column exists but value is null
                null as T
            } else {
                throw NoSuchElementException("Column '${column.name}' not found in result row. Available: ${data.keys}")
            }
        return value as T
    }

    /**
     * Get the value of a column, or null if not present or null.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T> getOrNull(column: Column<T>): T? = data[column.name] as? T

    /**
     * Check if a column exists in this row.
     */
    fun contains(column: Column<*>): Boolean = column.name in data.keys

    /**
     * Get the raw map of column names to values.
     */
    fun toMap(): Map<String, Any?> = data
}

/**
 * Builder for INSERT statements.
 */
class InsertBuilder(private val table: Table, private val dialect: SqlDialect) {
    private val values = mutableMapOf<Column<*>, Any?>()

    operator fun <T> set(column: Column<T>, value: T) {
        values[column] = value
    }

    fun build(): Pair<String, List<Any?>> {
        val columns = values.keys.joinToString(", ") { dialect.quoteIdentifier(it.name) }
        val placeholders = values.keys.joinToString(", ") { "?" }
        val sql = "INSERT INTO ${dialect.quoteIdentifier(table.tableName)} ($columns) VALUES ($placeholders)"
        return sql to values.values.toList()
    }
}

/**
 * Builder for UPDATE statements.
 */
class UpdateBuilder(private val table: Table, private val dialect: SqlDialect) {
    private val sets = mutableMapOf<Column<*>, Any?>()

    operator fun <T> set(column: Column<T>, value: T) {
        sets[column] = value
    }

    fun build(): Pair<String, List<Any?>> {
        val setSql = sets.keys.joinToString(", ") { "${dialect.quoteIdentifier(it.name)} = ?" }
        return setSql to sets.values.toList()
    }
}

/**
 * Get the cached KormaDsl helper from the application.
 * The instance is lazily created and cached per application.
 */
val Application.korma: KormaDsl
    get() = attributes.getOrNull(KormaDslKey)
        ?: KormaDsl(kormaDatabase, kormaConfig).also { attributes.put(KormaDslKey, it) }

/**
 * Get the KormaDsl helper from a call.
 * Uses the cached instance from the application.
 */
val ApplicationCall.korma: KormaDsl
    get() = application.korma
