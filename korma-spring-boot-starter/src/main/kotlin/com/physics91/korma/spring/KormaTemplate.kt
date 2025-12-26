package com.physics91.korma.spring

import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.SqlDialect
import com.physics91.korma.sql.SqlLogger
import org.slf4j.LoggerFactory

/**
 * Spring-friendly template for Korma database operations.
 *
 * KormaTemplate provides a convenient way to perform database operations
 * within Spring applications, similar to Spring's JdbcTemplate.
 *
 * Example usage:
 * ```kotlin
 * @Service
 * class UserService(private val korma: KormaTemplate) {
 *
 *     fun findAllUsers(): List<User> = korma.selectAll(Users) { row ->
 *         User(
 *             id = row[Users.id],
 *             name = row[Users.name],
 *             email = row[Users.email]
 *         )
 *     }
 *
 *     @Transactional
 *     fun createUser(name: String, email: String): Long {
 *         return korma.insert(Users) {
 *             it[Users.name] = name
 *             it[Users.email] = email
 *         }
 *     }
 * }
 * ```
 */
open class KormaTemplate(
    val database: JdbcDatabase,
    private val properties: KormaProperties
) {
    private val sqlLogger = SqlLogger(
        showSql = properties.showSql,
        formatSql = properties.formatSql,
        logger = LoggerFactory.getLogger(KormaTemplate::class.java)
    )

    val dialect: SqlDialect get() = database.dialect

    /**
     * Execute a raw SQL query and map results.
     */
    fun <T> query(sql: String, vararg params: Any?, mapper: (Map<String, Any?>) -> T): List<T> {
        sqlLogger.log(sql, params.toList())
        return database.executeQuery(sql, params.toList(), mapper)
    }

    /**
     * Execute a raw SQL update/insert/delete.
     */
    fun execute(sql: String, vararg params: Any?): Int {
        sqlLogger.log(sql, params.toList())
        return database.executeUpdate(sql, params.toList())
    }

    /**
     * Select all rows from a table.
     */
    fun <T> selectAll(table: Table, mapper: (ResultRow) -> T): List<T> {
        val sql = "SELECT * FROM ${dialect.quoteIdentifier(table.tableName)}"
        sqlLogger.log(sql, emptyList())
        return database.executeQuery(sql, emptyList()) { row ->
            mapper(ResultRow(row, table))
        }
    }

    /**
     * Select rows with a WHERE clause.
     */
    fun <T> selectWhere(
        table: Table,
        whereClause: String,
        vararg params: Any?,
        mapper: (ResultRow) -> T
    ): List<T> {
        val sql = "SELECT * FROM ${dialect.quoteIdentifier(table.tableName)} WHERE $whereClause"
        sqlLogger.log(sql, params.toList())
        return database.executeQuery(sql, params.toList()) { row ->
            mapper(ResultRow(row, table))
        }
    }

    /**
     * Find a single row by primary key.
     */
    fun <T, ID> findById(table: Table, idColumn: Column<ID>, id: ID, mapper: (ResultRow) -> T): T? {
        val sql = "SELECT * FROM ${dialect.quoteIdentifier(table.tableName)} " +
                "WHERE ${dialect.quoteIdentifier(idColumn.name)} = ?"
        sqlLogger.log(sql, listOf(id))
        val results = database.executeQuery(sql, listOf(id)) { row ->
            mapper(ResultRow(row, table))
        }
        return results.firstOrNull()
    }

    /**
     * Insert a row and return generated keys.
     */
    fun insert(table: Table, block: InsertBuilder.(InsertBuilder) -> Unit): Long {
        val builder = InsertBuilder(table, dialect)
        builder.block(builder)
        val (sql, params) = builder.build()
        sqlLogger.log(sql, params)
        return database.executeInsert(sql, params)
    }

    /**
     * Insert multiple rows in a batch.
     */
    fun insertBatch(table: Table, rows: List<InsertBuilder.(InsertBuilder) -> Unit>): List<Long> {
        if (rows.isEmpty()) return emptyList()

        return rows.chunked(properties.batchSize).flatMap { chunk ->
            chunk.map { block ->
                insert(table, block)
            }
        }
    }

    /**
     * Update rows matching a condition.
     */
    fun update(
        table: Table,
        whereClause: String,
        vararg whereParams: Any?,
        block: UpdateBuilder.(UpdateBuilder) -> Unit
    ): Int {
        val builder = UpdateBuilder(table, dialect)
        builder.block(builder)
        val (setSql, setParams) = builder.build()
        val sql = "UPDATE ${dialect.quoteIdentifier(table.tableName)} SET $setSql WHERE $whereClause"
        val params = setParams + whereParams.toList()
        sqlLogger.log(sql, params)
        return database.executeUpdate(sql, params)
    }

    /**
     * Delete rows matching a condition.
     */
    fun delete(table: Table, whereClause: String, vararg params: Any?): Int {
        val sql = "DELETE FROM ${dialect.quoteIdentifier(table.tableName)} WHERE $whereClause"
        sqlLogger.log(sql, params.toList())
        return database.executeUpdate(sql, params.toList())
    }

    /**
     * Delete all rows from a table.
     */
    fun deleteAll(table: Table): Int {
        val sql = "DELETE FROM ${dialect.quoteIdentifier(table.tableName)}"
        sqlLogger.log(sql, emptyList())
        return database.executeUpdate(sql, emptyList())
    }

    /**
     * Count rows in a table.
     */
    fun count(table: Table): Long {
        val sql = "SELECT COUNT(*) FROM ${dialect.quoteIdentifier(table.tableName)}"
        sqlLogger.log(sql, emptyList())
        val results = database.executeQuery(sql, emptyList()) { row ->
            (row.values.first() as Number).toLong()
        }
        return results.first()
    }

    /**
     * Count rows matching a condition.
     */
    fun countWhere(table: Table, whereClause: String, vararg params: Any?): Long {
        val sql = "SELECT COUNT(*) FROM ${dialect.quoteIdentifier(table.tableName)} WHERE $whereClause"
        sqlLogger.log(sql, params.toList())
        val results = database.executeQuery(sql, params.toList()) { row ->
            (row.values.first() as Number).toLong()
        }
        return results.first()
    }

    /**
     * Check if any rows exist matching a condition.
     */
    fun exists(table: Table, whereClause: String, vararg params: Any?): Boolean {
        return countWhere(table, whereClause, *params) > 0
    }

    /**
     * Execute DDL to create a table.
     */
    fun createTable(table: Table, ifNotExists: Boolean = true): Int {
        val sql = dialect.createTableStatement(table, ifNotExists)
        sqlLogger.log(sql, emptyList())
        return database.executeUpdate(sql, emptyList())
    }

    /**
     * Execute DDL to drop a table.
     */
    fun dropTable(table: Table, ifExists: Boolean = true): Int {
        val sql = dialect.dropTableStatement(table, ifExists)
        sqlLogger.log(sql, emptyList())
        return database.executeUpdate(sql, emptyList())
    }

}

/**
 * Wrapper for result row data with typed column access.
 */
class ResultRow(private val data: Map<String, Any?>, private val table: Table) {
    @Suppress("UNCHECKED_CAST")
    operator fun <T> get(column: Column<T>): T = data[column.name] as T

    fun <T> getOrNull(column: Column<T>): T? = data[column.name] as? T

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
