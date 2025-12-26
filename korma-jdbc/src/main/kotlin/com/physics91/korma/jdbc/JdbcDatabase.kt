package com.physics91.korma.jdbc

import com.physics91.korma.dsl.*
import com.physics91.korma.expression.OrderByExpression
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.sql.Connection
import javax.sql.DataSource

/**
 * Main entry point for JDBC database operations.
 *
 * Provides:
 * - Connection management via HikariCP
 * - Transaction support
 * - Query execution with type-safe DSL
 *
 * Example:
 * ```kotlin
 * val db = JdbcDatabase(
 *     config = DatabaseConfig(jdbcUrl = "jdbc:h2:mem:test"),
 *     dialect = H2Dialect
 * )
 *
 * db.transaction {
 *     val users = from(Users)
 *         .select(Users.id, Users.name)
 *         .where { Users.age gt 18 }
 *         .fetch()
 *
 *     insertInto(Users) {
 *         it[Users.name] = "John"
 *         it[Users.email] = "john@example.com"
 *     }.execute()
 * }
 * ```
 */
class JdbcDatabase : Closeable {

    val config: DatabaseConfig?
    val dialect: SqlDialect
    val dataSource: DataSource
    private val ownsDataSource: Boolean

    private val logger = LoggerFactory.getLogger(JdbcDatabase::class.java)

    /**
     * Create JdbcDatabase with configuration (creates HikariCP DataSource).
     */
    constructor(config: DatabaseConfig, dialect: SqlDialect) {
        this.config = config
        this.dialect = dialect
        this.dataSource = config.createDataSource()
        this.ownsDataSource = true
        logger.info("Initialized JdbcDatabase with dialect: {}", dialect.name)
    }

    /**
     * Create JdbcDatabase with an existing DataSource (e.g., from Spring).
     */
    constructor(dataSource: DataSource, dialect: SqlDialect) {
        this.config = null
        this.dialect = dialect
        this.dataSource = dataSource
        this.ownsDataSource = false
        logger.info("Initialized JdbcDatabase with dialect: {}", dialect.name)
    }

    /**
     * Transaction manager for this database.
     */
    val transactionManager: TransactionManager by lazy { TransactionManager(this) }

    // ============== Transaction Operations ==============

    /**
     * Execute a block within a transaction.
     */
    fun <T> transaction(block: DatabaseContext.() -> T): T {
        return transactionManager.transactionWithContext {
            val executor = JdbcExecutor(connection, dialect)
            val context = DatabaseContext(this@JdbcDatabase, executor, this)
            context.block()
        }
    }

    /**
     * Execute a block within a new transaction (REQUIRES_NEW semantics).
     */
    fun <T> newTransaction(block: DatabaseContext.() -> T): T {
        return transactionManager.newTransaction {
            val executor = JdbcExecutor(connection, dialect)
            val context = DatabaseContext(this@JdbcDatabase, executor, this)
            context.block()
        }
    }

    /**
     * Execute read-only operations.
     */
    fun <T> readOnly(block: DatabaseContext.() -> T): T {
        return transactionManager.readOnly {
            val executor = JdbcExecutor(connection, dialect)
            val context = DatabaseContext(this@JdbcDatabase, executor, this)
            context.block()
        }
    }

    /**
     * Execute with a specific isolation level.
     */
    fun <T> transaction(
        isolation: TransactionIsolation,
        block: DatabaseContext.() -> T
    ): T {
        return transactionManager.transaction(isolation) {
            val executor = JdbcExecutor(connection, dialect)
            val context = DatabaseContext(this@JdbcDatabase, executor, this)
            context.block()
        }
    }

    // ============== Direct Connection Operations ==============

    /**
     * Execute a block with a direct connection (no transaction).
     */
    fun <T> useConnection(block: (Connection) -> T): T {
        return dataSource.connection.use { conn ->
            block(conn)
        }
    }

    /**
     * Execute raw SQL directly.
     */
    fun executeRaw(sql: String) {
        useConnection { conn ->
            JdbcExecutor(conn, dialect).executeRaw(sql)
        }
    }

    // ============== Schema Operations ==============

    /**
     * Create a table if it doesn't exist.
     */
    fun createTable(table: Table, ifNotExists: Boolean = true) {
        val sql = dialect.createTableStatement(table, ifNotExists)
        logger.debug("Creating table: {}", sql)
        executeRaw(sql)
    }

    /**
     * Create multiple tables.
     */
    fun createTables(vararg tables: Table, ifNotExists: Boolean = true) {
        tables.forEach { createTable(it, ifNotExists) }
    }

    /**
     * Drop a table.
     */
    fun dropTable(table: Table, ifExists: Boolean = true) {
        val sql = dialect.dropTableStatement(table, ifExists)
        logger.debug("Dropping table: {}", sql)
        executeRaw(sql)
    }

    /**
     * Drop multiple tables.
     */
    fun dropTables(vararg tables: Table, ifExists: Boolean = true) {
        tables.reversed().forEach { dropTable(it, ifExists) }
    }

    /**
     * Check if a table exists.
     */
    fun tableExists(table: Table): Boolean {
        return useConnection { conn ->
            JdbcExecutor(conn, dialect).tableExists(table.tableName)
        }
    }

    // ============== Lifecycle ==============

    /**
     * Check if the database connection is healthy.
     */
    fun isHealthy(): Boolean {
        return try {
            useConnection { conn ->
                conn.isValid(5)
            }
        } catch (e: Exception) {
            logger.warn("Database health check failed", e)
            false
        }
    }

    /**
     * Close the database connection pool.
     * Only closes if this instance owns the DataSource.
     */
    override fun close() {
        if (ownsDataSource) {
            logger.info("Closing JdbcDatabase")
            (dataSource as? HikariDataSource)?.close()
        }
    }

    // ============== Simple Query Methods for Spring Integration ==============

    /**
     * Execute a query and map results.
     */
    fun <T> executeQuery(sql: String, params: List<Any?>, mapper: (Map<String, Any?>) -> T): List<T> {
        return useConnection { conn ->
            conn.prepareStatement(sql).use { stmt ->
                params.forEachIndexed { index, param ->
                    stmt.setObject(index + 1, param)
                }
                stmt.executeQuery().use { rs ->
                    val results = mutableListOf<T>()
                    val metaData = rs.metaData
                    val columnCount = metaData.columnCount

                    while (rs.next()) {
                        val row = mutableMapOf<String, Any?>()
                        for (i in 1..columnCount) {
                            row[metaData.getColumnName(i)] = rs.getObject(i)
                        }
                        results.add(mapper(row))
                    }
                    results
                }
            }
        }
    }

    /**
     * Execute an update statement.
     */
    fun executeUpdate(sql: String, params: List<Any?>): Int {
        return useConnection { conn ->
            conn.prepareStatement(sql).use { stmt ->
                params.forEachIndexed { index, param ->
                    stmt.setObject(index + 1, param)
                }
                stmt.executeUpdate()
            }
        }
    }

    /**
     * Execute an insert and return generated key.
     */
    fun executeInsert(sql: String, params: List<Any?>): Long {
        return useConnection { conn ->
            conn.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS).use { stmt ->
                params.forEachIndexed { index, param ->
                    stmt.setObject(index + 1, param)
                }
                stmt.executeUpdate()
                stmt.generatedKeys.use { generatedKeys ->
                    if (generatedKeys.next()) {
                        generatedKeys.getLong(1)
                    } else {
                        0L
                    }
                }
            }
        }
    }
}

/**
 * Context for database operations within a transaction.
 */
class DatabaseContext(
    val database: JdbcDatabase,
    val executor: JdbcExecutor,
    val transaction: Transaction
) {
    val dialect: SqlDialect get() = database.dialect

    // ============== SELECT Operations ==============

    /**
     * Start a SELECT query.
     */
    fun from(table: Table): SelectExecutor = SelectExecutor(SelectBuilder(table), this)

    /**
     * Start a SELECT query from a subquery.
     */
    fun from(subquery: SelectBuilder, alias: String): SelectExecutor =
        SelectExecutor(SelectBuilder(SubquerySource(subquery, alias)), this)

    // ============== INSERT Operations ==============

    /**
     * Start an INSERT query.
     */
    fun insertInto(table: Table): InsertExecutor = InsertExecutor(InsertBuilder(table), this)

    /**
     * INSERT with DSL.
     */
    inline fun insertInto(table: Table, block: InsertBuilder.() -> Unit): InsertExecutor =
        InsertExecutor(InsertBuilder(table).apply(block), this)

    /**
     * Start a batch INSERT.
     */
    fun batchInsertInto(table: Table): BatchInsertExecutor =
        BatchInsertExecutor(BatchInsertBuilder(table), this)

    /**
     * Batch INSERT with items.
     */
    inline fun <T> batchInsertInto(
        table: Table,
        items: Collection<T>,
        crossinline mapper: InsertBuilder.(T) -> Unit
    ): BatchInsertExecutor =
        BatchInsertExecutor(BatchInsertBuilder(table).addRows(items) { mapper(it) }, this)

    // ============== UPDATE Operations ==============

    /**
     * Start an UPDATE query.
     */
    fun update(table: Table): UpdateExecutor = UpdateExecutor(UpdateBuilder(table), this)

    /**
     * UPDATE with DSL.
     */
    inline fun update(table: Table, block: UpdateBuilder.() -> Unit): UpdateExecutor =
        UpdateExecutor(UpdateBuilder(table).apply(block), this)

    // ============== DELETE Operations ==============

    /**
     * Start a DELETE query.
     */
    fun deleteFrom(table: Table): DeleteExecutor = DeleteExecutor(DeleteBuilder(table), this)

    /**
     * DELETE with condition.
     */
    fun deleteFrom(table: Table, condition: () -> Predicate): DeleteExecutor =
        DeleteExecutor(DeleteBuilder(table).where(condition()), this)

    // ============== Raw Query ==============

    /**
     * Execute raw SQL query.
     */
    fun <T> query(sql: String, params: List<Any?> = emptyList(), mapper: (Row) -> T): List<T> {
        return executor.query(PreparedSql(sql, params), mapper)
    }

    /**
     * Execute raw SQL statement.
     */
    fun execute(sql: String, params: List<Any?> = emptyList()): Int {
        return executor.execute(PreparedSql(sql, params))
    }
}

// ============== Query Executors ==============

/**
 * Executor for SELECT queries.
 */
class SelectExecutor(
    private val builder: SelectBuilder,
    private val context: DatabaseContext
) {
    fun select(vararg columns: Column<*>) = also { builder.select(*columns) }
    fun selectAll() = also { builder.selectAll() }
    fun distinct() = also { builder.distinct() }
    fun where(condition: Predicate) = also { builder.where(condition) }
    fun where(condition: () -> Predicate) = also { builder.where(condition) }
    fun andWhere(condition: Predicate) = also { builder.andWhere(condition) }
    fun orWhere(condition: Predicate) = also { builder.orWhere(condition) }
    fun join(table: Table, on: Predicate, alias: String? = null) = also { builder.join(table, on, alias) }
    fun leftJoin(table: Table, on: Predicate, alias: String? = null) = also { builder.leftJoin(table, on, alias) }
    fun rightJoin(table: Table, on: Predicate, alias: String? = null) = also { builder.rightJoin(table, on, alias) }
    fun groupBy(vararg columns: Column<*>) = also { builder.groupBy(*columns) }
    fun having(condition: Predicate) = also { builder.having(condition) }
    fun orderBy(vararg orders: OrderByExpression) = also { builder.orderBy(*orders) }
    fun limit(count: Int) = also { builder.limit(count) }
    fun offset(count: Long) = also { builder.offset(count) }
    fun paginate(page: Int, pageSize: Int) = also { builder.paginate(page, pageSize) }
    fun forUpdate() = also { builder.forUpdate() }

    /**
     * Execute the query and map results.
     */
    fun <T> fetch(mapper: (Row) -> T): List<T> {
        val sql = builder.build(context.dialect)
        return context.executor.query(sql, mapper)
    }

    /**
     * Execute the query and get the first result.
     */
    fun <T> fetchFirst(mapper: (Row) -> T): T? {
        val sql = builder.limit(1).build(context.dialect)
        return context.executor.querySingle(sql, mapper)
    }

    /**
     * Execute the query and get exactly one result.
     */
    fun <T> fetchSingle(mapper: (Row) -> T): T {
        val sql = builder.build(context.dialect)
        return context.executor.querySingle(sql, mapper)
            ?: throw NoSuchElementException("Query returned no results")
    }

    /**
     * Count the number of matching rows.
     */
    fun count(): Long {
        val sql = builder.build(context.dialect)
        val countSql = PreparedSql(
            "SELECT COUNT(*) FROM (${sql.sql}) AS _count_subquery",
            sql.params
        )
        return context.executor.querySingle(countSql) { row ->
            row.getLong(1)
        } ?: 0L
    }

    /**
     * Check if any matching rows exist.
     */
    fun exists(): Boolean = count() > 0

    /**
     * Get the built SQL without executing.
     */
    fun toSql(): PreparedSql = builder.build(context.dialect)
}

/**
 * Executor for INSERT queries.
 */
class InsertExecutor(
    private val builder: InsertBuilder,
    private val context: DatabaseContext
) {
    fun <T> set(column: Column<T>, value: T) = also { builder.set(column, value) }
    fun onConflict(vararg columns: Column<*>) = OnConflictExecutor(builder.onConflict(*columns), this)
    fun returning(vararg columns: Column<*>) = also { builder.returning(*columns) }
    fun returningAll() = also { builder.returningAll() }

    /**
     * Execute the insert and return affected row count.
     */
    fun execute(): Int {
        val sql = builder.build(context.dialect)
        return context.executor.execute(sql)
    }

    /**
     * Execute the insert and return generated ID.
     */
    fun <T> executeAndGetId(idColumn: Column<T>): T {
        val sql = builder.build(context.dialect)
        return context.executor.executeWithGeneratedKeys(sql) { row ->
            row[idColumn] ?: throw IllegalStateException("No generated ID")
        }
    }

    /**
     * Execute the insert and return generated Long ID.
     */
    fun executeAndGetId(): Long {
        val sql = builder.build(context.dialect)
        return context.executor.executeWithGeneratedKeys(sql) { row ->
            row.getLong(1)
        }
    }

    fun toSql(): PreparedSql = builder.build(context.dialect)

    inner class OnConflictExecutor(
        private val onConflict: InsertBuilder.OnConflictBuilder,
        private val parent: InsertExecutor
    ) {
        fun doNothing() = parent.also { onConflict.doNothing() }
        fun doUpdate(vararg columns: Column<*>) = parent.also { onConflict.doUpdate(*columns) }
        fun doUpdateAll() = parent.also { onConflict.doUpdateAll() }
    }
}

/**
 * Executor for batch INSERT queries.
 */
class BatchInsertExecutor(
    private val builder: BatchInsertBuilder,
    private val context: DatabaseContext
) {
    fun addRow(block: InsertBuilder.() -> Unit) = also { builder.addRow(block) }
    fun <T> addRows(items: Collection<T>, mapper: InsertBuilder.(T) -> Unit) = also {
        builder.addRows(items, mapper)
    }

    /**
     * Execute the batch insert.
     */
    fun execute(): IntArray {
        val sqls = builder.build(context.dialect)
        return context.executor.executeBatch(sqls)
    }

    /**
     * Execute and return total affected rows.
     */
    fun executeAndGetCount(): Int = execute().sum()
}

/**
 * Executor for UPDATE queries.
 */
class UpdateExecutor(
    private val builder: UpdateBuilder,
    private val context: DatabaseContext
) {
    fun <T> set(column: Column<T>, value: T) = also { builder.set(column, value) }
    fun <T> setNull(column: Column<T?>) = also { builder.setNull(column) }
    fun increment(column: Column<Int>, by: Int = 1) = also { builder.increment(column, by) }
    fun increment(column: Column<Long>, by: Long = 1) = also { builder.increment(column, by) }
    fun decrement(column: Column<Int>, by: Int = 1) = also { builder.decrement(column, by) }
    fun where(condition: Predicate) = also { builder.where(condition) }
    fun where(condition: () -> Predicate) = also { builder.where(condition) }
    fun andWhere(condition: Predicate) = also { builder.andWhere(condition) }
    fun returning(vararg columns: Column<*>) = also { builder.returning(*columns) }
    fun returningAll() = also { builder.returningAll() }

    /**
     * Execute the update and return affected row count.
     */
    fun execute(): Int {
        val sql = builder.build(context.dialect)
        return context.executor.execute(sql)
    }

    fun toSql(): PreparedSql = builder.build(context.dialect)
}

/**
 * Executor for DELETE queries.
 */
class DeleteExecutor(
    private val builder: DeleteBuilder,
    private val context: DatabaseContext
) {
    fun where(condition: Predicate) = also { builder.where(condition) }
    fun where(condition: () -> Predicate) = also { builder.where(condition) }
    fun andWhere(condition: Predicate) = also { builder.andWhere(condition) }
    fun orWhere(condition: Predicate) = also { builder.orWhere(condition) }
    fun returning(vararg columns: Column<*>) = also { builder.returning(*columns) }
    fun returningAll() = also { builder.returningAll() }

    /**
     * Execute the delete and return affected row count.
     */
    fun execute(): Int {
        val sql = builder.build(context.dialect)
        return context.executor.execute(sql)
    }

    fun toSql(): PreparedSql = builder.build(context.dialect)
}

// ============== Factory Functions ==============

/**
 * Create a JdbcDatabase with DSL configuration.
 */
fun jdbcDatabase(
    dialect: SqlDialect,
    configure: DatabaseConfigBuilder.() -> Unit
): JdbcDatabase {
    val config = DatabaseConfigBuilder().apply(configure).build()
    return JdbcDatabase(config, dialect)
}
