package com.physics91.korma.migration

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.SqlDialect

/**
 * Context for executing migration operations.
 * Provides a DSL for defining schema changes.
 */
class MigrationContext(
    val dialect: SqlDialect,
    private val executor: MigrationExecutor
) {
    private val operations = mutableListOf<MigrationOperation>()

    // ============== Table Operations ==============

    /**
     * Create a new table.
     */
    fun createTable(name: String, block: TableBuilder.() -> Unit) {
        val builder = TableBuilder(name)
        builder.block()
        operations.add(MigrationOperation.CreateTable(builder.build()))
    }

    /**
     * Create a table from an existing Table definition.
     */
    fun createTable(table: Table) {
        operations.add(MigrationOperation.CreateTable(table))
    }

    /**
     * Drop a table.
     */
    fun dropTable(name: String) {
        operations.add(MigrationOperation.DropTable(name))
    }

    /**
     * Drop a table.
     */
    fun dropTable(table: Table) {
        operations.add(MigrationOperation.DropTable(table.tableName))
    }

    /**
     * Rename a table.
     */
    fun renameTable(oldName: String, newName: String) {
        operations.add(MigrationOperation.RenameTable(oldName, newName))
    }

    // ============== Column Operations ==============

    /**
     * Add a column to an existing table.
     */
    fun addColumn(tableName: String, column: Column<*>) {
        operations.add(MigrationOperation.AddColumn(tableName, column))
    }

    /**
     * Add a column to an existing table using DSL.
     */
    fun addColumn(tableName: String, name: String, block: ColumnBuilder.() -> Unit) {
        val builder = ColumnBuilder(name)
        builder.block()
        operations.add(MigrationOperation.AddColumn(tableName, builder.build()))
    }

    /**
     * Drop a column from a table.
     */
    fun dropColumn(tableName: String, columnName: String) {
        operations.add(MigrationOperation.DropColumn(tableName, columnName))
    }

    /**
     * Rename a column.
     */
    fun renameColumn(tableName: String, oldName: String, newName: String) {
        operations.add(MigrationOperation.RenameColumn(tableName, oldName, newName))
    }

    /**
     * Modify a column's type or constraints.
     */
    fun modifyColumn(tableName: String, column: Column<*>) {
        operations.add(MigrationOperation.ModifyColumn(tableName, column))
    }

    /**
     * Modify a column using DSL (type/constraints).
     */
    fun modifyColumn(tableName: String, name: String, block: ColumnBuilder.() -> Unit) {
        val builder = ColumnBuilder(name)
        builder.block()
        operations.add(MigrationOperation.ModifyColumn(tableName, builder.build()))
    }

    // ============== Index Operations ==============

    /**
     * Create an index.
     */
    fun createIndex(
        indexName: String,
        tableName: String,
        columns: List<String>,
        unique: Boolean = false
    ) {
        operations.add(MigrationOperation.CreateIndex(indexName, tableName, columns, unique))
    }

    /**
     * Create a unique index.
     */
    fun createUniqueIndex(indexName: String, tableName: String, vararg columns: String) {
        createIndex(indexName, tableName, columns.toList(), unique = true)
    }

    /**
     * Drop an index.
     */
    fun dropIndex(indexName: String, tableName: String? = null) {
        operations.add(MigrationOperation.DropIndex(indexName, tableName))
    }

    // ============== Constraint Operations ==============

    /**
     * Add a foreign key constraint.
     */
    fun addForeignKey(
        constraintName: String,
        tableName: String,
        column: String,
        referencedTable: String,
        referencedColumn: String,
        onDelete: ForeignKeyAction = ForeignKeyAction.NO_ACTION,
        onUpdate: ForeignKeyAction = ForeignKeyAction.NO_ACTION
    ) {
        operations.add(
            MigrationOperation.AddForeignKey(
                constraintName, tableName, column,
                referencedTable, referencedColumn,
                onDelete, onUpdate
            )
        )
    }

    /**
     * Drop a foreign key constraint.
     */
    fun dropForeignKey(constraintName: String, tableName: String) {
        operations.add(MigrationOperation.DropConstraint(constraintName, tableName))
    }

    /**
     * Add a check constraint.
     */
    fun addCheckConstraint(constraintName: String, tableName: String, expression: String) {
        operations.add(MigrationOperation.AddCheckConstraint(constraintName, tableName, expression))
    }

    /**
     * Drop a constraint.
     */
    fun dropConstraint(constraintName: String, tableName: String) {
        operations.add(MigrationOperation.DropConstraint(constraintName, tableName))
    }

    // ============== Raw SQL ==============

    /**
     * Execute raw SQL.
     */
    fun sql(statement: String) {
        operations.add(MigrationOperation.RawSql(statement))
    }

    /**
     * Execute multiple raw SQL statements.
     */
    fun sql(vararg statements: String) {
        statements.forEach { sql(it) }
    }

    // ============== Execution ==============

    /**
     * Get all recorded operations.
     */
    fun getOperations(): List<MigrationOperation> = operations.toList()

    /**
     * Execute all operations.
     */
    internal fun execute() {
        operations.forEach { operation ->
            val sql = operation.toSql(dialect)
            executor.execute(sql)
        }
    }

    /**
     * Generate SQL for all operations without executing.
     */
    fun generateSql(): List<String> {
        return operations.map { it.toSql(dialect) }
    }
}

/**
 * Foreign key actions.
 */
enum class ForeignKeyAction {
    NO_ACTION,
    RESTRICT,
    CASCADE,
    SET_NULL,
    SET_DEFAULT
}

/**
 * Executor interface for migration operations.
 */
interface MigrationExecutor {
    fun execute(sql: String)
    fun executeQuery(sql: String): List<Map<String, Any?>>
}
