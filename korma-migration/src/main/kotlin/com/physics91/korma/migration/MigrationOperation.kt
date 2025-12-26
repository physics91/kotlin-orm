package com.physics91.korma.migration

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.SqlDialect

/**
 * Sealed class representing all possible migration operations.
 * Each operation can generate SQL for the target dialect.
 */
sealed class MigrationOperation {
    abstract fun toSql(dialect: SqlDialect): String

    // ============== Table Operations ==============

    data class CreateTable(val tableInfo: MigrationTableInfo) : MigrationOperation() {
        constructor(table: Table) : this(MigrationTableInfo.fromTable(table))

        override fun toSql(dialect: SqlDialect): String {
            return buildString {
                append("CREATE TABLE ")
                append(dialect.quoteIdentifier(tableInfo.tableName))
                append(" (")

                val columnDefs = tableInfo.columns.map { column ->
                    buildColumnDefinition(column, dialect)
                }
                append(columnDefs.joinToString(", "))

                // Primary key constraint
                val primaryKeys = tableInfo.columns.filter { it.isPrimaryKey }
                if (primaryKeys.isNotEmpty()) {
                    append(", PRIMARY KEY (")
                    append(primaryKeys.joinToString(", ") { dialect.quoteIdentifier(it.name) })
                    append(")")
                }

                append(")")
            }
        }

        private fun buildColumnDefinition(column: MigrationColumn<*>, dialect: SqlDialect): String {
            return buildString {
                append(dialect.quoteIdentifier(column.name))
                append(" ")
                append(
                    if (column.isAutoIncrement) {
                        dialect.autoIncrementType(column.type)
                    } else {
                        dialect.sqlTypeName(column.type)
                    }
                )
                if (!column.isNullable && !column.isPrimaryKey) {
                    append(" NOT NULL")
                }
                if (column.isUnique) {
                    append(" UNIQUE")
                }
                column.defaultValue?.let { default ->
                    append(" DEFAULT ")
                    append(formatDefault(default))
                }
            }
        }
    }

    data class DropTable(val tableName: String) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return "DROP TABLE ${dialect.quoteIdentifier(tableName)}"
        }
    }

    data class RenameTable(val oldName: String, val newName: String) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return "ALTER TABLE ${dialect.quoteIdentifier(oldName)} RENAME TO ${dialect.quoteIdentifier(newName)}"
        }
    }

    // ============== Column Operations ==============

    data class AddColumn(val tableName: String, val column: MigrationColumn<*>) : MigrationOperation() {
        constructor(tableName: String, column: Column<*>) : this(tableName, column.toMigrationColumn())

        override fun toSql(dialect: SqlDialect): String {
            return buildString {
                append("ALTER TABLE ")
                append(dialect.quoteIdentifier(tableName))
                append(" ADD COLUMN ")
                append(dialect.quoteIdentifier(column.name))
                append(" ")
                append(
                    if (column.isAutoIncrement) {
                        dialect.autoIncrementType(column.type)
                    } else {
                        dialect.sqlTypeName(column.type)
                    }
                )
                if (!column.isNullable) {
                    append(" NOT NULL")
                }
                if (column.isUnique) {
                    append(" UNIQUE")
                }
                column.defaultValue?.let { default ->
                    append(" DEFAULT ")
                    append(formatDefault(default))
                }
            }
        }
    }

    data class DropColumn(val tableName: String, val columnName: String) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return "ALTER TABLE ${dialect.quoteIdentifier(tableName)} DROP COLUMN ${dialect.quoteIdentifier(columnName)}"
        }
    }

    data class RenameColumn(
        val tableName: String,
        val oldName: String,
        val newName: String
    ) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return "ALTER TABLE ${dialect.quoteIdentifier(tableName)} RENAME COLUMN ${dialect.quoteIdentifier(oldName)} TO ${dialect.quoteIdentifier(newName)}"
        }
    }

    data class ModifyColumn(val tableName: String, val column: MigrationColumn<*>) : MigrationOperation() {
        constructor(tableName: String, column: Column<*>) : this(tableName, column.toMigrationColumn())

        override fun toSql(dialect: SqlDialect): String {
            return buildString {
                append("ALTER TABLE ")
                append(dialect.quoteIdentifier(tableName))
                append(" ALTER COLUMN ")
                append(dialect.quoteIdentifier(column.name))
                append(" ")
                append(dialect.sqlTypeName(column.type))
                if (!column.isNullable) {
                    append(" NOT NULL")
                }
            }
        }
    }

    // ============== Index Operations ==============

    data class CreateIndex(
        val indexName: String,
        val tableName: String,
        val columns: List<String>,
        val unique: Boolean = false
    ) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return buildString {
                append("CREATE ")
                if (unique) append("UNIQUE ")
                append("INDEX ")
                append(dialect.quoteIdentifier(indexName))
                append(" ON ")
                append(dialect.quoteIdentifier(tableName))
                append(" (")
                append(columns.joinToString(", ") { dialect.quoteIdentifier(it) })
                append(")")
            }
        }
    }

    data class DropIndex(val indexName: String, val tableName: String?) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            // Some databases require table name for DROP INDEX
            return if (tableName != null) {
                "DROP INDEX ${dialect.quoteIdentifier(indexName)} ON ${dialect.quoteIdentifier(tableName)}"
            } else {
                "DROP INDEX ${dialect.quoteIdentifier(indexName)}"
            }
        }
    }

    // ============== Constraint Operations ==============

    data class AddForeignKey(
        val constraintName: String,
        val tableName: String,
        val column: String,
        val referencedTable: String,
        val referencedColumn: String,
        val onDelete: ForeignKeyAction = ForeignKeyAction.NO_ACTION,
        val onUpdate: ForeignKeyAction = ForeignKeyAction.NO_ACTION
    ) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return buildString {
                append("ALTER TABLE ")
                append(dialect.quoteIdentifier(tableName))
                append(" ADD CONSTRAINT ")
                append(dialect.quoteIdentifier(constraintName))
                append(" FOREIGN KEY (")
                append(dialect.quoteIdentifier(column))
                append(") REFERENCES ")
                append(dialect.quoteIdentifier(referencedTable))
                append(" (")
                append(dialect.quoteIdentifier(referencedColumn))
                append(")")
                if (onDelete != ForeignKeyAction.NO_ACTION) {
                    append(" ON DELETE ")
                    append(onDelete.toSql())
                }
                if (onUpdate != ForeignKeyAction.NO_ACTION) {
                    append(" ON UPDATE ")
                    append(onUpdate.toSql())
                }
            }
        }
    }

    data class AddCheckConstraint(
        val constraintName: String,
        val tableName: String,
        val expression: String
    ) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return "ALTER TABLE ${dialect.quoteIdentifier(tableName)} ADD CONSTRAINT ${dialect.quoteIdentifier(constraintName)} CHECK ($expression)"
        }
    }

    data class DropConstraint(
        val constraintName: String,
        val tableName: String
    ) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String {
            return "ALTER TABLE ${dialect.quoteIdentifier(tableName)} DROP CONSTRAINT ${dialect.quoteIdentifier(constraintName)}"
        }
    }

    // ============== Raw SQL ==============

    data class RawSql(val statement: String) : MigrationOperation() {
        override fun toSql(dialect: SqlDialect): String = statement
    }
}

/**
 * Format a default value for SQL.
 */
private fun formatDefault(value: Any?): String = when (value) {
    null -> "NULL"
    is SqlDefault -> value.sql
    is String -> "'${value.replace("'", "''")}'"
    is Boolean -> if (value) "TRUE" else "FALSE"
    else -> value.toString()
}

/**
 * Extension to convert ForeignKeyAction to SQL.
 */
fun ForeignKeyAction.toSql(): String = when (this) {
    ForeignKeyAction.NO_ACTION -> "NO ACTION"
    ForeignKeyAction.RESTRICT -> "RESTRICT"
    ForeignKeyAction.CASCADE -> "CASCADE"
    ForeignKeyAction.SET_NULL -> "SET NULL"
    ForeignKeyAction.SET_DEFAULT -> "SET DEFAULT"
}

/**
 * Table information for migration operations.
 */
data class MigrationTableInfo(
    val tableName: String,
    val columns: List<MigrationColumn<*>>
) {
    companion object {
        fun fromTable(table: Table): MigrationTableInfo {
            return MigrationTableInfo(
                tableName = table.tableName,
                columns = table.columns.map { it.toMigrationColumn() }
            )
        }
    }
}
