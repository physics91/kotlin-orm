package com.physics91.korma.migration

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.ColumnType

/**
 * Lightweight column representation for migration operations.
 * Unlike the core Column class, this doesn't require a Table reference.
 */
data class MigrationColumn<T>(
    val name: String,
    val type: ColumnType<T>,
    val isPrimaryKey: Boolean = false,
    val isAutoIncrement: Boolean = false,
    val isNullable: Boolean = false,
    val isUnique: Boolean = false,
    val defaultValue: Any? = null
) {
    companion object {
        /**
         * Create a MigrationColumn from an existing Column.
         */
        fun <T> fromColumn(column: Column<T>): MigrationColumn<T> {
            return MigrationColumn(
                name = column.name,
                type = column.type,
                isPrimaryKey = column.isPrimaryKey,
                isAutoIncrement = column.isAutoIncrement,
                isNullable = column.nullable,
                isUnique = column.isUnique,
                defaultValue = column.defaultValue?.let { SqlDefault(it) }
            )
        }
    }
}

/**
 * Marker for raw SQL default expressions (e.g., CURRENT_TIMESTAMP).
 */
data class SqlDefault(val sql: String)

/**
 * Extension to convert Column to MigrationColumn.
 */
fun <T> Column<T>.toMigrationColumn(): MigrationColumn<T> = MigrationColumn.fromColumn(this)
