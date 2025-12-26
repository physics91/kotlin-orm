package com.physics91.korma.schema

import com.physics91.korma.expression.ColumnExpression

/**
 * Represents a column in a database table.
 *
 * @param T The Kotlin type of the column value
 * @property table The table this column belongs to
 * @property name The column name in the database
 * @property type The column type definition
 */
class Column<T>(
    val table: Table,
    val name: String,
    val type: ColumnType<T>
) {
    /** Whether this column is part of the primary key */
    var isPrimaryKey: Boolean = false
        internal set

    /** Whether this column auto-increments */
    var isAutoIncrement: Boolean = false
        internal set

    /** Whether this column has a UNIQUE constraint */
    var isUnique: Boolean = false
        internal set

    /** Whether this column has a NOT NULL constraint */
    var isNotNull: Boolean = false
        internal set

    /** Default value expression (SQL string) */
    var defaultValue: String? = null
        internal set

    /** Client-side default value generator */
    var clientDefaultValue: (() -> T)? = null
        internal set

    /** Foreign key reference */
    var foreignKey: ForeignKeyReference? = null
        internal set

    /** Index name if this column is indexed */
    var indexName: String? = null
        internal set

    /** Whether this column is nullable (derived from type) */
    val nullable: Boolean
        get() = type.nullable

    /** Full qualified name: table.column */
    val qualifiedName: String
        get() = "${table.tableName}.$name"

    // ============== Column Modifiers (Fluent API) ==============

    /** Mark this column as primary key */
    fun primaryKey(): Column<T> = apply {
        isPrimaryKey = true
        table.registerPrimaryKeyColumn(this)
    }

    /** Mark this column as auto-increment */
    fun autoIncrement(): Column<T> = apply {
        isAutoIncrement = true
        isPrimaryKey = true
        table.registerPrimaryKeyColumn(this)
    }

    /** Mark this column as unique */
    fun unique(): Column<T> = apply { isUnique = true }

    /** Mark this column as NOT NULL */
    fun notNull(): Column<T> = apply { isNotNull = true }

    /** Set a default SQL expression */
    fun default(expression: String): Column<T> = apply { defaultValue = expression }

    /** Set a default value (will be converted to SQL literal) */
    fun defaultValue(value: T): Column<T> = apply {
        defaultValue = when (value) {
            is String -> "'${escapeString(value)}'"
            is Boolean -> if (value) "TRUE" else "FALSE"
            else -> value.toString()
        }
    }

    /** Escape single quotes in SQL string literals */
    private fun escapeString(value: String): String =
        value.replace("'", "''")

    /** Set a client-side default value generator */
    fun clientDefault(generator: () -> T): Column<T> = apply { clientDefaultValue = generator }

    /** Create a simple index on this column */
    fun index(name: String? = null): Column<T> = apply {
        indexName = name ?: "idx_${table.tableName}_${this.name}"
        table.addIndex(Index(indexName!!, listOf(this), false))
    }

    /** Create a unique index on this column */
    fun uniqueIndex(name: String? = null): Column<T> = apply {
        indexName = name ?: "idx_${table.tableName}_${this.name}_unique"
        table.addIndex(Index(indexName!!, listOf(this), true))
        isUnique = true
    }

    /** Define a foreign key reference */
    fun references(
        column: Column<*>,
        onDelete: ReferentialAction = ReferentialAction.NO_ACTION,
        onUpdate: ReferentialAction = ReferentialAction.NO_ACTION
    ): Column<T> = apply {
        foreignKey = ForeignKeyReference(
            targetTable = column.table,
            targetColumn = column,
            onDelete = onDelete,
            onUpdate = onUpdate
        )
    }

    // ============== Expression Conversion ==============

    /** Convert this column to an expression for use in queries */
    fun asExpression(): ColumnExpression<T> = ColumnExpression(this)

    override fun toString(): String = qualifiedName

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Column<*>) return false
        return table.tableName == other.table.tableName && name == other.name
    }

    override fun hashCode(): Int {
        var result = table.tableName.hashCode()
        result = 31 * result + name.hashCode()
        return result
    }
}

/**
 * Represents a foreign key reference
 */
data class ForeignKeyReference(
    val targetTable: Table,
    val targetColumn: Column<*>,
    val onDelete: ReferentialAction = ReferentialAction.NO_ACTION,
    val onUpdate: ReferentialAction = ReferentialAction.NO_ACTION
)

/**
 * Referential actions for foreign key constraints
 */
enum class ReferentialAction(val sql: String) {
    NO_ACTION("NO ACTION"),
    RESTRICT("RESTRICT"),
    CASCADE("CASCADE"),
    SET_NULL("SET NULL"),
    SET_DEFAULT("SET DEFAULT")
}

/**
 * Represents a database index
 */
data class Index(
    val name: String,
    val columns: List<Column<*>>,
    val isUnique: Boolean = false
) {
    val tableName: String
        get() = columns.first().table.tableName

    val columnNames: List<String>
        get() = columns.map { it.name }
}
