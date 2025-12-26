package com.physics91.korma.migration

import com.physics91.korma.schema.*

/**
 * DSL builder for creating a single column in migrations.
 * Used primarily for addColumn operations.
 */
class ColumnBuilder(private val name: String) {
    private var type: ColumnType<*>? = null
    private var isPrimaryKey = false
    private var isAutoIncrement = false
    private var isNullable = false
    private var isUnique = false
    private var defaultValue: Any? = null

    // ============== Type Setters ==============

    fun integer(): ColumnBuilder = apply {
        type = IntColumnType
    }

    fun long(): ColumnBuilder = apply {
        type = LongColumnType
    }

    fun smallint(): ColumnBuilder = apply {
        type = ShortColumnType
    }

    fun varchar(length: Int): ColumnBuilder = apply {
        type = VarcharColumnType(length)
    }

    fun text(): ColumnBuilder = apply {
        type = TextColumnType
    }

    fun char(length: Int): ColumnBuilder = apply {
        type = CharColumnType(length)
    }

    fun decimal(precision: Int, scale: Int): ColumnBuilder = apply {
        type = DecimalColumnType(precision, scale)
    }

    fun double(): ColumnBuilder = apply {
        type = DoubleColumnType
    }

    fun float(): ColumnBuilder = apply {
        type = FloatColumnType
    }

    fun boolean(): ColumnBuilder = apply {
        type = BooleanColumnType
    }

    fun date(): ColumnBuilder = apply {
        type = DateColumnType
    }

    fun time(): ColumnBuilder = apply {
        type = TimeColumnType
    }

    fun timestamp(): ColumnBuilder = apply {
        type = TimestampColumnType
    }

    fun datetime(): ColumnBuilder = apply {
        type = DateTimeColumnType
    }

    fun blob(): ColumnBuilder = apply {
        type = BlobColumnType
    }

    fun binary(): ColumnBuilder = apply {
        type = BinaryColumnType
    }

    fun uuid(): ColumnBuilder = apply {
        type = UUIDColumnType
    }

    // ============== Modifiers ==============

    fun primaryKey(): ColumnBuilder = apply {
        isPrimaryKey = true
    }

    fun autoIncrement(): ColumnBuilder = apply {
        isAutoIncrement = true
    }

    fun nullable(): ColumnBuilder = apply {
        isNullable = true
    }

    fun notNull(): ColumnBuilder = apply {
        isNullable = false
    }

    fun unique(): ColumnBuilder = apply {
        isUnique = true
    }

    fun default(value: Any): ColumnBuilder = apply {
        defaultValue = value
    }

    /**
     * Set a raw SQL default expression (e.g., CURRENT_TIMESTAMP).
     */
    fun defaultExpression(sql: String): ColumnBuilder = apply {
        defaultValue = SqlDefault(sql)
    }

    // ============== Build ==============

    @Suppress("UNCHECKED_CAST")
    fun build(): MigrationColumn<*> {
        val columnType = type ?: throw IllegalStateException("Column type must be specified for column '$name'")

        return MigrationColumn(
            name = name,
            type = columnType as ColumnType<Any>,
            isPrimaryKey = isPrimaryKey,
            isAutoIncrement = isAutoIncrement,
            isNullable = isNullable,
            isUnique = isUnique,
            defaultValue = defaultValue
        )
    }
}
