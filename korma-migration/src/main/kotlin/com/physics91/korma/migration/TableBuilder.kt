package com.physics91.korma.migration

import com.physics91.korma.schema.*
import java.math.BigDecimal
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import java.util.UUID

/**
 * DSL builder for creating tables in migrations.
 */
class TableBuilder(private val tableName: String) {
    private val columnBuilders = mutableListOf<MigrationColumnBuilder<*>>()

    // ============== Integer Types ==============

    fun integer(name: String): MigrationColumnBuilder<Int> {
        return MigrationColumnBuilder<Int>(name, IntColumnType).also { columnBuilders.add(it) }
    }

    fun long(name: String): MigrationColumnBuilder<Long> {
        return MigrationColumnBuilder<Long>(name, LongColumnType).also { columnBuilders.add(it) }
    }

    fun smallint(name: String): MigrationColumnBuilder<Short> {
        return MigrationColumnBuilder<Short>(name, ShortColumnType).also { columnBuilders.add(it) }
    }

    // ============== String Types ==============

    fun varchar(name: String, length: Int): MigrationColumnBuilder<String> {
        return MigrationColumnBuilder<String>(name, VarcharColumnType(length)).also { columnBuilders.add(it) }
    }

    fun text(name: String): MigrationColumnBuilder<String> {
        return MigrationColumnBuilder<String>(name, TextColumnType).also { columnBuilders.add(it) }
    }

    fun char(name: String, length: Int): MigrationColumnBuilder<String> {
        return MigrationColumnBuilder<String>(name, CharColumnType(length)).also { columnBuilders.add(it) }
    }

    // ============== Numeric Types ==============

    fun decimal(name: String, precision: Int, scale: Int): MigrationColumnBuilder<BigDecimal> {
        return MigrationColumnBuilder<BigDecimal>(name, DecimalColumnType(precision, scale)).also { columnBuilders.add(it) }
    }

    fun double(name: String): MigrationColumnBuilder<Double> {
        return MigrationColumnBuilder<Double>(name, DoubleColumnType).also { columnBuilders.add(it) }
    }

    fun float(name: String): MigrationColumnBuilder<Float> {
        return MigrationColumnBuilder<Float>(name, FloatColumnType).also { columnBuilders.add(it) }
    }

    // ============== Boolean Type ==============

    fun boolean(name: String): MigrationColumnBuilder<Boolean> {
        return MigrationColumnBuilder<Boolean>(name, BooleanColumnType).also { columnBuilders.add(it) }
    }

    // ============== Date/Time Types ==============

    fun date(name: String): MigrationColumnBuilder<LocalDate> {
        return MigrationColumnBuilder<LocalDate>(name, DateColumnType).also { columnBuilders.add(it) }
    }

    fun time(name: String): MigrationColumnBuilder<LocalTime> {
        return MigrationColumnBuilder<LocalTime>(name, TimeColumnType).also { columnBuilders.add(it) }
    }

    fun timestamp(name: String): MigrationColumnBuilder<Instant> {
        return MigrationColumnBuilder<Instant>(name, TimestampColumnType).also { columnBuilders.add(it) }
    }

    fun datetime(name: String): MigrationColumnBuilder<LocalDateTime> {
        return MigrationColumnBuilder<LocalDateTime>(name, DateTimeColumnType).also { columnBuilders.add(it) }
    }

    // ============== Binary Types ==============

    fun blob(name: String): MigrationColumnBuilder<ByteArray> {
        return MigrationColumnBuilder<ByteArray>(name, BlobColumnType).also { columnBuilders.add(it) }
    }

    fun binary(name: String): MigrationColumnBuilder<ByteArray> {
        return MigrationColumnBuilder<ByteArray>(name, BinaryColumnType).also { columnBuilders.add(it) }
    }

    // ============== UUID Type ==============

    fun uuid(name: String): MigrationColumnBuilder<UUID> {
        return MigrationColumnBuilder<UUID>(name, UUIDColumnType).also { columnBuilders.add(it) }
    }

    /**
     * Build the MigrationTableInfo from the builder configuration.
     */
    fun build(): MigrationTableInfo {
        return MigrationTableInfo(tableName, columnBuilders.map { it.build() })
    }
}

/**
 * Fluent DSL for building migration columns with chained modifiers.
 */
class MigrationColumnBuilder<T : Any>(
    private val name: String,
    private val type: ColumnType<T>
) {
    private var isPrimaryKey = false
    private var isAutoIncrement = false
    private var isNullable = false
    private var isUnique = false
    private var defaultValue: Any? = null

    fun primaryKey(): MigrationColumnBuilder<T> = apply {
        isPrimaryKey = true
    }

    fun autoIncrement(): MigrationColumnBuilder<T> = apply {
        isAutoIncrement = true
        isPrimaryKey = true
    }

    fun nullable(): MigrationColumnBuilder<T> = apply {
        isNullable = true
    }

    fun notNull(): MigrationColumnBuilder<T> = apply {
        isNullable = false
    }

    fun unique(): MigrationColumnBuilder<T> = apply {
        isUnique = true
    }

    fun default(value: T): MigrationColumnBuilder<T> = apply {
        defaultValue = value
    }

    /**
     * Set a raw SQL default expression (e.g., CURRENT_TIMESTAMP).
     */
    fun defaultExpression(sql: String): MigrationColumnBuilder<T> = apply {
        defaultValue = SqlDefault(sql)
    }

    /**
     * Build and return the migration column.
     */
    internal fun build(): MigrationColumn<T> {
        return MigrationColumn(
            name = name,
            type = type,
            isPrimaryKey = isPrimaryKey,
            isAutoIncrement = isAutoIncrement,
            isNullable = isNullable,
            isUnique = isUnique,
            defaultValue = defaultValue
        )
    }
}
