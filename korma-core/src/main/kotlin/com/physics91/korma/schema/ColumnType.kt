package com.physics91.korma.schema

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.util.UUID

/**
 * Base interface for SQL column types.
 * Handles conversion between Kotlin types and SQL types.
 *
 * @param T The Kotlin type this column type represents
 */
abstract class ColumnType<T> {
    /** The JDBC SQL type constant from java.sql.Types */
    abstract val jdbcType: Int

    /** Whether this type allows NULL values */
    open val nullable: Boolean = false

    /** SQL type name for DDL generation (dialect-specific) */
    abstract fun sqlType(): String

    /** Convert a Kotlin value to database representation */
    abstract fun toDb(value: T): Any?

    /** Convert a database value to Kotlin representation */
    abstract fun fromDb(value: Any?): T?

    /** Set a parameter on a PreparedStatement */
    open fun setParameter(ps: PreparedStatement, index: Int, value: T?) {
        if (value == null) {
            ps.setNull(index, jdbcType)
        } else {
            ps.setObject(index, toDb(value), jdbcType)
        }
    }

    /** Get a value from a ResultSet */
    open fun getValue(rs: ResultSet, index: Int): T? {
        val value = rs.getObject(index)
        return if (rs.wasNull()) null else fromDb(value)
    }

    /** Get a value from a ResultSet by column name */
    open fun getValue(rs: ResultSet, columnName: String): T? {
        val value = rs.getObject(columnName)
        return if (rs.wasNull()) null else fromDb(value)
    }

    /** Create a nullable version of this type */
    open fun asNullable(): ColumnType<T?> = NullableColumnType(this)
}

/**
 * Wrapper for nullable column types
 */
class NullableColumnType<T>(
    /** The underlying non-nullable column type */
    val delegate: ColumnType<T>
) : ColumnType<T?>() {
    override val jdbcType: Int = delegate.jdbcType
    override val nullable: Boolean = true

    override fun sqlType(): String = delegate.sqlType()
    override fun toDb(value: T?): Any? = value?.let { delegate.toDb(it) }
    override fun fromDb(value: Any?): T? = value?.let { delegate.fromDb(it) }

    override fun asNullable(): ColumnType<T?> = this
}

// ============== Primitive Types ==============

/** Integer column type (INT) */
object IntColumnType : ColumnType<Int>() {
    override val jdbcType: Int = Types.INTEGER
    override fun sqlType(): String = "INT"
    override fun toDb(value: Int): Any = value
    override fun fromDb(value: Any?): Int? = when (value) {
        is Int -> value
        is Number -> value.toInt()
        else -> null
    }
}

/** Long column type (BIGINT) */
object LongColumnType : ColumnType<Long>() {
    override val jdbcType: Int = Types.BIGINT
    override fun sqlType(): String = "BIGINT"
    override fun toDb(value: Long): Any = value
    override fun fromDb(value: Any?): Long? = when (value) {
        is Long -> value
        is Number -> value.toLong()
        else -> null
    }
}

/** Short column type (SMALLINT) */
object ShortColumnType : ColumnType<Short>() {
    override val jdbcType: Int = Types.SMALLINT
    override fun sqlType(): String = "SMALLINT"
    override fun toDb(value: Short): Any = value
    override fun fromDb(value: Any?): Short? = when (value) {
        is Short -> value
        is Number -> value.toShort()
        else -> null
    }
}

/** Float column type (REAL) */
object FloatColumnType : ColumnType<Float>() {
    override val jdbcType: Int = Types.REAL
    override fun sqlType(): String = "REAL"
    override fun toDb(value: Float): Any = value
    override fun fromDb(value: Any?): Float? = when (value) {
        is Float -> value
        is Number -> value.toFloat()
        else -> null
    }
}

/** Double column type (DOUBLE PRECISION) */
object DoubleColumnType : ColumnType<Double>() {
    override val jdbcType: Int = Types.DOUBLE
    override fun sqlType(): String = "DOUBLE PRECISION"
    override fun toDb(value: Double): Any = value
    override fun fromDb(value: Any?): Double? = when (value) {
        is Double -> value
        is Number -> value.toDouble()
        else -> null
    }
}

/** Boolean column type (BOOLEAN) */
object BooleanColumnType : ColumnType<Boolean>() {
    override val jdbcType: Int = Types.BOOLEAN
    override fun sqlType(): String = "BOOLEAN"
    override fun toDb(value: Boolean): Any = value
    override fun fromDb(value: Any?): Boolean? = when (value) {
        is Boolean -> value
        is Number -> value.toInt() != 0
        else -> null
    }
}

// ============== String Types ==============

/** Variable-length character type (VARCHAR) */
class VarcharColumnType(val length: Int) : ColumnType<String>() {
    override val jdbcType: Int = Types.VARCHAR
    override fun sqlType(): String = "VARCHAR($length)"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/** Fixed-length character type (CHAR) */
class CharColumnType(val length: Int) : ColumnType<String>() {
    override val jdbcType: Int = Types.CHAR
    override fun sqlType(): String = "CHAR($length)"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

/** Unlimited text type (TEXT) */
object TextColumnType : ColumnType<String>() {
    override val jdbcType: Int = Types.LONGVARCHAR
    override fun sqlType(): String = "TEXT"
    override fun toDb(value: String): Any = value
    override fun fromDb(value: Any?): String? = value?.toString()
}

// ============== Numeric Types ==============

/** Decimal type with precision and scale */
class DecimalColumnType(
    val precision: Int,
    val scale: Int
) : ColumnType<BigDecimal>() {
    override val jdbcType: Int = Types.DECIMAL
    override fun sqlType(): String = "DECIMAL($precision, $scale)"
    override fun toDb(value: BigDecimal): Any = value
    override fun fromDb(value: Any?): BigDecimal? = when (value) {
        is BigDecimal -> value
        is Number -> BigDecimal(value.toString())
        is String -> BigDecimal(value)
        else -> null
    }
}

// ============== Date/Time Types ==============

/** Timestamp with timezone (kotlinx-datetime Instant) */
object TimestampColumnType : ColumnType<Instant>() {
    override val jdbcType: Int = Types.TIMESTAMP_WITH_TIMEZONE
    override fun sqlType(): String = "TIMESTAMP WITH TIME ZONE"
    override fun toDb(value: Instant): Any = java.sql.Timestamp.from(java.time.Instant.parse(value.toString()))
    override fun fromDb(value: Any?): Instant? = when (value) {
        is java.sql.Timestamp -> Instant.fromEpochMilliseconds(value.time)
        is java.time.Instant -> Instant.fromEpochMilliseconds(value.toEpochMilli())
        else -> null
    }
}

/** Date without time (kotlinx-datetime LocalDate) */
object DateColumnType : ColumnType<LocalDate>() {
    override val jdbcType: Int = Types.DATE
    override fun sqlType(): String = "DATE"
    override fun toDb(value: LocalDate): Any = java.sql.Date.valueOf(value.toString())
    override fun fromDb(value: Any?): LocalDate? = when (value) {
        is java.sql.Date -> LocalDate.parse(value.toString())
        is java.time.LocalDate -> LocalDate.parse(value.toString())
        else -> null
    }
}

/** Time without date (kotlinx-datetime LocalTime) */
object TimeColumnType : ColumnType<LocalTime>() {
    override val jdbcType: Int = Types.TIME
    override fun sqlType(): String = "TIME"
    override fun toDb(value: LocalTime): Any = java.sql.Time.valueOf(value.toString())
    override fun fromDb(value: Any?): LocalTime? = when (value) {
        is java.sql.Time -> LocalTime.parse(value.toString())
        is java.time.LocalTime -> LocalTime.parse(value.toString())
        else -> null
    }
}

/** DateTime without timezone (kotlinx-datetime LocalDateTime) */
object DateTimeColumnType : ColumnType<LocalDateTime>() {
    override val jdbcType: Int = Types.TIMESTAMP
    override fun sqlType(): String = "TIMESTAMP"
    override fun toDb(value: LocalDateTime): Any = java.sql.Timestamp.valueOf(value.toString().replace('T', ' '))
    override fun fromDb(value: Any?): LocalDateTime? = when (value) {
        is java.sql.Timestamp -> LocalDateTime.parse(value.toString().replace(' ', 'T').substringBefore('.'))
        is java.time.LocalDateTime -> LocalDateTime.parse(value.toString())
        else -> null
    }
}

// ============== Binary Types ==============

/** Binary data type (BYTEA/BLOB) */
object BinaryColumnType : ColumnType<ByteArray>() {
    override val jdbcType: Int = Types.BINARY
    override fun sqlType(): String = "BYTEA"
    override fun toDb(value: ByteArray): Any = value
    override fun fromDb(value: Any?): ByteArray? = when (value) {
        is ByteArray -> value
        else -> null
    }
}

/** Large binary object (BLOB) */
object BlobColumnType : ColumnType<ByteArray>() {
    override val jdbcType: Int = Types.BLOB
    override fun sqlType(): String = "BLOB"
    override fun toDb(value: ByteArray): Any = value
    override fun fromDb(value: Any?): ByteArray? = when (value) {
        is ByteArray -> value
        is java.sql.Blob -> value.getBytes(1, value.length().toInt())
        else -> null
    }
}

// ============== UUID Type ==============

/** UUID type */
object UUIDColumnType : ColumnType<UUID>() {
    override val jdbcType: Int = Types.OTHER
    override fun sqlType(): String = "UUID"
    override fun toDb(value: UUID): Any = value
    override fun fromDb(value: Any?): UUID? = when (value) {
        is UUID -> value
        is String -> UUID.fromString(value)
        else -> null
    }
}

// ============== Enum Type ==============

/** Enum stored as VARCHAR */
class EnumColumnType<T : Enum<T>>(
    private val enumClass: Class<T>
) : ColumnType<T>() {
    override val jdbcType: Int = Types.VARCHAR
    override fun sqlType(): String = "VARCHAR(255)"
    override fun toDb(value: T): Any = value.name
    override fun fromDb(value: Any?): T? {
        val name = value?.toString() ?: return null
        return enumClass.enumConstants.firstOrNull { it.name == name }
    }
}

/** Enum stored as ordinal (INT) */
class EnumOrdinalColumnType<T : Enum<T>>(
    private val enumClass: Class<T>
) : ColumnType<T>() {
    override val jdbcType: Int = Types.INTEGER
    override fun sqlType(): String = "INT"
    override fun toDb(value: T): Any = value.ordinal
    override fun fromDb(value: Any?): T? {
        val ordinal = when (value) {
            is Number -> value.toInt()
            else -> return null
        }
        val constants = enumClass.enumConstants
        return if (ordinal in constants.indices) constants[ordinal] else null
    }
}
