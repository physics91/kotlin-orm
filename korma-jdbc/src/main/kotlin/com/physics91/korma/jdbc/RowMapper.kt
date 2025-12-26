package com.physics91.korma.jdbc

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import java.sql.ResultSet

/**
 * Maps a ResultSet row to a type T.
 */
fun interface RowMapper<T> {
    fun map(rs: ResultSet): T
}

/**
 * Result set row accessor.
 *
 * Provides type-safe access to column values.
 */
class Row(val rs: ResultSet) {

    /**
     * Get a column value by column definition.
     */
    operator fun <T> get(column: Column<T>): T? {
        val value = rs.getObject(column.name)
        return if (rs.wasNull()) null else column.type.fromDb(value)
    }

    /**
     * Get a column value by name.
     */
    fun <T> get(columnName: String, clazz: Class<T>): T? {
        val value = rs.getObject(columnName)
        return if (rs.wasNull()) null else clazz.cast(value)
    }

    /**
     * Get a column value by index (1-based).
     */
    fun <T> get(index: Int, clazz: Class<T>): T? {
        val value = rs.getObject(index)
        return if (rs.wasNull()) null else clazz.cast(value)
    }

    /**
     * Get a non-null column value.
     * Throws if the value is null.
     */
    fun <T> require(column: Column<T>): T {
        return get(column) ?: throw IllegalStateException("Column ${column.name} is null")
    }

    // ============== Primitive Accessors ==============

    fun getInt(column: Column<Int>): Int = rs.getInt(column.name)
    fun getLong(column: Column<Long>): Long = rs.getLong(column.name)
    fun getString(column: Column<String>): String? = rs.getString(column.name)
    fun getBoolean(column: Column<Boolean>): Boolean = rs.getBoolean(column.name)
    fun getDouble(column: Column<Double>): Double = rs.getDouble(column.name)
    fun getFloat(column: Column<Float>): Float = rs.getFloat(column.name)
    fun getShort(column: Column<Short>): Short = rs.getShort(column.name)

    fun getInt(columnName: String): Int = rs.getInt(columnName)
    fun getInt(columnIndex: Int): Int = rs.getInt(columnIndex)
    fun getLong(columnName: String): Long = rs.getLong(columnName)
    fun getLong(columnIndex: Int): Long = rs.getLong(columnIndex)
    fun getString(columnName: String): String? = rs.getString(columnName)
    fun getBoolean(columnName: String): Boolean = rs.getBoolean(columnName)
    fun getDouble(columnName: String): Double = rs.getDouble(columnName)

    // ============== Date/Time Accessors ==============

    fun getTimestamp(columnName: String): java.sql.Timestamp? = rs.getTimestamp(columnName)
    fun getDate(columnName: String): java.sql.Date? = rs.getDate(columnName)
    fun getTime(columnName: String): java.sql.Time? = rs.getTime(columnName)

    // ============== Binary Accessors ==============

    fun getBytes(columnName: String): ByteArray? = rs.getBytes(columnName)
    fun getBlob(columnName: String): java.sql.Blob? = rs.getBlob(columnName)

    // ============== Check for null ==============

    fun isNull(column: Column<*>): Boolean {
        rs.getObject(column.name)
        return rs.wasNull()
    }

    fun isNull(columnName: String): Boolean {
        rs.getObject(columnName)
        return rs.wasNull()
    }
}

/**
 * Extension to create a Row from ResultSet.
 */
fun ResultSet.toRow(): Row = Row(this)

/**
 * Map ResultSet to a list using a RowMapper.
 */
fun <T> ResultSet.mapTo(mapper: RowMapper<T>): List<T> {
    val results = mutableListOf<T>()
    while (next()) {
        results.add(mapper.map(this))
    }
    return results
}

/**
 * Map ResultSet to a list using a Row accessor.
 */
fun <T> ResultSet.map(mapper: (Row) -> T): List<T> {
    val results = mutableListOf<T>()
    while (next()) {
        results.add(mapper(Row(this)))
    }
    return results
}

/**
 * Map ResultSet to a sequence for lazy processing.
 */
fun <T> ResultSet.asSequence(mapper: (Row) -> T): Sequence<T> = sequence {
    while (next()) {
        yield(mapper(Row(this@asSequence)))
    }
}

/**
 * Get the first row or null.
 */
fun <T> ResultSet.firstOrNull(mapper: (Row) -> T): T? {
    return if (next()) mapper(Row(this)) else null
}

/**
 * Get the first row or throw.
 */
fun <T> ResultSet.first(mapper: (Row) -> T): T {
    return firstOrNull(mapper) ?: throw NoSuchElementException("ResultSet is empty")
}

/**
 * Get exactly one row or throw.
 */
fun <T> ResultSet.single(mapper: (Row) -> T): T {
    if (!next()) throw NoSuchElementException("ResultSet is empty")
    val result = mapper(Row(this))
    if (next()) throw IllegalStateException("ResultSet has more than one row")
    return result
}

/**
 * Get exactly one row or null if empty.
 * Throws if more than one row exists.
 */
fun <T> ResultSet.singleOrNull(mapper: (Row) -> T): T? {
    if (!next()) return null
    val result = mapper(Row(this))
    if (next()) throw IllegalStateException("ResultSet has more than one row")
    return result
}
