package com.physics91.korma.schema

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import org.junit.jupiter.api.Test
import java.sql.Types
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ColumnTypeTest {

    enum class TestStatus { ACTIVE, INACTIVE, PENDING }

    // ============== EnumColumnType Tests ==============

    @Test
    fun `EnumColumnType jdbcType is VARCHAR`() {
        val type = EnumColumnType(TestStatus::class.java)
        assertEquals(Types.VARCHAR, type.jdbcType)
    }

    @Test
    fun `EnumColumnType sqlType returns VARCHAR`() {
        val type = EnumColumnType(TestStatus::class.java)
        assertEquals("VARCHAR(255)", type.sqlType())
    }

    @Test
    fun `EnumColumnType toDb returns enum name`() {
        val type = EnumColumnType(TestStatus::class.java)
        assertEquals("ACTIVE", type.toDb(TestStatus.ACTIVE))
        assertEquals("PENDING", type.toDb(TestStatus.PENDING))
    }

    @Test
    fun `EnumColumnType fromDb parses enum name`() {
        val type = EnumColumnType(TestStatus::class.java)
        assertEquals(TestStatus.ACTIVE, type.fromDb("ACTIVE"))
        assertEquals(TestStatus.INACTIVE, type.fromDb("INACTIVE"))
    }

    @Test
    fun `EnumColumnType fromDb returns null for unknown name`() {
        val type = EnumColumnType(TestStatus::class.java)
        assertNull(type.fromDb("UNKNOWN"))
    }

    @Test
    fun `EnumColumnType fromDb returns null for null input`() {
        val type = EnumColumnType(TestStatus::class.java)
        assertNull(type.fromDb(null))
    }

    // ============== EnumOrdinalColumnType Tests ==============

    @Test
    fun `EnumOrdinalColumnType jdbcType is INTEGER`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertEquals(Types.INTEGER, type.jdbcType)
    }

    @Test
    fun `EnumOrdinalColumnType sqlType returns INT`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertEquals("INT", type.sqlType())
    }

    @Test
    fun `EnumOrdinalColumnType toDb returns ordinal`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertEquals(0, type.toDb(TestStatus.ACTIVE))
        assertEquals(1, type.toDb(TestStatus.INACTIVE))
        assertEquals(2, type.toDb(TestStatus.PENDING))
    }

    @Test
    fun `EnumOrdinalColumnType fromDb parses ordinal`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertEquals(TestStatus.ACTIVE, type.fromDb(0))
        assertEquals(TestStatus.INACTIVE, type.fromDb(1))
        assertEquals(TestStatus.PENDING, type.fromDb(2))
    }

    @Test
    fun `EnumOrdinalColumnType fromDb handles Number types`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertEquals(TestStatus.ACTIVE, type.fromDb(0L))
        assertEquals(TestStatus.INACTIVE, type.fromDb(1.0))
    }

    @Test
    fun `EnumOrdinalColumnType fromDb returns null for invalid ordinal`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertNull(type.fromDb(100))
        assertNull(type.fromDb(-1))
    }

    @Test
    fun `EnumOrdinalColumnType fromDb returns null for non-number`() {
        val type = EnumOrdinalColumnType(TestStatus::class.java)
        assertNull(type.fromDb("string"))
    }

    // ============== DateTimeColumnType Tests ==============

    @Test
    fun `DateTimeColumnType jdbcType is TIMESTAMP`() {
        assertEquals(Types.TIMESTAMP, DateTimeColumnType.jdbcType)
    }

    @Test
    fun `DateTimeColumnType sqlType returns TIMESTAMP`() {
        assertEquals("TIMESTAMP", DateTimeColumnType.sqlType())
    }

    @Test
    fun `DateTimeColumnType toDb converts LocalDateTime`() {
        val dt = LocalDateTime(2024, 1, 15, 10, 30, 45)
        val result = DateTimeColumnType.toDb(dt)
        assertTrue(result is java.sql.Timestamp)
    }

    @Test
    fun `DateTimeColumnType fromDb parses Timestamp`() {
        val timestamp = java.sql.Timestamp.valueOf("2024-01-15 10:30:45")
        val result = DateTimeColumnType.fromDb(timestamp)
        assertEquals(2024, result?.year)
        assertEquals(1, result?.monthNumber)
        assertEquals(15, result?.dayOfMonth)
    }

    @Test
    fun `DateTimeColumnType fromDb parses java LocalDateTime`() {
        val javaDateTime = java.time.LocalDateTime.of(2024, 1, 15, 10, 30, 45)
        val result = DateTimeColumnType.fromDb(javaDateTime)
        assertEquals(2024, result?.year)
    }

    @Test
    fun `DateTimeColumnType fromDb returns null for invalid type`() {
        assertNull(DateTimeColumnType.fromDb("invalid"))
        assertNull(DateTimeColumnType.fromDb(123))
    }

    // ============== DateColumnType Tests ==============

    @Test
    fun `DateColumnType jdbcType is DATE`() {
        assertEquals(Types.DATE, DateColumnType.jdbcType)
    }

    @Test
    fun `DateColumnType toDb converts LocalDate`() {
        val date = LocalDate(2024, 1, 15)
        val result = DateColumnType.toDb(date)
        assertTrue(result is java.sql.Date)
    }

    @Test
    fun `DateColumnType fromDb parses Date`() {
        val sqlDate = java.sql.Date.valueOf("2024-01-15")
        val result = DateColumnType.fromDb(sqlDate)
        assertEquals(LocalDate(2024, 1, 15), result)
    }

    @Test
    fun `DateColumnType fromDb parses java LocalDate`() {
        val javaDate = java.time.LocalDate.of(2024, 1, 15)
        val result = DateColumnType.fromDb(javaDate)
        assertEquals(LocalDate(2024, 1, 15), result)
    }

    // ============== TimeColumnType Tests ==============

    @Test
    fun `TimeColumnType jdbcType is TIME`() {
        assertEquals(Types.TIME, TimeColumnType.jdbcType)
    }

    @Test
    fun `TimeColumnType toDb converts LocalTime`() {
        val time = LocalTime(10, 30, 45)
        val result = TimeColumnType.toDb(time)
        assertTrue(result is java.sql.Time)
    }

    @Test
    fun `TimeColumnType fromDb parses Time`() {
        val sqlTime = java.sql.Time.valueOf("10:30:45")
        val result = TimeColumnType.fromDb(sqlTime)
        assertEquals(10, result?.hour)
        assertEquals(30, result?.minute)
    }

    @Test
    fun `TimeColumnType fromDb parses java LocalTime`() {
        val javaTime = java.time.LocalTime.of(10, 30, 45)
        val result = TimeColumnType.fromDb(javaTime)
        assertEquals(10, result?.hour)
    }

    // ============== NullableColumnType Tests ==============

    @Test
    fun `NullableColumnType wraps delegate`() {
        val nullable = NullableColumnType(IntColumnType)

        assertTrue(nullable.nullable)
        assertEquals(Types.INTEGER, nullable.jdbcType)
        assertEquals("INT", nullable.sqlType())
    }

    @Test
    fun `NullableColumnType toDb handles null`() {
        val nullable = NullableColumnType(IntColumnType)

        assertNull(nullable.toDb(null))
        assertEquals(42, nullable.toDb(42))
    }

    @Test
    fun `NullableColumnType fromDb handles null`() {
        val nullable = NullableColumnType(IntColumnType)

        assertNull(nullable.fromDb(null))
        assertEquals(42, nullable.fromDb(42))
    }

    @Test
    fun `NullableColumnType asNullable returns self`() {
        val nullable = NullableColumnType(IntColumnType)
        assertEquals(nullable, nullable.asNullable())
    }

    // ============== Primitive ColumnType Tests ==============

    @Test
    fun `IntColumnType fromDb handles Number types`() {
        assertEquals(42, IntColumnType.fromDb(42))
        assertEquals(42, IntColumnType.fromDb(42L))
        assertEquals(42, IntColumnType.fromDb(42.0))
        assertNull(IntColumnType.fromDb("not a number"))
    }

    @Test
    fun `LongColumnType fromDb handles Number types`() {
        assertEquals(42L, LongColumnType.fromDb(42L))
        assertEquals(42L, LongColumnType.fromDb(42))
        assertEquals(42L, LongColumnType.fromDb(42.0))
        assertNull(LongColumnType.fromDb("not a number"))
    }

    @Test
    fun `BooleanColumnType fromDb handles Number types`() {
        assertEquals(true, BooleanColumnType.fromDb(true))
        assertEquals(true, BooleanColumnType.fromDb(1))
        assertEquals(false, BooleanColumnType.fromDb(0))
        assertNull(BooleanColumnType.fromDb("not a boolean"))
    }

    @Test
    fun `DecimalColumnType fromDb handles various types`() {
        val type = DecimalColumnType(10, 2)
        assertEquals(java.math.BigDecimal("42.50"), type.fromDb(java.math.BigDecimal("42.50")))
        assertEquals(java.math.BigDecimal("42"), type.fromDb(42))
        assertEquals(java.math.BigDecimal("42.5"), type.fromDb("42.5"))
        assertNull(type.fromDb(listOf(1, 2, 3)))
    }
}
