package com.physics91.korma.extensions

import com.physics91.korma.dsl.eq
import com.physics91.korma.dsl.gte
import com.physics91.korma.dsl.lt
import com.physics91.korma.dsl.lte
import com.physics91.korma.expression.BetweenPredicate
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.ComparisonPredicate
import com.physics91.korma.schema.Column
import kotlinx.datetime.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds

/**
 * Kotlin-native date/time extensions for Korma.
 *
 * Uses kotlinx-datetime types for cross-platform compatibility.
 * Provides idiomatic Kotlin operations for temporal columns.
 */

// ============== LocalDate Column Extensions ==============

/**
 * Creates a predicate matching today's date.
 */
@JvmName("localDateIsToday")
fun Column<LocalDate>.isToday(): ComparisonPredicate<LocalDate> =
    this eq Clock.System.todayIn(TimeZone.currentSystemDefault())

/**
 * Creates a predicate matching yesterday's date.
 */
fun Column<LocalDate>.isYesterday(): ComparisonPredicate<LocalDate> {
    val yesterday = Clock.System.todayIn(TimeZone.currentSystemDefault()).minus(1, DateTimeUnit.DAY)
    return this eq yesterday
}

/**
 * Creates a predicate matching tomorrow's date.
 */
fun Column<LocalDate>.isTomorrow(): ComparisonPredicate<LocalDate> {
    val tomorrow = Clock.System.todayIn(TimeZone.currentSystemDefault()).plus(1, DateTimeUnit.DAY)
    return this eq tomorrow
}

/**
 * Creates a BETWEEN predicate for the current week (Monday to Sunday).
 */
fun Column<LocalDate>.isThisWeek(): BetweenPredicate<LocalDate> {
    val today = Clock.System.todayIn(TimeZone.currentSystemDefault())
    val dayOfWeek = today.dayOfWeek
    val daysFromMonday = dayOfWeek.ordinal
    val startOfWeek = today.minus(daysFromMonday, DateTimeUnit.DAY)
    val endOfWeek = startOfWeek.plus(6, DateTimeUnit.DAY)
    return ColumnExpression(this).between(startOfWeek, endOfWeek)
}

/**
 * Creates a BETWEEN predicate for the current month.
 */
@JvmName("localDateIsThisMonth")
fun Column<LocalDate>.isThisMonth(): BetweenPredicate<LocalDate> {
    val today = Clock.System.todayIn(TimeZone.currentSystemDefault())
    val startOfMonth = LocalDate(today.year, today.month, 1)
    val endOfMonth = LocalDate(today.year, today.month, today.month.length(isLeapYear(today.year)))
    return ColumnExpression(this).between(startOfMonth, endOfMonth)
}

/**
 * Creates a BETWEEN predicate for the last N days.
 */
fun Column<LocalDate>.isInLastDays(days: Long): BetweenPredicate<LocalDate> {
    val today = Clock.System.todayIn(TimeZone.currentSystemDefault())
    val startDate = today.minus(days.toInt(), DateTimeUnit.DAY)
    return ColumnExpression(this).between(startDate, today)
}

/**
 * Creates a BETWEEN predicate for the current year.
 */
fun Column<LocalDate>.isThisYear(): BetweenPredicate<LocalDate> {
    val today = Clock.System.todayIn(TimeZone.currentSystemDefault())
    val startOfYear = LocalDate(today.year, Month.JANUARY, 1)
    val endOfYear = LocalDate(today.year, Month.DECEMBER, 31)
    return ColumnExpression(this).between(startOfYear, endOfYear)
}

/**
 * Creates a predicate for dates before today.
 */
@JvmName("localDateIsPast")
fun Column<LocalDate>.isPast(): ComparisonPredicate<LocalDate> =
    this lt Clock.System.todayIn(TimeZone.currentSystemDefault())

/**
 * Creates a predicate for dates after today.
 */
@JvmName("localDateIsFuture")
fun Column<LocalDate>.isFuture(): ComparisonPredicate<LocalDate> {
    val tomorrow = Clock.System.todayIn(TimeZone.currentSystemDefault()).plus(1, DateTimeUnit.DAY)
    return this gte tomorrow
}

/**
 * Creates a predicate for dates in a specific month of a year.
 */
fun Column<LocalDate>.isInMonth(year: Int, month: Month): BetweenPredicate<LocalDate> {
    val startOfMonth = LocalDate(year, month, 1)
    val endOfMonth = LocalDate(year, month, month.length(isLeapYear(year)))
    return ColumnExpression(this).between(startOfMonth, endOfMonth)
}

// ============== LocalDateTime Column Extensions ==============

/**
 * Creates a BETWEEN predicate for today (from midnight to end of day).
 */
@JvmName("localDateTimeIsToday")
fun Column<LocalDateTime>.isToday(): BetweenPredicate<LocalDateTime> {
    val today = Clock.System.todayIn(TimeZone.currentSystemDefault())
    val startOfDay = today.atStartOfDayIn(TimeZone.currentSystemDefault())
        .toLocalDateTime(TimeZone.currentSystemDefault())
    val tomorrow = today.plus(1, DateTimeUnit.DAY)
    val endOfDay = tomorrow.atStartOfDayIn(TimeZone.currentSystemDefault())
        .minus(1.nanoseconds)
        .toLocalDateTime(TimeZone.currentSystemDefault())
    return ColumnExpression(this).between(startOfDay, endOfDay)
}

/**
 * Creates a BETWEEN predicate for the last N hours.
 */
@JvmName("localDateTimeIsInLastHours")
fun Column<LocalDateTime>.isInLastHours(hours: Long): BetweenPredicate<LocalDateTime> {
    val tz = TimeZone.currentSystemDefault()
    val now = Clock.System.now().toLocalDateTime(tz)
    val startTime = Clock.System.now().minus(hours.hours).toLocalDateTime(tz)
    return ColumnExpression(this).between(startTime, now)
}

/**
 * Creates a BETWEEN predicate for the last N minutes.
 */
fun Column<LocalDateTime>.isInLastMinutes(minutes: Long): BetweenPredicate<LocalDateTime> {
    val tz = TimeZone.currentSystemDefault()
    val now = Clock.System.now().toLocalDateTime(tz)
    val startTime = Clock.System.now().minus(minutes.minutes).toLocalDateTime(tz)
    return ColumnExpression(this).between(startTime, now)
}

/**
 * Creates a predicate for timestamps in a specific Duration.
 */
@JvmName("localDateTimeIsInLast")
fun Column<LocalDateTime>.isInLast(duration: Duration): BetweenPredicate<LocalDateTime> {
    val tz = TimeZone.currentSystemDefault()
    val now = Clock.System.now().toLocalDateTime(tz)
    val startTime = Clock.System.now().minus(duration).toLocalDateTime(tz)
    return ColumnExpression(this).between(startTime, now)
}

/**
 * Creates a predicate for datetimes before now.
 */
@JvmName("localDateTimeIsPast")
fun Column<LocalDateTime>.isPast(): ComparisonPredicate<LocalDateTime> {
    val now = Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault())
    return this lt now
}

/**
 * Creates a predicate for datetimes after now.
 */
@JvmName("localDateTimeIsFuture")
fun Column<LocalDateTime>.isFuture(): ComparisonPredicate<LocalDateTime> {
    val now = Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault())
    return this gte now
}

// ============== Instant Column Extensions ==============

/**
 * Creates a BETWEEN predicate for today (UTC).
 */
fun Column<Instant>.isTodayUtc(): BetweenPredicate<Instant> {
    val today = Clock.System.todayIn(TimeZone.UTC)
    val startOfDay = today.atStartOfDayIn(TimeZone.UTC)
    val endOfDay = today.plus(1, DateTimeUnit.DAY).atStartOfDayIn(TimeZone.UTC).minus(1.nanoseconds)
    return ColumnExpression(this).between(startOfDay, endOfDay)
}

/**
 * Creates a BETWEEN predicate for the last N hours from now.
 */
@JvmName("instantIsInLastHours")
fun Column<Instant>.isInLastHours(hours: Long): BetweenPredicate<Instant> {
    val now = Clock.System.now()
    val startTime = now.minus(hours.hours)
    return ColumnExpression(this).between(startTime, now)
}

/**
 * Creates a predicate for instants in a specific Duration.
 */
@JvmName("instantIsInLast")
fun Column<Instant>.isInLast(duration: Duration): BetweenPredicate<Instant> {
    val now = Clock.System.now()
    val startTime = now.minus(duration)
    return ColumnExpression(this).between(startTime, now)
}

/**
 * Creates a predicate for timestamps before now.
 */
@JvmName("instantIsPast")
fun Column<Instant>.isPast(): ComparisonPredicate<Instant> =
    this lt Clock.System.now()

/**
 * Creates a predicate for timestamps after now.
 */
@JvmName("instantIsFuture")
fun Column<Instant>.isFuture(): ComparisonPredicate<Instant> =
    this gte Clock.System.now()

// ============== Date Range Helpers ==============

/**
 * Creates a date range from start to end (inclusive).
 */
@JvmName("localDateIsBetween")
fun Column<LocalDate>.isBetween(startDate: LocalDate, endDate: LocalDate): BetweenPredicate<LocalDate> =
    ColumnExpression(this).between(startDate, endDate)

/**
 * Creates a datetime range from start to end (inclusive).
 */
@JvmName("localDateTimeIsBetween")
fun Column<LocalDateTime>.isBetween(startDateTime: LocalDateTime, endDateTime: LocalDateTime): BetweenPredicate<LocalDateTime> =
    ColumnExpression(this).between(startDateTime, endDateTime)

/**
 * Creates an instant range from start to end (inclusive).
 */
@JvmName("instantIsBetween")
fun Column<Instant>.isBetween(startInstant: Instant, endInstant: Instant): BetweenPredicate<Instant> =
    ColumnExpression(this).between(startInstant, endInstant)

// ============== Helper Functions ==============

/**
 * Checks if a year is a leap year.
 */
private fun isLeapYear(year: Int): Boolean =
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)

/**
 * Gets the number of days in a month.
 */
private fun Month.length(isLeapYear: Boolean): Int = when (this) {
    Month.JANUARY, Month.MARCH, Month.MAY, Month.JULY, Month.AUGUST, Month.OCTOBER, Month.DECEMBER -> 31
    Month.APRIL, Month.JUNE, Month.SEPTEMBER, Month.NOVEMBER -> 30
    Month.FEBRUARY -> if (isLeapYear) 29 else 28
    else -> 30
}

// ============== Period-based Extensions ==============

/**
 * Creates a predicate for dates on or after a certain date.
 */
fun Column<LocalDate>.isOnOrAfter(date: LocalDate): ComparisonPredicate<LocalDate> =
    this gte date

/**
 * Creates a predicate for dates on or before a certain date.
 */
fun Column<LocalDate>.isOnOrBefore(date: LocalDate): ComparisonPredicate<LocalDate> =
    this lte date
