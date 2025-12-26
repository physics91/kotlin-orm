package com.physics91.korma.extensions

import com.physics91.korma.dsl.eq
import com.physics91.korma.dsl.gt
import com.physics91.korma.dsl.gte
import com.physics91.korma.dsl.lt
import com.physics91.korma.dsl.lte
import com.physics91.korma.expression.BetweenPredicate
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.ComparisonPredicate
import com.physics91.korma.expression.InPredicate
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import java.math.BigDecimal

/**
 * Kotlin-native numeric extensions for Korma.
 *
 * Provides idiomatic Kotlin operations for numeric columns,
 * making queries more readable and expressive.
 */

// ============== Integer Column Extensions ==============

/**
 * Creates a predicate for positive values (> 0).
 */
@JvmName("intIsPositive")
fun Column<Int>.isPositive(): ComparisonPredicate<Int> =
    this gt 0

/**
 * Creates a predicate for negative values (< 0).
 */
@JvmName("intIsNegative")
fun Column<Int>.isNegative(): ComparisonPredicate<Int> =
    this lt 0

/**
 * Creates a predicate for zero.
 */
@JvmName("intIsZero")
fun Column<Int>.isZero(): ComparisonPredicate<Int> =
    this eq 0

/**
 * Creates a predicate for non-negative values (>= 0).
 */
@JvmName("intIsZeroOrPositive")
fun Column<Int>.isZeroOrPositive(): ComparisonPredicate<Int> =
    this gte 0

/**
 * Creates a predicate for non-positive values (<= 0).
 */
@JvmName("intIsZeroOrNegative")
fun Column<Int>.isZeroOrNegative(): ComparisonPredicate<Int> =
    this lte 0

/**
 * Creates a BETWEEN predicate for a range.
 */
@JvmName("intIsInRange")
fun Column<Int>.isInRange(range: IntRange): BetweenPredicate<Int> =
    ColumnExpression(this).between(range.first, range.last)

/**
 * Creates an IN predicate for specific values.
 */
@JvmName("intIsOneOf")
fun Column<Int>.isOneOf(vararg values: Int): InPredicate<Int> =
    ColumnExpression(this).inList(values.toList())

// ============== Long Column Extensions ==============

/**
 * Creates a predicate for positive values (> 0).
 */
@JvmName("longIsPositive")
fun Column<Long>.isPositive(): ComparisonPredicate<Long> =
    this gt 0L

/**
 * Creates a predicate for negative values (< 0).
 */
@JvmName("longIsNegative")
fun Column<Long>.isNegative(): ComparisonPredicate<Long> =
    this lt 0L

/**
 * Creates a predicate for zero.
 */
@JvmName("longIsZero")
fun Column<Long>.isZero(): ComparisonPredicate<Long> =
    this eq 0L

/**
 * Creates a predicate for non-negative values (>= 0).
 */
@JvmName("longIsZeroOrPositive")
fun Column<Long>.isZeroOrPositive(): ComparisonPredicate<Long> =
    this gte 0L

/**
 * Creates a BETWEEN predicate for a range.
 */
@JvmName("longIsInRange")
fun Column<Long>.isInRange(range: LongRange): BetweenPredicate<Long> =
    ColumnExpression(this).between(range.first, range.last)

/**
 * Creates an IN predicate for specific values.
 */
@JvmName("longIsOneOf")
fun Column<Long>.isOneOf(vararg values: Long): InPredicate<Long> =
    ColumnExpression(this).inList(values.toList())

// ============== BigDecimal Column Extensions ==============

/**
 * Creates a predicate for positive values (> 0).
 */
@JvmName("bigDecimalIsPositive")
fun Column<BigDecimal>.isPositive(): ComparisonPredicate<BigDecimal> =
    this gt BigDecimal.ZERO

/**
 * Creates a predicate for negative values (< 0).
 */
@JvmName("bigDecimalIsNegative")
fun Column<BigDecimal>.isNegative(): ComparisonPredicate<BigDecimal> =
    this lt BigDecimal.ZERO

/**
 * Creates a predicate for zero.
 */
@JvmName("bigDecimalIsZero")
fun Column<BigDecimal>.isZero(): ComparisonPredicate<BigDecimal> =
    this eq BigDecimal.ZERO

/**
 * Creates a predicate for non-negative values (>= 0).
 */
@JvmName("bigDecimalIsZeroOrPositive")
fun Column<BigDecimal>.isZeroOrPositive(): ComparisonPredicate<BigDecimal> =
    this gte BigDecimal.ZERO

/**
 * Creates a BETWEEN predicate for a range.
 */
@JvmName("bigDecimalIsInRange")
fun Column<BigDecimal>.isInRange(min: BigDecimal, max: BigDecimal): BetweenPredicate<BigDecimal> =
    ColumnExpression(this).between(min, max)

/**
 * Creates a predicate for values within a percentage of a target.
 */
fun Column<BigDecimal>.isWithinPercent(target: BigDecimal, percent: Double): BetweenPredicate<BigDecimal> {
    val factor = BigDecimal.valueOf(percent / 100.0)
    val delta = target.multiply(factor)
    val min = target.subtract(delta)
    val max = target.add(delta)
    return ColumnExpression(this).between(min, max)
}

// ============== Double Column Extensions ==============

/**
 * Creates a predicate for positive values (> 0).
 */
@JvmName("doubleIsPositive")
fun Column<Double>.isPositive(): ComparisonPredicate<Double> =
    this gt 0.0

/**
 * Creates a predicate for negative values (< 0).
 */
@JvmName("doubleIsNegative")
fun Column<Double>.isNegative(): ComparisonPredicate<Double> =
    this lt 0.0

/**
 * Creates a predicate for zero.
 */
@JvmName("doubleIsZero")
fun Column<Double>.isZero(): ComparisonPredicate<Double> =
    this eq 0.0

/**
 * Creates a predicate for non-negative values (>= 0).
 */
@JvmName("doubleIsZeroOrPositive")
fun Column<Double>.isZeroOrPositive(): ComparisonPredicate<Double> =
    this gte 0.0

/**
 * Creates a BETWEEN predicate for a range.
 */
@JvmName("doubleIsInRange")
fun Column<Double>.isInRange(min: Double, max: Double): BetweenPredicate<Double> =
    ColumnExpression(this).between(min, max)

// ============== Boolean Column Extensions ==============

/**
 * Creates a predicate for true values.
 */
fun Column<Boolean>.isTrue(): ComparisonPredicate<Boolean> =
    this eq true

/**
 * Creates a predicate for false values.
 */
fun Column<Boolean>.isFalse(): ComparisonPredicate<Boolean> =
    this eq false

// ============== Generic Comparable Extensions ==============

/**
 * Creates a >= predicate using Kotlin's idiomatic >= operator feel.
 */
infix fun <T : Comparable<T>> Column<T>.greaterThanOrEqualTo(value: T): ComparisonPredicate<T> =
    this gte value

/**
 * Creates a <= predicate using Kotlin's idiomatic <= operator feel.
 */
infix fun <T : Comparable<T>> Column<T>.lessThanOrEqualTo(value: T): ComparisonPredicate<T> =
    this lte value

/**
 * Creates a > predicate using Kotlin's idiomatic > operator feel.
 */
infix fun <T : Comparable<T>> Column<T>.greaterThan(value: T): ComparisonPredicate<T> =
    this gt value

/**
 * Creates a < predicate using Kotlin's idiomatic < operator feel.
 */
infix fun <T : Comparable<T>> Column<T>.lessThan(value: T): ComparisonPredicate<T> =
    this lt value
