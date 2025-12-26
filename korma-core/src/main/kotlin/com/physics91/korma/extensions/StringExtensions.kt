package com.physics91.korma.extensions

import com.physics91.korma.dsl.eq
import com.physics91.korma.dsl.neq
import com.physics91.korma.expression.ComparisonPredicate
import com.physics91.korma.expression.LikePredicate
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.schema.Column

/**
 * Kotlin-native string extensions for Korma.
 *
 * Provides idiomatic Kotlin operations for string columns,
 * making queries more readable and expressive.
 *
 * Usage:
 * ```kotlin
 * // Using extension functions
 * Users.selectAll()
 *     .where {
 *         Users.name.contains("john") and
 *         Users.email.endsWith("@example.com") and
 *         Users.phone.matches("\\d{3}-\\d{4}")
 *     }
 *
 * // Case-insensitive operations
 * Users.selectAll()
 *     .where { Users.name.containsIgnoreCase("JOHN") }
 * ```
 */

// ============== String Column Extensions ==============

/**
 * Creates a LIKE predicate for substring matching.
 * Matches if the column contains the given substring anywhere.
 *
 * SQL: column LIKE '%substring%'
 */
fun Column<String>.contains(substring: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "%$substring%", false, false)

/**
 * Creates a case-insensitive LIKE predicate for substring matching.
 *
 * SQL: column ILIKE '%substring%' (PostgreSQL) or LOWER(column) LIKE LOWER('%substring%')
 */
fun Column<String>.containsIgnoreCase(substring: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "%$substring%", false, true)

/**
 * Creates a LIKE predicate for prefix matching.
 * Matches if the column starts with the given prefix.
 *
 * SQL: column LIKE 'prefix%'
 */
fun Column<String>.startsWith(prefix: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "$prefix%", false, false)

/**
 * Creates a case-insensitive LIKE predicate for prefix matching.
 */
fun Column<String>.startsWithIgnoreCase(prefix: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "$prefix%", false, true)

/**
 * Creates a LIKE predicate for suffix matching.
 * Matches if the column ends with the given suffix.
 *
 * SQL: column LIKE '%suffix'
 */
fun Column<String>.endsWith(suffix: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "%$suffix", false, false)

/**
 * Creates a case-insensitive LIKE predicate for suffix matching.
 */
fun Column<String>.endsWithIgnoreCase(suffix: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "%$suffix", false, true)

/**
 * Creates a LIKE predicate with a custom pattern.
 * Use % for multi-character wildcard and _ for single character.
 *
 * SQL: column LIKE 'pattern'
 */
fun Column<String>.matches(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, false, false)

/**
 * Creates a case-insensitive LIKE predicate with a custom pattern.
 */
fun Column<String>.matchesIgnoreCase(pattern: String): LikePredicate =
    LikePredicate(ColumnExpression(this), pattern, false, true)

/**
 * Creates a NOT LIKE predicate.
 * Matches if the column does NOT contain the given substring.
 *
 * SQL: column NOT LIKE '%substring%'
 */
fun Column<String>.notContains(substring: String): LikePredicate =
    LikePredicate(ColumnExpression(this), "%$substring%", true, false)

/**
 * Creates a predicate matching empty strings.
 *
 * SQL: column = ''
 */
fun Column<String>.isEmpty(): ComparisonPredicate<String> =
    this eq ""

/**
 * Creates a predicate matching non-empty strings.
 *
 * SQL: column <> ''
 */
fun Column<String>.isNotEmpty(): ComparisonPredicate<String> =
    this neq ""

// ============== String Escaping Utilities ==============

/**
 * Escapes special LIKE pattern characters (% and _) in a string.
 * Use when the search term may contain literal % or _ characters.
 *
 * Example:
 * ```kotlin
 * val searchTerm = "50%"
 * Users.name.contains(searchTerm.escapeLikePattern())
 * // Generates: name LIKE '%50\%%'
 * ```
 */
fun String.escapeLikePattern(): String =
    this.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")

/**
 * Safely creates a contains predicate with escaped special characters.
 */
fun Column<String>.containsSafe(substring: String): LikePredicate =
    contains(substring.escapeLikePattern())

/**
 * Safely creates a startsWith predicate with escaped special characters.
 */
fun Column<String>.startsWithSafe(prefix: String): LikePredicate =
    startsWith(prefix.escapeLikePattern())

/**
 * Safely creates an endsWith predicate with escaped special characters.
 */
fun Column<String>.endsWithSafe(suffix: String): LikePredicate =
    endsWith(suffix.escapeLikePattern())
