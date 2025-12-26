package com.physics91.korma.sql

/**
 * SQL formatting utility.
 *
 * Provides consistent SQL formatting across all Korma modules.
 * Single Source of Truth for SQL formatting behavior.
 */
object SqlFormatter {
    /**
     * Format SQL for improved readability.
     *
     * Adds newlines before major SQL clauses for better log output.
     */
    // Regex to match all JOIN types in one pass (order matters: longest first)
    private val joinRegex = Regex(
        " (LEFT OUTER JOIN|RIGHT OUTER JOIN|FULL OUTER JOIN|" +
        "LEFT JOIN|RIGHT JOIN|FULL JOIN|INNER JOIN|CROSS JOIN|NATURAL JOIN|JOIN) "
    )

    fun format(sql: String): String {
        return sql
            .replace(Regex("\\s+"), " ")
            .replace(" FROM ", "\n  FROM ")
            .replace(" WHERE ", "\n  WHERE ")
            .replace(" AND ", "\n    AND ")
            .replace(" OR ", "\n    OR ")
            .replace(" ORDER BY ", "\n  ORDER BY ")
            .replace(" GROUP BY ", "\n  GROUP BY ")
            .replace(" HAVING ", "\n  HAVING ")
            .replace(joinRegex) { "\n  ${it.groupValues[1]} " }
            .trim()
    }
}
