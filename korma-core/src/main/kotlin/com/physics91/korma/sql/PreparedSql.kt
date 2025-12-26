package com.physics91.korma.sql

/**
 * Represents a prepared SQL statement with parameters.
 *
 * @property sql The SQL string with ? placeholders
 * @property parameters The parameter values in order
 */
data class PreparedSql(
    val sql: String,
    val params: List<Any?>
) {
    /** Number of parameters */
    val parameterCount: Int get() = params.size

    override fun toString(): String {
        if (params.isEmpty()) return sql

        var result = sql
        var index = 0
        return result.replace(Regex("\\?")) {
            val value = params.getOrNull(index++)
            when (value) {
                null -> "NULL"
                is String -> "'$value'"
                is Number -> value.toString()
                is Boolean -> if (value) "TRUE" else "FALSE"
                else -> "'$value'"
            }
        }
    }

    companion object {
        /** Empty SQL (for debugging) */
        val EMPTY = PreparedSql("", emptyList())

        /** Create from SQL string without parameters */
        fun of(sql: String) = PreparedSql(sql, emptyList())
    }
}

/**
 * Builder for constructing SQL with parameters.
 */
class SqlBuilder(
    private val dialect: SqlDialect
) {
    private val sql = StringBuilder()
    private val parameters = mutableListOf<Any?>()

    /** Append raw SQL */
    fun append(text: String): SqlBuilder {
        sql.append(text)
        return this
    }

    /** Append with space prefix */
    fun appendWithSpace(text: String): SqlBuilder {
        if (sql.isNotEmpty() && !sql.endsWith(" ") && !sql.endsWith("\n")) {
            sql.append(" ")
        }
        sql.append(text)
        return this
    }

    /** Append a new line */
    fun newLine(): SqlBuilder {
        sql.append("\n")
        return this
    }

    /** Append a parameter placeholder */
    fun appendParameter(value: Any?): SqlBuilder {
        sql.append("?")
        parameters.add(value)
        return this
    }

    /** Append multiple parameter placeholders */
    fun appendParameters(values: List<Any?>): SqlBuilder {
        sql.append(values.joinToString(", ") { "?" })
        parameters.addAll(values)
        return this
    }

    /** Append a quoted identifier */
    fun appendIdentifier(name: String): SqlBuilder {
        sql.append(dialect.quoteIdentifier(name))
        return this
    }

    /** Append table.column */
    fun appendQualifiedColumn(tableName: String, columnName: String): SqlBuilder {
        sql.append(dialect.quoteIdentifier(tableName))
        sql.append(".")
        sql.append(dialect.quoteIdentifier(columnName))
        return this
    }

    /** Get current SQL length */
    val length: Int get() = sql.length

    /** Check if empty */
    val isEmpty: Boolean get() = sql.isEmpty()

    /** Build the prepared SQL */
    fun build(): PreparedSql = PreparedSql(sql.toString(), parameters.toList())

    /** Get current SQL string (for debugging) */
    override fun toString(): String = sql.toString()
}

/**
 * Extension for building SQL.
 */
inline fun buildSql(dialect: SqlDialect, block: SqlBuilder.() -> Unit): PreparedSql {
    return SqlBuilder(dialect).apply(block).build()
}
