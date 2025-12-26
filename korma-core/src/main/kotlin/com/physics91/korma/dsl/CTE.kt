package com.physics91.korma.dsl

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.Expression
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Common Table Expression (CTE) definition.
 *
 * Usage:
 * ```kotlin
 * val activeDepts = cte("active_departments") {
 *     from(Departments)
 *         .select(Departments.id, Departments.name)
 *         .where { Departments.active eq true }
 * }
 *
 * withCte(activeDepts)
 *     .select(Users.name, activeDepts["name"])
 *     .from(Users)
 *     .join(activeDepts, on = { Users.departmentId eq activeDepts["id"] })
 *     .build(dialect)
 * ```
 */
class CteDefinition(
    val name: String,
    val columnAliases: List<String> = emptyList(),
    val query: SelectBuilder,
    val recursive: Boolean = false
) {
    /**
     * Reference a column from this CTE by name.
     */
    operator fun get(columnName: String): CteColumnReference =
        CteColumnReference(this, columnName)

    /**
     * Convert to virtual table for use in FROM/JOIN clauses.
     */
    fun asTable(): CteTable = CteTable(this)
}

/**
 * Reference to a column in a CTE.
 */
class CteColumnReference(
    val cte: CteDefinition,
    val columnName: String
) : Expression<Any> {
    override val columnType = null

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "${dialect.quoteIdentifier(cte.name)}.${dialect.quoteIdentifier(columnName)}"
    }

    override fun toString(): String = "${cte.name}.$columnName"

    // Comparison operators for WHERE clauses
    infix fun eq(value: Any?): Expression<Boolean> = CteColumnPredicate(this, "=", value)
    infix fun neq(value: Any?): Expression<Boolean> = CteColumnPredicate(this, "<>", value)
    infix fun lt(value: Any?): Expression<Boolean> = CteColumnPredicate(this, "<", value)
    infix fun lte(value: Any?): Expression<Boolean> = CteColumnPredicate(this, "<=", value)
    infix fun gt(value: Any?): Expression<Boolean> = CteColumnPredicate(this, ">", value)
    infix fun gte(value: Any?): Expression<Boolean> = CteColumnPredicate(this, ">=", value)

    // Comparison with column
    infix fun <T> eq(column: Column<T>): Expression<Boolean> = CteColumnColumnPredicate(this, "=", column)
    infix fun <T> eq(expr: ColumnExpression<T>): Expression<Boolean> = CteColumnExprPredicate(this, "=", expr)
}

/**
 * Predicate for CTE column comparison with literal value.
 */
class CteColumnPredicate(
    val column: CteColumnReference,
    val operator: String,
    val value: Any?
) : Expression<Boolean> {
    override val columnType = null

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        if (value == null) {
            return when (operator) {
                "=" -> "${column.toSql(dialect, params)} IS NULL"
                "<>" -> "${column.toSql(dialect, params)} IS NOT NULL"
                else -> "${column.toSql(dialect, params)} $operator NULL"
            }
        }
        params.add(value)
        return "${column.toSql(dialect, params)} $operator ?"
    }
}

/**
 * Predicate for CTE column comparison with another column.
 */
class CteColumnColumnPredicate<T>(
    val cteColumn: CteColumnReference,
    val operator: String,
    val column: Column<T>
) : Expression<Boolean> {
    override val columnType = null

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "${cteColumn.toSql(dialect, params)} $operator ${dialect.quoteIdentifier(column.table.tableName)}.${dialect.quoteIdentifier(column.name)}"
    }
}

/**
 * Predicate for CTE column comparison with column expression.
 */
class CteColumnExprPredicate<T>(
    val cteColumn: CteColumnReference,
    val operator: String,
    val expr: ColumnExpression<T>
) : Expression<Boolean> {
    override val columnType = null

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "${cteColumn.toSql(dialect, params)} $operator ${expr.toSql(dialect, params)}"
    }
}

/**
 * Virtual table representing a CTE for use in FROM/JOIN clauses.
 */
class CteTable(
    val cte: CteDefinition
) : Table(cte.name)

/**
 * Builder for queries with CTEs.
 */
@QueryDsl
class WithCteBuilder(
    private val ctes: List<CteDefinition>
) {
    /**
     * Start a SELECT from a CTE.
     */
    fun from(cte: CteDefinition): CteSelectBuilder {
        return CteSelectBuilder(ctes, CteSource(cte))
    }

    /**
     * Start a SELECT from a table.
     */
    fun from(table: Table): CteSelectBuilder {
        return CteSelectBuilder(ctes, TableSource(table))
    }
}

/**
 * SelectBuilder that includes CTE definitions in the generated SQL.
 */
@QueryDsl
class CteSelectBuilder(
    private val ctes: List<CteDefinition>,
    from: FromSource
) : SelectBuilder(from) {

    override fun build(dialect: SqlDialect): PreparedSql {
        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        // WITH clause
        val hasRecursive = ctes.any { it.recursive }
        sql.append(if (hasRecursive) "WITH RECURSIVE " else "WITH ")

        sql.append(ctes.joinToString(", ") { cte ->
            val cteSql = cte.query.build(dialect)
            params.addAll(cteSql.params)

            val columnAliasesPart = if (cte.columnAliases.isNotEmpty()) {
                " (${cte.columnAliases.joinToString(", ") { dialect.quoteIdentifier(it) }})"
            } else ""

            "${dialect.quoteIdentifier(cte.name)}$columnAliasesPart AS (${cteSql.sql})"
        })

        // Main query
        sql.append(" ")
        val mainSql = super.build(dialect)
        params.addAll(mainSql.params)
        sql.append(mainSql.sql)

        return PreparedSql(sql.toString(), params)
    }
}

/**
 * CTE as a FROM source.
 */
class CteSource(
    val cte: CteDefinition
) : FromSource {
    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return dialect.quoteIdentifier(cte.name)
    }
}

// ============== Convenience Functions ==============

/**
 * Create a CTE definition.
 */
fun cte(name: String, query: () -> SelectBuilder): CteDefinition =
    CteDefinition(name = name, query = query())

/**
 * Create a CTE definition with column aliases.
 */
fun cte(name: String, vararg columnAliases: String, query: () -> SelectBuilder): CteDefinition =
    CteDefinition(name = name, columnAliases = columnAliases.toList(), query = query())

/**
 * Create a recursive CTE definition.
 */
fun recursiveCte(name: String, query: () -> SelectBuilder): CteDefinition =
    CteDefinition(name = name, query = query(), recursive = true)

/**
 * Create a recursive CTE definition with column aliases.
 */
fun recursiveCte(name: String, vararg columnAliases: String, query: () -> SelectBuilder): CteDefinition =
    CteDefinition(name = name, columnAliases = columnAliases.toList(), query = query(), recursive = true)

/**
 * Start a query with one or more CTEs.
 */
fun withCte(vararg ctes: CteDefinition): WithCteBuilder =
    WithCteBuilder(ctes.toList())

/**
 * Start a query with CTEs.
 */
fun withCte(ctes: List<CteDefinition>): WithCteBuilder =
    WithCteBuilder(ctes)

// ============== Recursive CTE Support ==============

/**
 * UNION ALL expression for recursive CTEs.
 */
class UnionAllBuilder(
    private val firstQuery: SelectBuilder
) {
    private val additionalQueries = mutableListOf<SelectBuilder>()

    /**
     * Add a query with UNION ALL.
     */
    fun unionAll(query: SelectBuilder): UnionAllBuilder {
        additionalQueries.add(query)
        return this
    }

    /**
     * Build as a combined query.
     */
    fun build(dialect: SqlDialect): PreparedSql {
        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        val firstSql = firstQuery.build(dialect)
        params.addAll(firstSql.params)
        sql.append(firstSql.sql)

        for (query in additionalQueries) {
            sql.append(" UNION ALL ")
            val querySql = query.build(dialect)
            params.addAll(querySql.params)
            sql.append(querySql.sql)
        }

        return PreparedSql(sql.toString(), params)
    }
}

/**
 * Start a UNION ALL query.
 */
fun SelectBuilder.unionAll(other: SelectBuilder): UnionAllBuilder =
    UnionAllBuilder(this).unionAll(other)
