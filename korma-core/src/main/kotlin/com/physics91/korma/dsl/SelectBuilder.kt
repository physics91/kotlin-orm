package com.physics91.korma.dsl

import com.physics91.korma.dsl.clauses.WhereClauseSupport
import com.physics91.korma.expression.*
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Builder for SELECT queries.
 *
 * Supports:
 * - Column selection
 * - WHERE conditions
 * - JOINs
 * - GROUP BY / HAVING
 * - ORDER BY
 * - LIMIT / OFFSET
 * - DISTINCT
 *
 * Example:
 * ```kotlin
 * val query = SelectBuilder(Users)
 *     .select(Users.id, Users.name, Users.email)
 *     .where { (Users.age gt 18) and (Users.status eq "active") }
 *     .orderBy(Users.name.asc())
 *     .limit(10)
 *     .build(dialect)
 * ```
 */
@QueryDsl
open class SelectBuilder(
    private val from: FromSource
) : QueryBuilder, WhereClauseSupport<SelectBuilder> {

    constructor(table: Table) : this(TableSource(table))

    // ============== State ==============

    private val selectedColumns = mutableListOf<Expression<*>>()
    private var distinct = false
    private val joins = mutableListOf<JoinClause>()
    override var whereClause: Predicate? = null
    private val groupByColumns = mutableListOf<Expression<*>>()
    private var havingClause: Predicate? = null
    private val orderByExpressions = mutableListOf<OrderByExpression>()
    private var limitValue: Int? = null
    private var offsetValue: Long? = null
    private var forUpdate = false

    // ============== SELECT Clause ==============

    /**
     * Select specific columns or expressions.
     */
    fun select(vararg columns: Expression<*>): SelectBuilder {
        selectedColumns.addAll(columns)
        return this
    }

    /**
     * Select specific columns.
     */
    fun select(vararg columns: Column<*>): SelectBuilder {
        selectedColumns.addAll(columns.map { ColumnExpression(it) })
        return this
    }

    /**
     * Select all columns from the main table.
     */
    fun selectAll(): SelectBuilder {
        selectedColumns.add(AllColumnsExpression)
        return this
    }

    /**
     * Select all columns from a specific table.
     */
    fun selectAll(table: Table): SelectBuilder {
        selectedColumns.add(TableAllColumnsExpression(table))
        return this
    }

    /**
     * Add DISTINCT to the query.
     */
    fun distinct(): SelectBuilder {
        distinct = true
        return this
    }

    // ============== JOIN Clause ==============

    /**
     * Add an INNER JOIN.
     */
    fun join(table: Table, on: Predicate, alias: String? = null): SelectBuilder {
        joins.add(JoinClause(JoinType.INNER, table, on, alias))
        return this
    }

    /**
     * Add an INNER JOIN with condition builder.
     */
    fun join(table: Table, alias: String? = null, on: () -> Predicate): SelectBuilder {
        joins.add(JoinClause(JoinType.INNER, table, on(), alias))
        return this
    }

    /**
     * Add a LEFT JOIN.
     */
    fun leftJoin(table: Table, on: Predicate, alias: String? = null): SelectBuilder {
        joins.add(JoinClause(JoinType.LEFT, table, on, alias))
        return this
    }

    /**
     * Add a LEFT JOIN with condition builder.
     */
    fun leftJoin(table: Table, alias: String? = null, on: () -> Predicate): SelectBuilder {
        joins.add(JoinClause(JoinType.LEFT, table, on(), alias))
        return this
    }

    /**
     * Add a RIGHT JOIN.
     */
    fun rightJoin(table: Table, on: Predicate, alias: String? = null): SelectBuilder {
        joins.add(JoinClause(JoinType.RIGHT, table, on, alias))
        return this
    }

    /**
     * Add a FULL OUTER JOIN.
     */
    fun fullJoin(table: Table, on: Predicate, alias: String? = null): SelectBuilder {
        joins.add(JoinClause(JoinType.FULL, table, on, alias))
        return this
    }

    /**
     * Internal method to add a type-safe join.
     * Used by TypeSafeJoin extension functions.
     */
    internal fun addTypedJoin(
        type: JoinType,
        table: Table,
        condition: Predicate,
        alias: String? = null
    ): SelectBuilder {
        joins.add(JoinClause(type, table, condition, alias))
        return this
    }

    // ============== WHERE Clause ==============
    // Inherited from WhereClauseSupport:
    // - where(condition), where(builder)
    // - andWhere(condition), orWhere(condition)
    // - whereIfNotNull(value, predicateFactory)

    // ============== GROUP BY / HAVING ==============

    /**
     * Add GROUP BY columns.
     */
    fun groupBy(vararg columns: Column<*>): SelectBuilder {
        groupByColumns.addAll(columns.map { ColumnExpression(it) })
        return this
    }

    /**
     * Add GROUP BY expressions.
     */
    fun groupBy(vararg expressions: Expression<*>): SelectBuilder {
        groupByColumns.addAll(expressions)
        return this
    }

    /**
     * Set the HAVING condition.
     */
    fun having(condition: Predicate): SelectBuilder {
        havingClause = condition
        return this
    }

    /**
     * Set the HAVING condition with a builder.
     */
    fun having(builder: () -> Predicate): SelectBuilder {
        havingClause = builder()
        return this
    }

    // ============== ORDER BY ==============

    /**
     * Add ORDER BY expressions.
     */
    fun orderBy(vararg orders: OrderByExpression): SelectBuilder {
        orderByExpressions.addAll(orders)
        return this
    }

    /**
     * Add ORDER BY column ascending.
     */
    fun orderBy(column: Column<*>): SelectBuilder {
        orderByExpressions.add(column.asc())
        return this
    }

    // ============== LIMIT / OFFSET ==============

    /**
     * Set the LIMIT.
     */
    fun limit(count: Int): SelectBuilder {
        limitValue = count
        return this
    }

    /**
     * Set the OFFSET.
     */
    fun offset(count: Long): SelectBuilder {
        offsetValue = count
        return this
    }

    /**
     * Set both LIMIT and OFFSET for pagination.
     */
    fun paginate(page: Int, pageSize: Int): SelectBuilder {
        limitValue = pageSize
        offsetValue = ((page - 1) * pageSize).toLong()
        return this
    }

    // ============== Locking ==============

    /**
     * Add FOR UPDATE clause.
     */
    fun forUpdate(): SelectBuilder {
        forUpdate = true
        return this
    }

    // ============== Build ==============

    /**
     * Build the SELECT query.
     */
    override fun build(dialect: SqlDialect): PreparedSql {
        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        // SELECT
        sql.append("SELECT ")
        if (distinct) {
            sql.append("DISTINCT ")
        }

        // Columns
        if (selectedColumns.isEmpty()) {
            sql.append("*")
        } else {
            sql.append(selectedColumns.joinToString(", ") { it.toSql(dialect, params) })
        }

        // FROM
        sql.append(" FROM ")
        sql.append(from.toSql(dialect, params))

        // JOINs
        for (join in joins) {
            sql.append(" ")
            sql.append(join.type.sql)
            sql.append(" ")
            sql.append(dialect.quoteIdentifier(join.table.tableName))
            if (join.alias != null) {
                sql.append(" AS ")
                sql.append(dialect.quoteIdentifier(join.alias))
            }
            sql.append(" ON ")
            sql.append(join.condition.toSql(dialect, params))
        }

        // WHERE
        if (whereClause != null) {
            sql.append(" WHERE ")
            sql.append(whereClause!!.toSql(dialect, params))
        }

        // GROUP BY
        if (groupByColumns.isNotEmpty()) {
            sql.append(" GROUP BY ")
            sql.append(groupByColumns.joinToString(", ") { it.toSql(dialect, params) })
        }

        // HAVING
        if (havingClause != null) {
            sql.append(" HAVING ")
            sql.append(havingClause!!.toSql(dialect, params))
        }

        // ORDER BY
        if (orderByExpressions.isNotEmpty()) {
            sql.append(" ORDER BY ")
            sql.append(orderByExpressions.joinToString(", ") { it.toSql(dialect, params) })
        }

        // LIMIT / OFFSET
        val limitOffset = dialect.limitOffsetClause(limitValue, offsetValue)
        if (limitOffset.isNotEmpty()) {
            sql.append(" ")
            sql.append(limitOffset)
        }

        // FOR UPDATE
        if (forUpdate) {
            sql.append(" FOR UPDATE")
        }

        return PreparedSql(sql.toString(), params)
    }
}

/**
 * Expression for table.* in SELECT.
 */
class TableAllColumnsExpression(
    val table: Table
) : Expression<Any> {
    override val columnType = null

    override fun toSql(dialect: SqlDialect, params: MutableList<Any?>): String {
        return "${dialect.quoteIdentifier(table.tableName)}.*"
    }

    override fun toString(): String = "${table.tableName}.*"
}

// ============== Convenience Functions ==============

/**
 * Start a SELECT query from a table.
 */
fun from(table: Table): SelectBuilder = SelectBuilder(table)

/**
 * Start a SELECT query from a subquery.
 */
fun from(subquery: SelectBuilder, alias: String): SelectBuilder =
    SelectBuilder(SubquerySource(subquery, alias))
