package com.physics91.korma.dsl

import com.physics91.korma.dsl.clauses.ReturningClauseSupport
import com.physics91.korma.dsl.clauses.WhereClauseSupport
import com.physics91.korma.expression.Predicate
import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.PreparedSql
import com.physics91.korma.sql.SqlDialect

/**
 * Builder for DELETE queries.
 *
 * Supports:
 * - WHERE conditions
 * - RETURNING clause
 *
 * Example:
 * ```kotlin
 * val delete = DeleteBuilder(Users)
 *     .where { Users.status eq "deleted" }
 *     .build(dialect)
 * ```
 */
@QueryDsl
class DeleteBuilder(
    override val table: Table
) : QueryBuilder, WhereClauseSupport<DeleteBuilder>, ReturningClauseSupport<DeleteBuilder> {

    // Required override due to multiple interface inheritance
    override fun self(): DeleteBuilder = this

    // ============== State ==============

    override var whereClause: Predicate? = null
    override var returningColumns: List<Column<*>> = emptyList()

    // ============== WHERE Clause ==============
    // Inherited from WhereClauseSupport:
    // - where(condition), where(builder)
    // - andWhere(condition), orWhere(condition)
    // - whereIfNotNull(value, predicateFactory)

    // ============== RETURNING ==============
    // Inherited from ReturningClauseSupport:
    // - returning(vararg columns), returning(columns: List)
    // - returningAll()

    // ============== Build ==============

    override fun build(dialect: SqlDialect): PreparedSql {
        val params = mutableListOf<Any?>()
        val sql = StringBuilder()

        // DELETE FROM table
        sql.append("DELETE FROM ")
        sql.append(dialect.quoteIdentifier(table.tableName))

        // WHERE
        if (whereClause != null) {
            sql.append(" WHERE ")
            sql.append(whereClause!!.toSql(dialect, params))
        }

        // RETURNING
        if (returningColumns.isNotEmpty() && dialect.supportsReturning) {
            sql.append(" ")
            sql.append(dialect.returningClause(returningColumns))
        }

        return PreparedSql(sql.toString(), params)
    }
}

// ============== Convenience Functions ==============

/**
 * Start a DELETE query.
 */
fun deleteFrom(table: Table): DeleteBuilder = DeleteBuilder(table)

/**
 * Delete with WHERE condition.
 */
fun deleteFrom(table: Table, condition: () -> Predicate): DeleteBuilder =
    DeleteBuilder(table).where(condition)
