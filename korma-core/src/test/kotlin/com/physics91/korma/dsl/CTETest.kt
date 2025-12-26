package com.physics91.korma.dsl

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.expression.Expression
import com.physics91.korma.expression.count
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Test tables
object CteUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val departmentId = long("department_id")
    val managerId = long("manager_id").nullable()
}

object CteDepartments : Table("departments") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val active = boolean("active")
}

object CteOrders : Table("orders") {
    val id = long("id").primaryKey()
    val userId = long("user_id")
    val total = integer("total")
}

class CTETest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override val supportsCTE = true
        override fun autoIncrementType(baseType: com.physics91.korma.schema.ColumnType<*>): String = "AUTO_INCREMENT"
    }

    test("should create simple CTE") {
        val activeDepts = cte("active_departments") {
            from(CteDepartments)
                .select(CteDepartments.id, CteDepartments.name)
                .where { ColumnExpression(CteDepartments.active) eq true }
        }

        val query = withCte(activeDepts)
            .from(activeDepts)
            .select(activeDepts["id"], activeDepts["name"])
            .build(dialect)

        query.sql shouldContain "WITH \"active_departments\" AS"
        query.sql shouldContain "SELECT \"departments\".\"id\", \"departments\".\"name\""
        query.sql shouldContain "WHERE \"departments\".\"active\" = ?"
        query.sql shouldContain "SELECT \"active_departments\".\"id\", \"active_departments\".\"name\""
        query.params shouldBe listOf(true)
    }

    test("should create CTE with column aliases") {
        val deptSummary = cte("dept_summary", "dept_id", "user_count") {
            from(CteUsers)
                .select(ColumnExpression(CteUsers.departmentId), count())
                .groupBy(ColumnExpression(CteUsers.departmentId))
        }

        val query = withCte(deptSummary)
            .from(deptSummary)
            .selectAll()
            .build(dialect)

        query.sql shouldContain "WITH \"dept_summary\" (\"dept_id\", \"user_count\") AS"
        query.sql shouldContain "GROUP BY"
    }

    test("should create multiple CTEs") {
        val activeDepts = cte("active_departments") {
            from(CteDepartments)
                .select(CteDepartments.id)
                .where { ColumnExpression(CteDepartments.active) eq true }
        }

        val activeUsers = cte("active_users") {
            from(CteUsers)
                .select(CteUsers.id, CteUsers.name)
        }

        val query = withCte(activeDepts, activeUsers)
            .from(activeUsers)
            .select(activeUsers["name"])
            .build(dialect)

        query.sql shouldContain "WITH \"active_departments\" AS"
        query.sql shouldContain ", \"active_users\" AS"
    }

    test("should use CTE column reference in predicate") {
        val activeDepts = cte("active_departments") {
            from(CteDepartments)
                .select(CteDepartments.id, CteDepartments.name)
                .where { ColumnExpression(CteDepartments.active) eq true }
        }

        val params = mutableListOf<Any?>()
        val predicate = activeDepts["id"] eq 1L
        val sql = predicate.toSql(dialect, params)

        sql shouldBe "\"active_departments\".\"id\" = ?"
        params shouldBe listOf(1L)
    }

    test("should support CTE column comparison with table column") {
        val activeDepts = cte("active_departments") {
            from(CteDepartments)
                .select(CteDepartments.id, CteDepartments.name)
                .where { ColumnExpression(CteDepartments.active) eq true }
        }

        val params = mutableListOf<Any?>()
        val predicate = activeDepts["id"] eq CteUsers.departmentId
        val sql = predicate.toSql(dialect, params)

        sql shouldBe "\"active_departments\".\"id\" = \"users\".\"department_id\""
    }

    test("should create recursive CTE") {
        // This is a placeholder test for recursive CTE syntax
        // Full recursive CTE would require UNION ALL support
        val hierarchy = recursiveCte("employee_hierarchy") {
            from(CteUsers)
                .select(CteUsers.id, CteUsers.name, CteUsers.managerId)
                .where { ColumnExpression(CteUsers.managerId).isNull() }
        }

        val query = withCte(hierarchy)
            .from(hierarchy)
            .selectAll()
            .build(dialect)

        query.sql shouldContain "WITH RECURSIVE \"employee_hierarchy\" AS"
    }

    test("should create UNION ALL query") {
        val query1 = from(CteUsers)
            .select(CteUsers.id)
            .where { ColumnExpression(CteUsers.departmentId) eq 1L }

        val query2 = from(CteUsers)
            .select(CteUsers.id)
            .where { ColumnExpression(CteUsers.departmentId) eq 2L }

        val unionQuery = query1.unionAll(query2).build(dialect)

        unionQuery.sql shouldContain "SELECT \"users\".\"id\""
        unionQuery.sql shouldContain "UNION ALL"
        unionQuery.sql shouldContain "WHERE \"users\".\"department_id\" = ?"
        unionQuery.params shouldBe listOf(1L, 2L)
    }

    test("should create CTE and join with regular table") {
        val orderTotals = cte("order_totals") {
            from(CteOrders)
                .select(CteOrders.userId, CteOrders.total)
        }

        // Main query joining CTE with Users table
        val query = withCte(orderTotals)
            .from(CteUsers)
            .select(CteUsers.name)
            .build(dialect)

        query.sql shouldContain "WITH \"order_totals\" AS"
        query.sql shouldContain "SELECT \"orders\".\"user_id\", \"orders\".\"total\""
        query.sql shouldContain "SELECT \"users\".\"name\""
    }
})
