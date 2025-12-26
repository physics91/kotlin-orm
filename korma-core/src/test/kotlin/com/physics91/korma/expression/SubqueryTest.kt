package com.physics91.korma.expression

import com.physics91.korma.dsl.from
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Test tables
object SubqUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val departmentId = long("department_id")
}

object SubqDepartments : Table("departments") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val active = boolean("active")
}

object SubqOrders : Table("orders") {
    val id = long("id").primaryKey()
    val userId = long("user_id")
    val amount = integer("amount")
}

class SubqueryTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: com.physics91.korma.schema.ColumnType<*>): String = "AUTO_INCREMENT"
    }

    test("should create IN subquery predicate") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqDepartments)
            .select(SubqDepartments.id)
            .where { ColumnExpression(SubqDepartments.active) eq true }

        val predicate = ColumnExpression(SubqUsers.departmentId) inSubquery subquery

        val sql = predicate.toSql(dialect, params)

        sql shouldContain "\"users\".\"department_id\" IN"
        sql shouldContain "SELECT"
        sql shouldContain "\"departments\""
        params shouldBe listOf(true)
    }

    test("should create NOT IN subquery predicate") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqDepartments)
            .select(SubqDepartments.id)
            .where { ColumnExpression(SubqDepartments.active) eq false }

        val predicate = ColumnExpression(SubqUsers.departmentId) notInSubquery subquery

        val sql = predicate.toSql(dialect, params)

        sql shouldContain "NOT IN"
        params shouldBe listOf(false)
    }

    test("should create EXISTS predicate") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqOrders)
            .select(SubqOrders.id)
            .where { ColumnExpression(SubqOrders.userId) eq ColumnExpression(SubqUsers.id) }

        val predicate = exists(subquery)

        val sql = predicate.toSql(dialect, params)

        sql shouldContain "EXISTS"
        sql shouldContain "SELECT"
        sql shouldContain "\"orders\""
    }

    test("should create NOT EXISTS predicate") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqOrders)
            .select(SubqOrders.id)
            .where { ColumnExpression(SubqOrders.userId) eq ColumnExpression(SubqUsers.id) }

        val predicate = notExists(subquery)

        val sql = predicate.toSql(dialect, params)

        sql shouldContain "NOT EXISTS"
    }

    test("should create scalar subquery") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqOrders)
            .select(count())
            .where { ColumnExpression(SubqOrders.userId) eq ColumnExpression(SubqUsers.id) }

        val scalarExpr = scalar<Long>(subquery)

        val sql = scalarExpr.toSql(dialect, params)

        sql shouldContain "(SELECT COUNT(*)"
        sql shouldContain ")"
    }

    test("should create comparison with subquery") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqOrders)
            .select(max(SubqOrders.amount))

        val predicate = ColumnExpression(SubqOrders.amount) gtSubquery subquery

        val sql = predicate.toSql(dialect, params)

        sql shouldContain "\"orders\".\"amount\" >"
        sql shouldContain "(SELECT MAX"
    }

    test("should create ANY subquery comparison") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqOrders)
            .select(SubqOrders.amount)
            .where { ColumnExpression(SubqOrders.userId) eq 1L }

        val anySubq = any(subquery)
        val predicate = ColumnExpression(SubqOrders.amount) gt anySubq

        val sql = predicate.toSql(dialect, params)

        sql shouldContain "> ANY"
        sql shouldContain "(SELECT"
        params shouldBe listOf(1L)
    }

    test("should create ALL subquery comparison") {
        val params = mutableListOf<Any?>()

        val subquery = from(SubqOrders)
            .select(SubqOrders.amount)

        val allSubq = all(subquery)
        val predicate = ColumnExpression(SubqOrders.amount) gte allSubq

        val sql = predicate.toSql(dialect, params)

        sql shouldContain ">= ALL"
        sql shouldContain "(SELECT"
    }

    test("should use IN subquery in SELECT query") {
        val subquery = from(SubqDepartments)
            .select(SubqDepartments.id)
            .where { ColumnExpression(SubqDepartments.active) eq true }

        val query = from(SubqUsers)
            .select(SubqUsers.name)
            .where { SubqUsers.departmentId inSubquery subquery }
            .build(dialect)

        query.sql shouldContain "SELECT \"users\".\"name\""
        query.sql shouldContain "WHERE \"users\".\"department_id\" IN"
        query.sql shouldContain "SELECT \"departments\".\"id\""
        query.params shouldBe listOf(true)
    }

    test("should use EXISTS in SELECT query") {
        val ordersSubquery = from(SubqOrders)
            .selectAll()
            .where { ColumnExpression(SubqOrders.userId) eq ColumnExpression(SubqUsers.id) }

        val query = from(SubqUsers)
            .select(SubqUsers.name)
            .where { exists(ordersSubquery) }
            .build(dialect)

        query.sql shouldContain "WHERE EXISTS"
        query.sql shouldContain "SELECT *"
    }
})
