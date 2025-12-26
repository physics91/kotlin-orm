package com.physics91.korma.expression

import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

// Define tables at file level
object ExprTestUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val age = integer("age")
    val active = boolean("active")
}

class ExpressionTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: com.physics91.korma.schema.ColumnType<*>): String = "AUTO_INCREMENT"
    }

    test("should create column expression") {
        val params = mutableListOf<Any?>()
        val expr = ColumnExpression(ExprTestUsers.id)

        expr.toSql(dialect, params) shouldBe "\"users\".\"id\""
    }

    test("should create literal expression") {
        val params = mutableListOf<Any?>()
        val expr = LiteralExpression(42, null)

        expr.toSql(dialect, params) shouldBe "?"
        params shouldBe listOf(42)
    }

    test("should create comparison predicates") {
        val params = mutableListOf<Any?>()

        val eqExpr = ColumnExpression(ExprTestUsers.id) eq 1L
        eqExpr.toSql(dialect, params) shouldBe "\"users\".\"id\" = ?"
        params shouldBe listOf(1L)

        params.clear()
        val gtExpr = ColumnExpression(ExprTestUsers.age) gt 18
        gtExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" > ?"
        params shouldBe listOf(18)

        params.clear()
        val lteExpr = ColumnExpression(ExprTestUsers.age) lte 65
        lteExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" <= ?"
        params shouldBe listOf(65)
    }

    test("should combine predicates with AND") {
        val params = mutableListOf<Any?>()

        val predicate = (ColumnExpression(ExprTestUsers.age) gt 18) and (ColumnExpression(ExprTestUsers.active) eq true)
        predicate.toSql(dialect, params) shouldBe "(\"users\".\"age\" > ?) AND (\"users\".\"active\" = ?)"
        params shouldBe listOf(18, true)
    }

    test("should combine predicates with OR") {
        val params = mutableListOf<Any?>()

        val predicate = (ColumnExpression(ExprTestUsers.name) eq "admin") or (ColumnExpression(ExprTestUsers.active) eq true)
        predicate.toSql(dialect, params) shouldBe "(\"users\".\"name\" = ?) OR (\"users\".\"active\" = ?)"
        params shouldBe listOf("admin", true)
    }

    test("should negate predicates") {
        val params = mutableListOf<Any?>()

        val predicate = !(ColumnExpression(ExprTestUsers.active) eq true)
        predicate.toSql(dialect, params) shouldBe "NOT (\"users\".\"active\" = ?)"
        params shouldBe listOf(true)
    }

    test("should create null checks") {
        val params = mutableListOf<Any?>()

        val isNullExpr = ColumnExpression(ExprTestUsers.age).isNull()
        isNullExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" IS NULL"

        val isNotNullExpr = ColumnExpression(ExprTestUsers.age).isNotNull()
        isNotNullExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" IS NOT NULL"
    }

    test("should create IN predicates") {
        val params = mutableListOf<Any?>()

        val inExpr = ColumnExpression(ExprTestUsers.id).inList(listOf(1L, 2L, 3L))
        inExpr.toSql(dialect, params) shouldBe "\"users\".\"id\" IN (?, ?, ?)"
        params shouldBe listOf(1L, 2L, 3L)
    }

    test("should create NOT IN predicates") {
        val params = mutableListOf<Any?>()

        val notInExpr = ColumnExpression(ExprTestUsers.id).notInList(listOf(1L, 2L))
        notInExpr.toSql(dialect, params) shouldBe "\"users\".\"id\" NOT IN (?, ?)"
        params shouldBe listOf(1L, 2L)
    }

    test("should create BETWEEN predicates") {
        val params = mutableListOf<Any?>()

        val betweenExpr = ColumnExpression(ExprTestUsers.age).between(18, 65)
        betweenExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" BETWEEN ? AND ?"
        params shouldBe listOf(18, 65)
    }

    test("should create LIKE predicates") {
        val params = mutableListOf<Any?>()

        val strExpr = StringColumnExpression(ExprTestUsers.name)
        val likeExpr = strExpr like "%john%"
        likeExpr.toSql(dialect, params) shouldBe "\"users\".\"name\" LIKE ?"
        params shouldBe listOf("%john%")
    }

    test("should create ORDER BY expressions") {
        val params = mutableListOf<Any?>()

        val ascExpr = ColumnExpression(ExprTestUsers.name).asc()
        ascExpr.toSql(dialect, params) shouldBe "\"users\".\"name\" ASC"

        val descExpr = ColumnExpression(ExprTestUsers.age).desc()
        descExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" DESC"

        val nullsFirstExpr = ColumnExpression(ExprTestUsers.age).ascNullsFirst()
        nullsFirstExpr.toSql(dialect, params) shouldBe "\"users\".\"age\" ASC NULLS FIRST"
    }

    test("should create alias expression") {
        val params = mutableListOf<Any?>()

        val aliasExpr = ColumnExpression(ExprTestUsers.name) alias "user_name"
        aliasExpr.toSql(dialect, params) shouldBe "\"users\".\"name\" AS \"user_name\""
    }

    test("should perform arithmetic operations") {
        val params = mutableListOf<Any?>()

        val addExpr = ColumnExpression(ExprTestUsers.age) + 1
        addExpr.toSql(dialect, params) shouldBe "(\"users\".\"age\" + ?)"
        params shouldBe listOf(1)

        params.clear()
        val subExpr = ColumnExpression(ExprTestUsers.age) - 5
        subExpr.toSql(dialect, params) shouldBe "(\"users\".\"age\" - ?)"
        params shouldBe listOf(5)
    }
})
