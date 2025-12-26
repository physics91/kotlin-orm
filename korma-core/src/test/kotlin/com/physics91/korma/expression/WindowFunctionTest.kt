package com.physics91.korma.expression

import com.physics91.korma.dsl.desc
import com.physics91.korma.dsl.from
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Test tables
object WinEmployees : Table("employees") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val department = varchar("department", 50)
    val salary = integer("salary")
    val hireDate = date("hire_date")
}

class WindowFunctionTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: com.physics91.korma.schema.ColumnType<*>): String = "AUTO_INCREMENT"
    }

    test("should create ROW_NUMBER() function") {
        val params = mutableListOf<Any?>()

        val expr = rowNumber().over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.salary.desc())
        }

        val sql = expr.toSql(dialect, params)

        sql shouldBe "ROW_NUMBER() OVER (PARTITION BY \"employees\".\"department\" ORDER BY \"employees\".\"salary\" DESC)"
    }

    test("should create RANK() function") {
        val params = mutableListOf<Any?>()

        val expr = rank().over {
            orderBy(WinEmployees.salary.desc())
        }

        val sql = expr.toSql(dialect, params)

        sql shouldBe "RANK() OVER (ORDER BY \"employees\".\"salary\" DESC)"
    }

    test("should create DENSE_RANK() function") {
        val params = mutableListOf<Any?>()

        val expr = denseRank().over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.salary.desc())
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "DENSE_RANK()"
        sql shouldContain "PARTITION BY"
        sql shouldContain "ORDER BY"
    }

    test("should create NTILE() function") {
        val params = mutableListOf<Any?>()

        val expr = ntile(4).over {
            orderBy(WinEmployees.salary.desc())
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "NTILE(?)"
        sql shouldContain "ORDER BY"
        params shouldBe listOf(4)
    }

    test("should create LAG() function") {
        val params = mutableListOf<Any?>()

        val expr = lag(WinEmployees.salary, 1).over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.hireDate)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "LAG(\"employees\".\"salary\", ?)"
        sql shouldContain "PARTITION BY"
        params shouldBe listOf(1)
    }

    test("should create LAG() function with default value") {
        val params = mutableListOf<Any?>()

        val expr = lag(WinEmployees.salary, 1, 0).over {
            orderBy(WinEmployees.hireDate)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "LAG(\"employees\".\"salary\", ?, ?)"
        params shouldBe listOf(1, 0)
    }

    test("should create LEAD() function") {
        val params = mutableListOf<Any?>()

        val expr = lead(WinEmployees.salary, 2).over {
            orderBy(WinEmployees.hireDate)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "LEAD(\"employees\".\"salary\", ?)"
        params shouldBe listOf(2)
    }

    test("should create FIRST_VALUE() function") {
        val params = mutableListOf<Any?>()

        val expr = firstValue(WinEmployees.name).over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.salary.desc())
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "FIRST_VALUE(\"employees\".\"name\")"
        sql shouldContain "OVER"
    }

    test("should create LAST_VALUE() function") {
        val params = mutableListOf<Any?>()

        val expr = lastValue(WinEmployees.salary).over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.hireDate)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "LAST_VALUE(\"employees\".\"salary\")"
    }

    test("should create SUM() as window function") {
        val params = mutableListOf<Any?>()

        val expr = sumOver(WinEmployees.salary).over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.hireDate)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "SUM(\"employees\".\"salary\")"
        sql shouldContain "OVER"
    }

    test("should create window function with frame clause") {
        val params = mutableListOf<Any?>()

        val expr = sumOver(WinEmployees.salary).over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.hireDate)
            rows(unboundedPreceding, currentRow)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    }

    test("should create window function with ROWS frame N PRECEDING") {
        val params = mutableListOf<Any?>()

        val expr = avgOver(WinEmployees.salary).over {
            orderBy(WinEmployees.hireDate)
            rows(preceding(3), following(1))
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING"
    }

    test("should create simple OVER() clause") {
        val params = mutableListOf<Any?>()

        val expr = rowNumber().over()

        val sql = expr.toSql(dialect, params)

        sql shouldBe "ROW_NUMBER() OVER ()"
    }

    test("should use window function in SELECT query") {
        val query = from(WinEmployees)
            .select(
                ColumnExpression(WinEmployees.name),
                ColumnExpression(WinEmployees.salary),
                rowNumber().over {
                    partitionBy(WinEmployees.department)
                    orderBy(WinEmployees.salary.desc())
                } alias "rank"
            )
            .build(dialect)

        query.sql shouldContain "SELECT"
        query.sql shouldContain "ROW_NUMBER()"
        query.sql shouldContain "OVER"
        query.sql shouldContain "AS \"rank\""
    }

    test("should create PERCENT_RANK() function") {
        val params = mutableListOf<Any?>()

        val expr = percentRank().over {
            orderBy(WinEmployees.salary.desc())
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "PERCENT_RANK()"
        sql shouldContain "OVER"
    }

    test("should create CUME_DIST() function") {
        val params = mutableListOf<Any?>()

        val expr = cumeDist().over {
            partitionBy(WinEmployees.department)
            orderBy(WinEmployees.salary)
        }

        val sql = expr.toSql(dialect, params)

        sql shouldContain "CUME_DIST()"
    }
})
