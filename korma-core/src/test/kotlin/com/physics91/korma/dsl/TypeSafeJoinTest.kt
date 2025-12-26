package com.physics91.korma.dsl

import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Test tables for type-safe join tests
object JoinTestUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val tenantId = long("tenant_id")
}

object JoinTestOrders : Table("orders") {
    val id = long("id").primaryKey()
    val userId = long("user_id")
    val orderNumber = varchar("order_number", 50)
    val tenantId = long("tenant_id")
    val total = decimal("total", 10, 2)
}

object JoinTestProducts : Table("products") {
    val id = long("id").primaryKey()
    val name = varchar("name", 200)
    val price = decimal("price", 10, 2)
}

object JoinTestOrderItems : Table("order_items") {
    val id = long("id").primaryKey()
    val orderId = long("order_id")
    val productId = long("product_id")
    val quantity = integer("quantity")
}

class TypeSafeJoinTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    context("typedJoin - INNER JOIN") {

        test("should build type-safe INNER JOIN with on()") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedJoin(JoinTestOrders).on(JoinTestUsers.id, JoinTestOrders.userId)
                .done()
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "ON \"users\".\"id\" = \"orders\".\"user_id\""
        }

        test("should build type-safe INNER JOIN with multiple conditions using and()") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedJoin(JoinTestOrders)
                    .on(JoinTestUsers.id, JoinTestOrders.userId)
                    .and(JoinTestUsers.tenantId, JoinTestOrders.tenantId)
                .done()
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "ON"
            query.sql shouldContain "\"users\".\"id\" = \"orders\".\"user_id\""
            query.sql shouldContain "AND"
            query.sql shouldContain "\"users\".\"tenant_id\" = \"orders\".\"tenant_id\""
        }

        test("should build type-safe INNER JOIN with lambda block") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedJoin(JoinTestOrders) { join ->
                    join.on(JoinTestUsers.id, JoinTestOrders.userId)
                }
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "ON \"users\".\"id\" = \"orders\".\"user_id\""
        }

        test("should support chaining back to SelectBuilder methods") {
            val query = SelectBuilder(JoinTestUsers)
                .typedJoin(JoinTestOrders).on(JoinTestUsers.id, JoinTestOrders.userId)
                .selectAll()
                .where { JoinTestUsers.name like "%test%" }
                .build(dialect)

            query.sql shouldContain "SELECT *"
            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "WHERE"
        }
    }

    context("typedLeftJoin - LEFT JOIN") {

        test("should build type-safe LEFT JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedLeftJoin(JoinTestOrders).on(JoinTestUsers.id, JoinTestOrders.userId)
                .done()
                .build(dialect)

            query.sql shouldContain "LEFT JOIN \"orders\""
            query.sql shouldContain "ON \"users\".\"id\" = \"orders\".\"user_id\""
        }

        test("should build type-safe LEFT JOIN with lambda") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedLeftJoin(JoinTestOrders) { join ->
                    join.on(JoinTestUsers.id, JoinTestOrders.userId)
                }
                .build(dialect)

            query.sql shouldContain "LEFT JOIN \"orders\""
        }
    }

    context("typedRightJoin - RIGHT JOIN") {

        test("should build type-safe RIGHT JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedRightJoin(JoinTestOrders).on(JoinTestUsers.id, JoinTestOrders.userId)
                .done()
                .build(dialect)

            query.sql shouldContain "RIGHT JOIN \"orders\""
            query.sql shouldContain "ON \"users\".\"id\" = \"orders\".\"user_id\""
        }
    }

    context("typedFullJoin - FULL OUTER JOIN") {

        test("should build type-safe FULL OUTER JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedFullJoin(JoinTestOrders).on(JoinTestUsers.id, JoinTestOrders.userId)
                .done()
                .build(dialect)

            query.sql shouldContain "FULL OUTER JOIN \"orders\""
            query.sql shouldContain "ON \"users\".\"id\" = \"orders\".\"user_id\""
        }
    }

    context("Quick type-safe join functions") {

        test("joinOn should build concise INNER JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .joinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId)
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "ON \"users\".\"id\" = \"orders\".\"user_id\""
        }

        test("leftJoinOn should build concise LEFT JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .leftJoinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId)
                .build(dialect)

            query.sql shouldContain "LEFT JOIN \"orders\""
        }

        test("rightJoinOn should build concise RIGHT JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .rightJoinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId)
                .build(dialect)

            query.sql shouldContain "RIGHT JOIN \"orders\""
        }

        test("fullJoinOn should build concise FULL OUTER JOIN") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .fullJoinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId)
                .build(dialect)

            query.sql shouldContain "FULL OUTER JOIN \"orders\""
        }
    }

    context("Multiple joins") {

        test("should support multiple type-safe joins") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .joinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId)
                .joinOn(JoinTestOrderItems, JoinTestOrders.id, JoinTestOrderItems.orderId)
                .joinOn(JoinTestProducts, JoinTestOrderItems.productId, JoinTestProducts.id)
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "INNER JOIN \"order_items\""
            query.sql shouldContain "INNER JOIN \"products\""
        }

        test("should support mixed join types") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedJoin(JoinTestOrders).on(JoinTestUsers.id, JoinTestOrders.userId).done()
                .leftJoinOn(JoinTestOrderItems, JoinTestOrders.id, JoinTestOrderItems.orderId)
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "LEFT JOIN \"order_items\""
        }
    }

    context("Join with alias") {

        test("should support table alias in type-safe join") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedJoin(JoinTestOrders, alias = "o").on(JoinTestUsers.id, JoinTestOrders.userId)
                .done()
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\" AS \"o\""
        }

        test("should support table alias in quick join") {
            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .joinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId, alias = "o")
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\" AS \"o\""
        }
    }

    context("Error handling") {

        test("should throw when on() is not called before done()") {
            shouldThrow<IllegalStateException> {
                SelectBuilder(JoinTestUsers)
                    .selectAll()
                    .typedJoin(JoinTestOrders)
                    .done()
            }
        }
    }

    context("Custom predicate support") {

        test("should support custom predicate via onCondition()") {
            val customCondition = (ColumnExpression(JoinTestUsers.id) eq ColumnExpression(JoinTestOrders.userId)) and
                    (ColumnExpression(JoinTestOrders.total) gt java.math.BigDecimal("100.00"))

            val query = SelectBuilder(JoinTestUsers)
                .selectAll()
                .typedJoin(JoinTestOrders).onCondition(customCondition)
                .done()
                .build(dialect)

            query.sql shouldContain "INNER JOIN \"orders\""
            query.sql shouldContain "ON"
            query.sql shouldContain "\"users\".\"id\" = \"orders\".\"user_id\""
            query.sql shouldContain "AND"
        }
    }

    // This test documents the type safety - it will NOT compile if you try:
    // .on(JoinTestUsers.id, JoinTestOrders.orderNumber)  // Long vs String = compile error!
    context("Type safety documentation") {

        test("type-safe join ensures column type compatibility") {
            // These compile because both columns are Long:
            val validQuery = SelectBuilder(JoinTestUsers)
                .selectAll()
                .joinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.userId)  // Long = Long OK
                .build(dialect)

            validQuery.sql shouldContain "INNER JOIN"

            // The following would NOT compile (documented for reference):
            // .joinOn(JoinTestOrders, JoinTestUsers.id, JoinTestOrders.orderNumber)
            // Error: Type mismatch. Required: Column<Long>, Found: Column<String>
        }
    }
})
