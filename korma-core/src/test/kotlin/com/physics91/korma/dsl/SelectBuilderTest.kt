package com.physics91.korma.dsl

import com.physics91.korma.expression.*
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Define tables at file level
object SelectTestUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val age = integer("age")
    val active = boolean("active")
}

object SelectTestPosts : Table("posts") {
    val id = long("id").primaryKey()
    val userId = long("user_id")
    val title = varchar("title", 200)
    val content = text("content")
}

class SelectBuilderTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    test("should build simple SELECT *") {
        val query = SelectBuilder(SelectTestUsers).build(dialect)

        query.sql shouldBe "SELECT * FROM \"users\""
        query.params shouldBe emptyList<Any?>()
    }

    test("should build SELECT with specific columns") {
        val query = SelectBuilder(SelectTestUsers)
            .select(SelectTestUsers.id, SelectTestUsers.name, SelectTestUsers.email)
            .build(dialect)

        query.sql shouldBe "SELECT \"users\".\"id\", \"users\".\"name\", \"users\".\"email\" FROM \"users\""
    }

    test("should build SELECT DISTINCT") {
        val query = SelectBuilder(SelectTestUsers)
            .select(SelectTestUsers.name)
            .distinct()
            .build(dialect)

        query.sql shouldBe "SELECT DISTINCT \"users\".\"name\" FROM \"users\""
    }

    test("should build SELECT with WHERE clause") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .where(ColumnExpression(SelectTestUsers.age) gt 18)
            .build(dialect)

        query.sql shouldBe "SELECT * FROM \"users\" WHERE \"users\".\"age\" > ?"
        query.params shouldBe listOf(18)
    }

    test("should build SELECT with complex WHERE clause") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .where {
                (ColumnExpression(SelectTestUsers.age) gt 18) and (ColumnExpression(SelectTestUsers.active) eq true)
            }
            .build(dialect)

        query.sql shouldContain "WHERE"
        query.sql shouldContain "AND"
        query.params shouldBe listOf(18, true)
    }

    test("should build SELECT with ORDER BY") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .orderBy(ColumnExpression(SelectTestUsers.name).asc(), ColumnExpression(SelectTestUsers.age).desc())
            .build(dialect)

        query.sql shouldContain "ORDER BY \"users\".\"name\" ASC, \"users\".\"age\" DESC"
    }

    test("should build SELECT with LIMIT and OFFSET") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .limit(10)
            .offset(20)
            .build(dialect)

        query.sql shouldContain "LIMIT 10"
        query.sql shouldContain "OFFSET 20"
    }

    test("should build SELECT with INNER JOIN") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .join(SelectTestPosts, ColumnExpression(SelectTestUsers.id) eq ColumnExpression(SelectTestPosts.userId))
            .build(dialect)

        query.sql shouldContain "INNER JOIN \"posts\""
        query.sql shouldContain "ON \"users\".\"id\" = \"posts\".\"user_id\""
    }

    test("should build SELECT with LEFT JOIN") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .leftJoin(SelectTestPosts, ColumnExpression(SelectTestUsers.id) eq ColumnExpression(SelectTestPosts.userId))
            .build(dialect)

        query.sql shouldContain "LEFT JOIN \"posts\""
    }

    test("should build SELECT with FOR UPDATE") {
        val query = SelectBuilder(SelectTestUsers)
            .selectAll()
            .where(ColumnExpression(SelectTestUsers.id) eq 1L)
            .forUpdate()
            .build(dialect)

        query.sql shouldContain "FOR UPDATE"
    }

    test("should use convenience function from()") {
        val query = from(SelectTestUsers)
            .select(SelectTestUsers.id, SelectTestUsers.name)
            .where { ColumnExpression(SelectTestUsers.active) eq true }
            .build(dialect)

        query.sql shouldContain "SELECT"
        query.sql shouldContain "FROM \"users\""
        query.sql shouldContain "WHERE"
    }
})
