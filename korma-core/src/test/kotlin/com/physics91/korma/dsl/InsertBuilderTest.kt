package com.physics91.korma.dsl

import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Define tables at file level
object InsertTestUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val age = integer("age")
}

class InsertBuilderTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override val supportsOnConflict = true
        override val supportsReturning = true
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    test("should build simple INSERT") {
        val query = InsertBuilder(InsertTestUsers)
            .set(InsertTestUsers.name, "John")
            .set(InsertTestUsers.email, "john@example.com")
            .set(InsertTestUsers.age, 25)
            .build(dialect)

        query.sql shouldContain "INSERT INTO \"users\""
        query.sql shouldContain "VALUES (?, ?, ?)"
        query.params shouldBe listOf("John", "john@example.com", 25)
    }

    test("should build INSERT with DSL syntax") {
        val query = insertInto(InsertTestUsers) {
            set(InsertTestUsers.name, "Jane")
            set(InsertTestUsers.email, "jane@example.com")
        }.build(dialect)

        query.sql shouldContain "INSERT INTO \"users\""
        query.params.size shouldBe 2
    }

    test("should build INSERT with ON CONFLICT DO NOTHING") {
        val query = InsertBuilder(InsertTestUsers)
            .set(InsertTestUsers.name, "John")
            .set(InsertTestUsers.email, "john@example.com")
            .onConflict(InsertTestUsers.email).doNothing()
            .build(dialect)

        query.sql shouldContain "ON CONFLICT"
        query.sql shouldContain "DO NOTHING"
    }

    test("should build INSERT with ON CONFLICT DO UPDATE") {
        val query = InsertBuilder(InsertTestUsers)
            .set(InsertTestUsers.name, "John")
            .set(InsertTestUsers.email, "john@example.com")
            .set(InsertTestUsers.age, 25)
            .onConflict(InsertTestUsers.email).doUpdate(InsertTestUsers.name, InsertTestUsers.age)
            .build(dialect)

        query.sql shouldContain "ON CONFLICT"
        query.sql shouldContain "DO UPDATE SET"
    }

    test("should build INSERT with RETURNING") {
        val query = InsertBuilder(InsertTestUsers)
            .set(InsertTestUsers.name, "John")
            .set(InsertTestUsers.email, "john@example.com")
            .returning(InsertTestUsers.id)
            .build(dialect)

        query.sql shouldContain "RETURNING \"id\""
    }

    test("should build batch INSERT") {
        val query = BatchInsertBuilder(InsertTestUsers)
            .addRow {
                set(InsertTestUsers.name, "John")
                set(InsertTestUsers.email, "john@example.com")
            }
            .addRow {
                set(InsertTestUsers.name, "Jane")
                set(InsertTestUsers.email, "jane@example.com")
            }
            .build(dialect)

        query.size shouldBe 1 // Multi-row insert
        query[0].sql shouldContain "VALUES"
        query[0].params.size shouldBe 4
    }

    test("should build batch INSERT from collection") {
        data class UserData(val name: String, val email: String)

        val users = listOf(
            UserData("John", "john@example.com"),
            UserData("Jane", "jane@example.com")
        )

        val query = batchInsertInto(InsertTestUsers, users) { user ->
            set(InsertTestUsers.name, user.name)
            set(InsertTestUsers.email, user.email)
        }.build(dialect)

        query.size shouldBe 1
        query[0].params.size shouldBe 4
    }
})
