package com.physics91.korma.jdbc

import com.physics91.korma.dialect.h2.H2Dialect
import com.physics91.korma.expression.ColumnExpression
import com.physics91.korma.schema.LongIdTable
import com.physics91.korma.schema.Table
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

// Define tables at file level
object JdbcTestUsers : LongIdTable("users") {
    val name = varchar("name", 100).notNull()
    val email = varchar("email", 255).unique().notNull()
    val age = integer("age").nullable()
}

object TempTestTable : Table("temp_test_table") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
}

class JdbcDatabaseTest : FunSpec({

    val config = DatabaseConfig(
        jdbcUrl = "jdbc:h2:mem:testdb_${System.currentTimeMillis()};DB_CLOSE_DELAY=-1",
        username = "sa",
        password = ""
    )

    lateinit var database: JdbcDatabase

    beforeEach {
        database = JdbcDatabase(config, H2Dialect)
        database.createTable(JdbcTestUsers)
    }

    afterEach {
        database.dropTable(JdbcTestUsers)
        database.close()
    }

    test("should connect to H2 database") {
        database.isHealthy() shouldBe true
    }

    test("should create and drop tables") {
        database.createTable(TempTestTable)
        database.tableExists(TempTestTable) shouldBe true

        database.dropTable(TempTestTable)
        database.tableExists(TempTestTable) shouldBe false
    }

    test("should insert and select data") {
        database.transaction {
            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "John")
                set(JdbcTestUsers.email, "john@example.com")
                set(JdbcTestUsers.age, 25)
            }.execute()

            val users = from(JdbcTestUsers)
                .select(JdbcTestUsers.id, JdbcTestUsers.name, JdbcTestUsers.email, JdbcTestUsers.age)
                .fetch { row ->
                    mapOf(
                        "id" to row[JdbcTestUsers.id],
                        "name" to row[JdbcTestUsers.name],
                        "email" to row[JdbcTestUsers.email],
                        "age" to row[JdbcTestUsers.age]
                    )
                }

            users shouldHaveSize 1
            users[0]["name"] shouldBe "John"
            users[0]["email"] shouldBe "john@example.com"
            users[0]["age"] shouldBe 25
        }
    }

    test("should insert and get generated id") {
        val id = database.transaction {
            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Jane")
                set(JdbcTestUsers.email, "jane@example.com")
            }.executeAndGetId()
        }

        id shouldNotBe null
        id shouldBe 1L
    }

    test("should update data") {
        database.transaction {
            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "John")
                set(JdbcTestUsers.email, "john@example.com")
                set(JdbcTestUsers.age, 25)
            }.execute()

            val updated = update(JdbcTestUsers) {
                set(JdbcTestUsers.age, 26)
            }.where { ColumnExpression(JdbcTestUsers.name).eq("John") }
                .execute()

            updated shouldBe 1

            val user = from(JdbcTestUsers)
                .select(JdbcTestUsers.age)
                .where { ColumnExpression(JdbcTestUsers.name).eq("John") }
                .fetchFirst { row -> row[JdbcTestUsers.age] }

            user shouldBe 26
        }
    }

    test("should delete data") {
        database.transaction {
            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "John")
                set(JdbcTestUsers.email, "john@example.com")
            }.execute()

            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Jane")
                set(JdbcTestUsers.email, "jane@example.com")
            }.execute()

            val deleted = deleteFrom(JdbcTestUsers)
                .where { ColumnExpression(JdbcTestUsers.name).eq("John") }
                .execute()

            deleted shouldBe 1

            val count = from(JdbcTestUsers)
                .selectAll()
                .fetch { row -> row[JdbcTestUsers.id] }
                .size

            count shouldBe 1
        }
    }

    test("should support transaction rollback") {
        try {
            database.transaction {
                insertInto(JdbcTestUsers) {
                    set(JdbcTestUsers.name, "John")
                    set(JdbcTestUsers.email, "john@example.com")
                }.execute()

                throw RuntimeException("Force rollback")
            }
        } catch (_: RuntimeException) {
            // Expected
        }

        // Data should not be persisted
        database.transaction {
            val count = from(JdbcTestUsers)
                .selectAll()
                .fetch { row -> row[JdbcTestUsers.id] }
                .size

            count shouldBe 0
        }
    }

    test("should support WHERE conditions") {
        database.transaction {
            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "John")
                set(JdbcTestUsers.email, "john@example.com")
                set(JdbcTestUsers.age, 25)
            }.execute()

            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Jane")
                set(JdbcTestUsers.email, "jane@example.com")
                set(JdbcTestUsers.age, 30)
            }.execute()

            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Bob")
                set(JdbcTestUsers.email, "bob@example.com")
                set(JdbcTestUsers.age, 20)
            }.execute()

            val adults = from(JdbcTestUsers)
                .select(JdbcTestUsers.name)
                .where { ColumnExpression(JdbcTestUsers.age).gt(22) }
                .fetch { row -> row[JdbcTestUsers.name] }

            adults shouldHaveSize 2
        }
    }

    test("should support ORDER BY") {
        database.transaction {
            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Charlie")
                set(JdbcTestUsers.email, "charlie@example.com")
            }.execute()

            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Alice")
                set(JdbcTestUsers.email, "alice@example.com")
            }.execute()

            insertInto(JdbcTestUsers) {
                set(JdbcTestUsers.name, "Bob")
                set(JdbcTestUsers.email, "bob@example.com")
            }.execute()

            val names = from(JdbcTestUsers)
                .select(JdbcTestUsers.name)
                .orderBy(ColumnExpression(JdbcTestUsers.name).asc())
                .fetch { row -> row[JdbcTestUsers.name] }

            names shouldBe listOf("Alice", "Bob", "Charlie")
        }
    }

    test("should support LIMIT and OFFSET") {
        database.transaction {
            (1..10).forEach { i ->
                insertInto(JdbcTestUsers) {
                    set(JdbcTestUsers.name, "User$i")
                    set(JdbcTestUsers.email, "user$i@example.com")
                }.execute()
            }

            val page = from(JdbcTestUsers)
                .select(JdbcTestUsers.name)
                .orderBy(ColumnExpression(JdbcTestUsers.id).asc())
                .limit(3)
                .offset(3)
                .fetch { row -> row[JdbcTestUsers.name] }

            page shouldHaveSize 3
            page[0] shouldBe "User4"
        }
    }

    test("should support batch insert") {
        database.transaction {
            val users = listOf(
                "user1@example.com" to "User1",
                "user2@example.com" to "User2",
                "user3@example.com" to "User3"
            )

            batchInsertInto(JdbcTestUsers, users) { (email, name) ->
                set(JdbcTestUsers.email, email)
                set(JdbcTestUsers.name, name)
            }.execute()

            val count = from(JdbcTestUsers)
                .selectAll()
                .fetch { row -> row[JdbcTestUsers.id] }
                .size

            count shouldBe 3
        }
    }
})
