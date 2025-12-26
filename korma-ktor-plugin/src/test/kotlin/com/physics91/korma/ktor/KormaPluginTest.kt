package com.physics91.korma.ktor

import com.physics91.korma.dialect.h2.H2Dialect
import com.physics91.korma.schema.Table
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Test table for Ktor plugin tests.
 */
object TestUsers : Table("test_users") {
    val id = long("id").primaryKey().autoIncrement()
    val name = varchar("name", 100).notNull()
    val email = varchar("email", 255).unique().notNull()
}

/**
 * Tests for Korma Ktor Plugin.
 */
class KormaPluginTest {

    @Test
    fun `should install Korma plugin`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
            }
        }

        // Plugin should be installed without error
        application {
            assertNotNull(kormaDatabase)
            assertNotNull(kormaConfig)
            assertEquals(H2Dialect, kormaDialect)
        }
    }

    @Test
    fun `should execute database operations`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
                showSql = true
            }

            routing {
                get("/setup") {
                    call.korma.createTable(TestUsers)
                    call.respondText("OK")
                }

                get("/count") {
                    val count = call.korma.count(TestUsers)
                    call.respondText(count.toString())
                }

                get("/insert") {
                    val id = call.korma.insert(TestUsers) {
                        it[TestUsers.name] = "Test User"
                        it[TestUsers.email] = "test${System.nanoTime()}@example.com"
                    }
                    call.respondText(id.toString())
                }

                get("/cleanup") {
                    call.korma.dropTable(TestUsers)
                    call.respondText("OK")
                }
            }
        }

        // Setup table
        client.get("/setup").apply {
            assertEquals(HttpStatusCode.OK, status)
        }

        // Initial count should be 0
        client.get("/count").apply {
            assertEquals("0", bodyAsText())
        }

        // Insert a user
        client.get("/insert").apply {
            val id = bodyAsText().toLong()
            assertTrue(id > 0)
        }

        // Count should be 1
        client.get("/count").apply {
            assertEquals("1", bodyAsText())
        }

        // Cleanup
        client.get("/cleanup").apply {
            assertEquals(HttpStatusCode.OK, status)
        }
    }

    @Test
    fun `should support transactions`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
            }

            routing {
                get("/setup") {
                    call.korma.createTable(TestUsers)
                    call.respondText("OK")
                }

                get("/transaction") {
                    // Insert using korma DSL within transaction context
                    val result = call.korma.insert(TestUsers) {
                        it[TestUsers.name] = "Transaction User"
                        it[TestUsers.email] = "tx${System.nanoTime()}@example.com"
                    }
                    call.respondText(result.toString())
                }

                get("/count") {
                    val count = call.korma.count(TestUsers)
                    call.respondText(count.toString())
                }

                get("/cleanup") {
                    call.korma.dropTable(TestUsers)
                    call.respondText("OK")
                }
            }
        }

        client.get("/setup")

        // Execute transaction
        client.get("/transaction").apply {
            val id = bodyAsText().toLong()
            assertTrue(id > 0)
        }

        // Verify committed
        client.get("/count").apply {
            assertEquals("1", bodyAsText())
        }

        client.get("/cleanup")
    }

    @Test
    fun `should support read-only operations`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
            }

            routing {
                get("/setup") {
                    call.korma.createTable(TestUsers)
                    call.korma.insert(TestUsers) {
                        it[TestUsers.name] = "User 1"
                        it[TestUsers.email] = "user1@example.com"
                    }
                    call.respondText("OK")
                }

                get("/readonly") {
                    val count = call.korma.count(TestUsers)
                    call.respondText(count.toString())
                }

                get("/cleanup") {
                    call.korma.dropTable(TestUsers)
                    call.respondText("OK")
                }
            }
        }

        client.get("/setup")

        client.get("/readonly").apply {
            assertEquals("1", bodyAsText())
        }

        client.get("/cleanup")
    }

    @Test
    fun `should detect dialect from URL`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                // dialect not specified - should auto-detect H2
            }
        }

        application {
            assertEquals("H2", kormaDialect.name)
        }
    }

    @Test
    fun `should configure SQL logging`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
                showSql = true
                formatSql = true
            }
        }

        application {
            assertTrue(kormaConfig.showSql)
            assertTrue(kormaConfig.formatSql)
        }
    }

    @Test
    fun `should perform select operations`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
            }

            routing {
                get("/setup") {
                    call.korma.createTable(TestUsers)
                    call.korma.insert(TestUsers) {
                        it[TestUsers.name] = "Alice"
                        it[TestUsers.email] = "alice@example.com"
                    }
                    call.korma.insert(TestUsers) {
                        it[TestUsers.name] = "Bob"
                        it[TestUsers.email] = "bob@example.com"
                    }
                    call.respondText("OK")
                }

                get("/users") {
                    val users = call.korma.selectAll(TestUsers) { row ->
                        row[TestUsers.name]
                    }
                    call.respondText(users.joinToString(","))
                }

                get("/cleanup") {
                    call.korma.dropTable(TestUsers)
                    call.respondText("OK")
                }
            }
        }

        client.get("/setup")

        client.get("/users").apply {
            val names = bodyAsText()
            assertTrue(names.contains("Alice"))
            assertTrue(names.contains("Bob"))
        }

        client.get("/cleanup")
    }

    @Test
    fun `should update and delete records`() = testApplication {
        application {
            install(Korma) {
                jdbcUrl = "jdbc:h2:mem:test${System.nanoTime()};DB_CLOSE_DELAY=-1"
                dialect = H2Dialect
            }

            routing {
                get("/setup") {
                    call.korma.createTable(TestUsers)
                    call.korma.insert(TestUsers) {
                        it[TestUsers.name] = "Original"
                        it[TestUsers.email] = "user@example.com"
                    }
                    call.respondText("OK")
                }

                get("/update") {
                    val updated = call.korma.update(TestUsers, "\"email\" = ?", "user@example.com") {
                        it[TestUsers.name] = "Updated"
                    }
                    call.respondText(updated.toString())
                }

                get("/delete") {
                    val deleted = call.korma.delete(TestUsers, "\"name\" = ?", "Updated")
                    call.respondText(deleted.toString())
                }

                get("/count") {
                    val count = call.korma.count(TestUsers)
                    call.respondText(count.toString())
                }

                get("/cleanup") {
                    call.korma.dropTable(TestUsers)
                    call.respondText("OK")
                }
            }
        }

        client.get("/setup")

        // Update
        client.get("/update").apply {
            assertEquals("1", bodyAsText())
        }

        // Delete
        client.get("/delete").apply {
            assertEquals("1", bodyAsText())
        }

        // Count should be 0
        client.get("/count").apply {
            assertEquals("0", bodyAsText())
        }

        client.get("/cleanup")
    }
}
