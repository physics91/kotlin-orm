package com.physics91.korma.spring

import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.schema.Table
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for KormaTemplate.
 */
class KormaTemplateTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DataSourceAutoConfiguration::class.java,
                DataSourceTransactionManagerAutoConfiguration::class.java,
                KormaAutoConfiguration::class.java,
                KormaRepositoryConfiguration::class.java
            )
        )
        .withPropertyValues(
            "spring.datasource.url=jdbc:h2:mem:kormatest${System.nanoTime()};DB_CLOSE_DELAY=-1",
            "spring.datasource.driver-class-name=org.h2.Driver",
            "korma.show-sql=true"
        )

    @Test
    fun `should create KormaTemplate bean`() {
        contextRunner.run { context ->
            assertNotNull(context.getBean(KormaTemplate::class.java))
        }
    }

    @Test
    fun `should execute raw SQL`() {
        contextRunner.run { context ->
            val template = context.getBean(KormaTemplate::class.java)

            // Create table
            template.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE
                )
            """.trimIndent())

            // Insert data
            val insertedRows = template.execute(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                "John Doe", "john@example.com"
            )
            assertEquals(1, insertedRows)

            // Query data
            val results = template.query(
                "SELECT * FROM users WHERE email = ?",
                "john@example.com"
            ) { row -> row }
            assertEquals(1, results.size)
            assertEquals("John Doe", results[0]["NAME"])

            // Cleanup
            template.execute("DROP TABLE users")
        }
    }

    @Test
    fun `should count records via raw SQL`() {
        contextRunner.run { context ->
            val template = context.getBean(KormaTemplate::class.java)

            // Create table
            template.execute("""
                CREATE TABLE IF NOT EXISTS count_test (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    num_value INT
                )
            """.trimIndent())

            // Insert records
            template.execute("INSERT INTO count_test (num_value) VALUES (1), (2), (3)")

            // Count
            val results = template.query("SELECT COUNT(*) as cnt FROM count_test") { row ->
                (row["CNT"] as Number).toLong()
            }
            assertEquals(3, results[0])

            // Cleanup
            template.execute("DROP TABLE count_test")
        }
    }

    @Test
    fun `should update records via raw SQL`() {
        contextRunner.run { context ->
            val template = context.getBean(KormaTemplate::class.java)

            // Create and populate table
            template.execute("""
                CREATE TABLE IF NOT EXISTS update_test (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """.trimIndent())

            template.execute("INSERT INTO update_test (name) VALUES ('old')")

            // Update
            val updated = template.execute(
                "UPDATE update_test SET name = ? WHERE name = ?",
                "new", "old"
            )
            assertEquals(1, updated)

            // Verify
            val results = template.query("SELECT name FROM update_test") { row ->
                row["NAME"] as String
            }
            assertEquals("new", results[0])

            // Cleanup
            template.execute("DROP TABLE update_test")
        }
    }

    @Test
    fun `should delete records via raw SQL`() {
        contextRunner.run { context ->
            val template = context.getBean(KormaTemplate::class.java)

            // Create table
            template.execute("""
                CREATE TABLE IF NOT EXISTS delete_test (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    data_value VARCHAR(50)
                )
            """.trimIndent())

            // Insert records
            template.execute("INSERT INTO delete_test (data_value) VALUES ('a'), ('b'), ('c')")

            // Delete one
            val deleted = template.execute("DELETE FROM delete_test WHERE data_value = ?", "a")
            assertEquals(1, deleted)

            // Verify count
            val results = template.query("SELECT COUNT(*) as cnt FROM delete_test") { row ->
                (row["CNT"] as Number).toLong()
            }
            assertEquals(2, results[0])

            // Cleanup
            template.execute("DROP TABLE delete_test")
        }
    }

    @Test
    fun `should work with typed table definitions`() {
        // Define test table inline
        val TestTable = object : Table("typed_test") {
            val id = long("id").primaryKey().autoIncrement()
            val name = varchar("name", 100).notNull()
        }

        contextRunner.run { context ->
            val template = context.getBean(KormaTemplate::class.java)

            // Create table
            template.createTable(TestTable)

            // Insert using template
            val id = template.insert(TestTable) { assignment ->
                assignment[TestTable.name] = "Test User"
            }
            assertTrue(id > 0)

            // Select all
            val users = template.selectAll(TestTable) { row ->
                row[TestTable.name]
            }
            assertEquals(1, users.size)
            assertEquals("Test User", users[0])

            // Count
            assertEquals(1, template.count(TestTable))

            // Cleanup
            template.dropTable(TestTable)
        }
    }

    @Test
    fun `should support batch insert`() {
        val TestTable = object : Table("batch_test") {
            val id = long("id").primaryKey().autoIncrement()
            val value = varchar("value", 100).notNull()
        }

        contextRunner.run { context ->
            val template = context.getBean(KormaTemplate::class.java)

            template.createTable(TestTable)

            // Batch insert
            val ids = template.insertBatch(TestTable, listOf(
                { it[TestTable.value] = "one" },
                { it[TestTable.value] = "two" },
                { it[TestTable.value] = "three" }
            ))

            assertEquals(3, ids.size)
            assertEquals(3, template.count(TestTable))

            template.dropTable(TestTable)
        }
    }
}
