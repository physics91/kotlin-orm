package com.physics91.korma.test.fixtures

import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.schema.Table
import java.time.Instant
import java.util.*

/**
 * Builder for creating test data in a fluent way.
 *
 * Usage:
 * ```kotlin
 * val builder = TestDataBuilder(database)
 *
 * val userIds = builder.users(10) { index ->
 *     name = "User $index"
 *     email = "user$index@example.com"
 * }
 *
 * val postIds = builder.posts(userIds.first(), 5) { index ->
 *     title = "Post $index"
 * }
 * ```
 */
class TestDataBuilder(private val database: JdbcDatabase) {

    private val generatedIds = mutableMapOf<String, MutableList<Any>>()

    /**
     * Creates multiple rows in a table.
     *
     * @param table The table to insert into
     * @param count Number of rows to create
     * @param customizer Customization function for each row
     * @return List of generated IDs
     */
    fun <T : Table> insert(
        table: T,
        count: Int,
        customizer: RowBuilder.(Int) -> Unit
    ): List<Long> {
        val ids = mutableListOf<Long>()

        repeat(count) { index ->
            val builder = RowBuilder()
            builder.customizer(index)

            val columns = builder.getColumns()
            val values = builder.getValues()

            if (columns.isEmpty()) return@repeat

            val columnsStr = columns.joinToString(", ") { "\"$it\"" }
            val placeholders = values.map { "?" }.joinToString(", ")

            val sql = "INSERT INTO \"${table.tableName}\" ($columnsStr) VALUES ($placeholders)"

            val id = database.executeInsert(sql, values)
            if (id > 0) {
                ids.add(id)
            }
        }

        generatedIds.getOrPut(table.tableName) { mutableListOf() }.addAll(ids)
        return ids
    }

    /**
     * Creates a batch of rows efficiently.
     */
    fun <T : Table> batchInsert(
        table: T,
        items: List<Any>,
        mapper: RowBuilder.(Any, Int) -> Unit
    ): List<Long> {
        if (items.isEmpty()) return emptyList()

        val ids = mutableListOf<Long>()

        items.forEachIndexed { index, item ->
            val builder = RowBuilder()
            builder.mapper(item, index)

            val columns = builder.getColumns()
            val values = builder.getValues()

            if (columns.isEmpty()) return@forEachIndexed

            val columnsStr = columns.joinToString(", ") { "\"$it\"" }
            val placeholders = columns.map { "?" }.joinToString(", ")
            val sql = "INSERT INTO \"${table.tableName}\" ($columnsStr) VALUES ($placeholders)"

            val id = database.executeInsert(sql, values)
            if (id > 0) {
                ids.add(id)
            }
        }

        generatedIds.getOrPut(table.tableName) { mutableListOf() }.addAll(ids)
        return ids
    }

    /**
     * Gets all generated IDs for a table.
     */
    fun getGeneratedIds(table: Table): List<Any> {
        return generatedIds[table.tableName] ?: emptyList()
    }

    /**
     * Clears all data from the specified tables.
     */
    fun clearTables(vararg tables: Table) {
        tables.forEach { table ->
            database.executeUpdate("DELETE FROM \"${table.tableName}\"", emptyList())
        }
    }

    /**
     * Truncates tables (faster than DELETE but may not work with foreign keys).
     */
    fun truncateTables(vararg tables: Table) {
        tables.forEach { table ->
            try {
                database.executeUpdate("TRUNCATE TABLE \"${table.tableName}\" CASCADE", emptyList())
            } catch (e: Exception) {
                // Fallback to DELETE if TRUNCATE fails
                database.executeUpdate("DELETE FROM \"${table.tableName}\"", emptyList())
            }
        }
    }
}

/**
 * Builder for a single row of data.
 */
class RowBuilder {
    private val data = mutableMapOf<String, Any?>()

    operator fun set(column: String, value: Any?) {
        data[column] = value
    }

    /**
     * Sets a string value.
     */
    fun string(column: String, value: String) {
        data[column] = value
    }

    /**
     * Sets an integer value.
     */
    fun int(column: String, value: Int) {
        data[column] = value
    }

    /**
     * Sets a long value.
     */
    fun long(column: String, value: Long) {
        data[column] = value
    }

    /**
     * Sets a boolean value.
     */
    fun boolean(column: String, value: Boolean) {
        data[column] = value
    }

    /**
     * Sets a timestamp value to current time.
     */
    fun now(column: String) {
        data[column] = java.sql.Timestamp.from(Instant.now())
    }

    /**
     * Sets a UUID value.
     */
    fun uuid(column: String, value: UUID = UUID.randomUUID()) {
        data[column] = value
    }

    /**
     * Sets a null value.
     */
    fun nullValue(column: String) {
        data[column] = null
    }

    /**
     * Generates a random email.
     */
    fun randomEmail(column: String, prefix: String = "user") {
        data[column] = "$prefix${UUID.randomUUID().toString().take(8)}@example.com"
    }

    /**
     * Generates a random string.
     */
    fun randomString(column: String, length: Int = 10) {
        val chars = ('a'..'z') + ('A'..'Z') + ('0'..'9')
        data[column] = (1..length).map { chars.random() }.joinToString("")
    }

    internal fun getColumns(): List<String> = data.keys.toList()
    internal fun getValues(): List<Any?> = data.values.toList()
}

/**
 * DSL function to create test data.
 *
 * Usage:
 * ```kotlin
 * testData(database) {
 *     insert(UsersTable, 10) { index ->
 *         this["name"] = "User $index"
 *         this["email"] = "user$index@example.com"
 *     }
 * }
 * ```
 */
fun testData(database: JdbcDatabase, block: TestDataBuilder.() -> Unit): TestDataBuilder {
    return TestDataBuilder(database).apply(block)
}
