package com.physics91.korma.dialect.mysql

import com.physics91.korma.schema.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class MySqlDialectTest {

    // Test table
    object TestUsers : Table("users") {
        val id = long("id").primaryKey().autoIncrement()
        val name = varchar("name", 100).notNull()
        val email = varchar("email", 255).unique().notNull()
        val age = integer("age").nullable()
    }

    object MySqlSpecificTable : Table("mysql_test") {
        val id = long("id").primaryKey().autoIncrement()
        val tiny = tinyInt("tiny_val")
        val medium = mediumInt("medium_val")
        val shortText = tinyText("short_text")
        val longContent = longText("long_content")
        val data = json("data")
        val status = enum("status", "ACTIVE", "INACTIVE", "PENDING")
        val permissions = set("permissions", "READ", "WRITE", "DELETE")
        val createdYear = year("created_year")
        val flags = bit("flags", 8)
    }

    @Test
    fun `dialect name should be MySQL`() {
        assertEquals("MySQL", MySqlDialect.name)
    }

    @Test
    fun `should support MySQL-specific features`() {
        assertFalse(MySqlDialect.supportsReturning)
        assertFalse(MySqlDialect.supportsOnConflict)
        assertFalse(MySqlDialect.supportsILike)
        assertTrue(MySqlDialect.supportsCTE)
        assertTrue(MySqlDialect.supportsWindowFunctions)
        assertTrue(MySqlDialect.supportsLimitOffset)
        assertFalse(MySqlDialect.supportsBooleanType)
        assertTrue(MySqlDialect.supportsOnDuplicateKeyUpdate)
    }

    @Test
    fun `should use backticks for identifiers`() {
        assertEquals('`', MySqlDialect.identifierQuoteChar)
        assertEquals("`users`", MySqlDialect.quoteIdentifier("users"))
        assertEquals("`user_name`", MySqlDialect.quoteIdentifier("user_name"))
    }

    @Test
    fun `should map types to MySQL syntax`() {
        assertEquals("INT", MySqlDialect.sqlTypeName(IntColumnType))
        assertEquals("BIGINT", MySqlDialect.sqlTypeName(LongColumnType))
        assertEquals("TINYINT(1)", MySqlDialect.sqlTypeName(BooleanColumnType))
        assertEquals("VARCHAR(100)", MySqlDialect.sqlTypeName(VarcharColumnType(100)))
        assertEquals("TEXT", MySqlDialect.sqlTypeName(TextColumnType))
        assertEquals("TIMESTAMP", MySqlDialect.sqlTypeName(TimestampColumnType))
        assertEquals("DATETIME", MySqlDialect.sqlTypeName(DateTimeColumnType))
        assertEquals("CHAR(36)", MySqlDialect.sqlTypeName(UUIDColumnType))
    }

    @Test
    fun `should map MySQL-specific types`() {
        assertEquals("TINYINT", MySqlDialect.sqlTypeName(TinyIntColumnType()))
        assertEquals("MEDIUMINT", MySqlDialect.sqlTypeName(MediumIntColumnType()))
        assertEquals("TINYTEXT", MySqlDialect.sqlTypeName(TinyTextColumnType()))
        assertEquals("MEDIUMTEXT", MySqlDialect.sqlTypeName(MediumTextColumnType()))
        assertEquals("LONGTEXT", MySqlDialect.sqlTypeName(LongTextColumnType()))
        assertEquals("JSON", MySqlDialect.sqlTypeName(JsonColumnType()))
        assertEquals("YEAR", MySqlDialect.sqlTypeName(YearColumnType()))
        assertEquals("BIT(8)", MySqlDialect.sqlTypeName(BitColumnType(8)))
    }

    @Test
    fun `should map ENUM type`() {
        val enumType = EnumColumnType(listOf("ACTIVE", "INACTIVE"))
        assertEquals("ENUM('ACTIVE','INACTIVE')", MySqlDialect.sqlTypeName(enumType))
    }

    @Test
    fun `should map SET type`() {
        val setType = SetColumnType(listOf("READ", "WRITE"))
        assertEquals("SET('READ','WRITE')", MySqlDialect.sqlTypeName(setType))
    }

    @Test
    fun `should generate CREATE TABLE with InnoDB engine`() {
        val sql = MySqlDialect.createTableStatement(TestUsers)

        assertTrue(sql.contains("CREATE TABLE `users`"))
        assertTrue(sql.contains("`id` BIGINT"))
        assertTrue(sql.contains("AUTO_INCREMENT"))
        assertTrue(sql.contains("`name` VARCHAR(100) NOT NULL"))
        assertTrue(sql.contains("ENGINE=InnoDB"))
        assertTrue(sql.contains("utf8mb4"))
    }

    @Test
    fun `should generate CREATE TABLE IF NOT EXISTS`() {
        val sql = MySqlDialect.createTableStatement(TestUsers, ifNotExists = true)
        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS `users`"))
    }

    @Test
    fun `should generate ON DUPLICATE KEY UPDATE`() {
        val onDuplicate = MySqlDialect.onDuplicateKeyUpdate(listOf(TestUsers.name, TestUsers.age))
        assertTrue(onDuplicate.contains("ON DUPLICATE KEY UPDATE"))
        assertTrue(onDuplicate.contains("`name` = VALUES(`name`)"))
        assertTrue(onDuplicate.contains("`age` = VALUES(`age`)"))
    }

    @Test
    fun `should generate INSERT IGNORE`() {
        val insertIgnore = MySqlDialect.insertIgnore(TestUsers)
        assertEquals("INSERT IGNORE INTO `users`", insertIgnore)
    }

    @Test
    fun `should generate REPLACE INTO`() {
        val replace = MySqlDialect.replaceInto(TestUsers)
        assertEquals("REPLACE INTO `users`", replace)
    }

    @Test
    fun `should generate LAST_INSERT_ID`() {
        assertEquals("LAST_INSERT_ID()", MySqlDialect.lastInsertId())
    }

    @Test
    fun `should generate EXPLAIN`() {
        val explain = MySqlDialect.explain("SELECT * FROM users")
        assertEquals("EXPLAIN SELECT * FROM users", explain)
    }

    @Test
    fun `should generate EXPLAIN ANALYZE`() {
        val explain = MySqlDialect.explainAnalyze("SELECT * FROM users")
        assertEquals("EXPLAIN ANALYZE SELECT * FROM users", explain)
    }

    @Test
    fun `should generate connection URL`() {
        val url = MySqlDialect.connectionUrl(
            host = "localhost",
            port = 3306,
            database = "mydb",
            useSSL = true,
            serverTimezone = "Asia/Seoul"
        )
        assertTrue(url.contains("jdbc:mysql://localhost:3306/mydb"))
        assertTrue(url.contains("useSSL=true"))
        assertTrue(url.contains("serverTimezone=Asia/Seoul"))
        assertTrue(url.contains("characterEncoding=UTF-8"))
    }

    @Test
    fun `should use NOW() for current timestamp`() {
        assertEquals("NOW()", MySqlDialect.currentTimestampExpression())
    }

    @Test
    fun `should use CURDATE() for current date`() {
        assertEquals("CURDATE()", MySqlDialect.currentDateExpression())
    }
}
