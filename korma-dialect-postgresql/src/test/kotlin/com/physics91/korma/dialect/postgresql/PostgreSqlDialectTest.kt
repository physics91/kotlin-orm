package com.physics91.korma.dialect.postgresql

import com.physics91.korma.schema.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class PostgreSqlDialectTest {

    // Test table with PostgreSQL-specific types
    object TestUsers : Table("users") {
        val id = long("id").primaryKey().autoIncrement()
        val name = varchar("name", 100).notNull()
        val email = varchar("email", 255).unique().notNull()
        val age = integer("age").nullable()
    }

    object PostgresSpecificTable : Table("pg_test") {
        val id = long("id").primaryKey().autoIncrement()
        val data = json("data")
        val metadata = jsonb("metadata")
        val tags = textArray("tags")
        val scores = intArray("scores")
        val ipAddress = inet("ip_address")
        val duration = interval("duration")
    }

    @Test
    fun `dialect name should be PostgreSQL`() {
        assertEquals("PostgreSQL", PostgreSqlDialect.name)
    }

    @Test
    fun `should support PostgreSQL-specific features`() {
        assertTrue(PostgreSqlDialect.supportsReturning)
        assertTrue(PostgreSqlDialect.supportsOnConflict)
        assertTrue(PostgreSqlDialect.supportsILike)
        assertTrue(PostgreSqlDialect.supportsCTE)
        assertTrue(PostgreSqlDialect.supportsWindowFunctions)
        assertTrue(PostgreSqlDialect.supportsLimitOffset)
        assertTrue(PostgreSqlDialect.supportsBooleanType)
    }

    @Test
    fun `should use double quotes for identifiers`() {
        assertEquals('"', PostgreSqlDialect.identifierQuoteChar)
        assertEquals("\"users\"", PostgreSqlDialect.quoteIdentifier("users"))
        assertEquals("\"user_name\"", PostgreSqlDialect.quoteIdentifier("user_name"))
    }

    @Test
    fun `should map types to PostgreSQL syntax`() {
        assertEquals("INTEGER", PostgreSqlDialect.sqlTypeName(IntColumnType))
        assertEquals("BIGINT", PostgreSqlDialect.sqlTypeName(LongColumnType))
        assertEquals("BOOLEAN", PostgreSqlDialect.sqlTypeName(BooleanColumnType))
        assertEquals("VARCHAR(100)", PostgreSqlDialect.sqlTypeName(VarcharColumnType(100)))
        assertEquals("TEXT", PostgreSqlDialect.sqlTypeName(TextColumnType))
        assertEquals("TIMESTAMP WITH TIME ZONE", PostgreSqlDialect.sqlTypeName(TimestampColumnType))
        assertEquals("BYTEA", PostgreSqlDialect.sqlTypeName(BinaryColumnType))
        assertEquals("UUID", PostgreSqlDialect.sqlTypeName(UUIDColumnType))
    }

    @Test
    fun `should map PostgreSQL-specific types`() {
        assertEquals("JSON", PostgreSqlDialect.sqlTypeName(JsonColumnType()))
        assertEquals("JSONB", PostgreSqlDialect.sqlTypeName(JsonbColumnType()))
        assertEquals("INET", PostgreSqlDialect.sqlTypeName(InetColumnType()))
        assertEquals("INTERVAL", PostgreSqlDialect.sqlTypeName(IntervalColumnType()))
        assertEquals("INTEGER[]", PostgreSqlDialect.sqlTypeName(ArrayColumnType(IntColumnType)))
        assertEquals("TEXT[]", PostgreSqlDialect.sqlTypeName(ArrayColumnType(TextColumnType)))
    }

    @Test
    fun `should generate SERIAL for auto-increment`() {
        assertEquals("SERIAL", PostgreSqlDialect.autoIncrementType(IntColumnType))
        assertEquals("BIGSERIAL", PostgreSqlDialect.autoIncrementType(LongColumnType))
        assertEquals("SMALLSERIAL", PostgreSqlDialect.autoIncrementType(ShortColumnType))
    }

    @Test
    fun `should generate CREATE TABLE statement`() {
        val sql = PostgreSqlDialect.createTableStatement(TestUsers)

        assertTrue(sql.contains("CREATE TABLE \"users\""))
        assertTrue(sql.contains("\"id\" BIGSERIAL"))
        assertTrue(sql.contains("\"name\" VARCHAR(100) NOT NULL"))
        assertTrue(sql.contains("\"email\" VARCHAR(255) NOT NULL UNIQUE"))
        assertTrue(sql.contains("\"age\" INTEGER"))
    }

    @Test
    fun `should generate CREATE TABLE IF NOT EXISTS`() {
        val sql = PostgreSqlDialect.createTableStatement(TestUsers, ifNotExists = true)
        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS \"users\""))
    }

    @Test
    fun `should generate RETURNING clause`() {
        val returning = PostgreSqlDialect.returningClause(listOf(TestUsers.id, TestUsers.name))
        assertEquals("RETURNING \"id\", \"name\"", returning)
    }

    @Test
    fun `should generate ON CONFLICT clause`() {
        val onConflict = PostgreSqlDialect.onConflictClause(
            conflictColumns = listOf(TestUsers.email),
            updateColumns = listOf(TestUsers.name, TestUsers.age)
        )
        assertTrue(onConflict.contains("ON CONFLICT (\"email\")"))
        assertTrue(onConflict.contains("DO UPDATE SET"))
        assertTrue(onConflict.contains("\"name\" = EXCLUDED.\"name\""))
    }

    @Test
    fun `should generate EXPLAIN ANALYZE`() {
        val explain = PostgreSqlDialect.explainAnalyze("SELECT * FROM users")
        assertEquals("EXPLAIN ANALYZE SELECT * FROM users", explain)
    }

    @Test
    fun `should generate TRUNCATE with options`() {
        val truncate = PostgreSqlDialect.truncateTable(TestUsers, cascade = true, restartIdentity = true)
        assertEquals("TRUNCATE TABLE \"users\" RESTART IDENTITY CASCADE", truncate)
    }

    @Test
    fun `should generate connection URL`() {
        val url = PostgreSqlDialect.connectionUrl("localhost", 5432, "mydb")
        assertEquals("jdbc:postgresql://localhost:5432/mydb", url)
    }

    @Test
    fun `should use NOW() for current timestamp`() {
        assertEquals("NOW()", PostgreSqlDialect.currentTimestampExpression())
    }

    @Test
    fun `should use CURRENT_DATE for current date`() {
        assertEquals("CURRENT_DATE", PostgreSqlDialect.currentDateExpression())
    }
}
