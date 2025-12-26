package com.physics91.korma.dialect.sqlite

import com.physics91.korma.schema.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class SqliteDialectTest {

    // Test table
    object TestUsers : Table("users") {
        val id = long("id").primaryKey().autoIncrement()
        val name = varchar("name", 100).notNull()
        val email = varchar("email", 255).unique().notNull()
        val age = integer("age").nullable()
    }

    object CompositeKeyTable : Table("order_items") {
        val orderId = long("order_id")
        val productId = long("product_id")
        val quantity = integer("quantity")

        override val compositeKey = primaryKey(orderId, productId)
    }

    @Test
    fun `dialect name should be SQLite`() {
        assertEquals("SQLite", SqliteDialect.name)
    }

    @Test
    fun `should support SQLite-specific features`() {
        assertTrue(SqliteDialect.supportsReturning)  // SQLite 3.35.0+
        assertTrue(SqliteDialect.supportsOnConflict)
        assertFalse(SqliteDialect.supportsILike)
        assertTrue(SqliteDialect.supportsCTE)
        assertTrue(SqliteDialect.supportsWindowFunctions)
        assertTrue(SqliteDialect.supportsLimitOffset)
        assertFalse(SqliteDialect.supportsBooleanType)
        assertTrue(SqliteDialect.supportsUpsert)
    }

    @Test
    fun `should use double quotes for identifiers`() {
        assertEquals('"', SqliteDialect.identifierQuoteChar)
        assertEquals("\"users\"", SqliteDialect.quoteIdentifier("users"))
    }

    @Test
    fun `should map types to SQLite storage classes`() {
        // INTEGER affinity
        assertEquals("INTEGER", SqliteDialect.sqlTypeName(IntColumnType))
        assertEquals("INTEGER", SqliteDialect.sqlTypeName(LongColumnType))
        assertEquals("INTEGER", SqliteDialect.sqlTypeName(ShortColumnType))
        assertEquals("INTEGER", SqliteDialect.sqlTypeName(BooleanColumnType))

        // REAL affinity
        assertEquals("REAL", SqliteDialect.sqlTypeName(FloatColumnType))
        assertEquals("REAL", SqliteDialect.sqlTypeName(DoubleColumnType))

        // TEXT affinity
        assertEquals("TEXT", SqliteDialect.sqlTypeName(VarcharColumnType(100)))
        assertEquals("TEXT", SqliteDialect.sqlTypeName(TextColumnType))
        assertEquals("TEXT", SqliteDialect.sqlTypeName(TimestampColumnType))
        assertEquals("TEXT", SqliteDialect.sqlTypeName(DateColumnType))
        assertEquals("TEXT", SqliteDialect.sqlTypeName(UUIDColumnType))

        // BLOB affinity
        assertEquals("BLOB", SqliteDialect.sqlTypeName(BinaryColumnType))
        assertEquals("BLOB", SqliteDialect.sqlTypeName(BlobColumnType))

        // NUMERIC affinity
        assertEquals("NUMERIC", SqliteDialect.sqlTypeName(DecimalColumnType(10, 2)))
    }

    @Test
    fun `should generate INTEGER for auto-increment`() {
        assertEquals("INTEGER", SqliteDialect.autoIncrementType(IntColumnType))
        assertEquals("INTEGER", SqliteDialect.autoIncrementType(LongColumnType))
    }

    @Test
    fun `should generate CREATE TABLE with AUTOINCREMENT`() {
        val sql = SqliteDialect.createTableStatement(TestUsers)

        assertTrue(sql.contains("CREATE TABLE \"users\""))
        assertTrue(sql.contains("\"id\" INTEGER PRIMARY KEY AUTOINCREMENT"))
        assertTrue(sql.contains("\"name\" TEXT NOT NULL"))
        assertTrue(sql.contains("\"email\" TEXT NOT NULL UNIQUE"))
        assertTrue(sql.contains("\"age\" INTEGER"))
    }

    @Test
    fun `should generate CREATE TABLE IF NOT EXISTS`() {
        val sql = SqliteDialect.createTableStatement(TestUsers, ifNotExists = true)
        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS \"users\""))
    }

    @Test
    fun `should generate CREATE TABLE IF NOT EXISTS via helper`() {
        val sql = SqliteDialect.createTableIfNotExists(TestUsers)
        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS \"users\""))
    }

    @Test
    fun `should generate UPSERT clause`() {
        val upsert = SqliteDialect.upsert(
            table = TestUsers,
            conflictColumns = listOf(TestUsers.email),
            updateColumns = listOf(TestUsers.name, TestUsers.age)
        )
        assertTrue(upsert.contains("ON CONFLICT(\"email\")"))
        assertTrue(upsert.contains("DO UPDATE SET"))
        assertTrue(upsert.contains("\"name\" = excluded.\"name\""))
    }

    @Test
    fun `should generate UPSERT DO NOTHING`() {
        val upsert = SqliteDialect.upsert(
            table = TestUsers,
            conflictColumns = listOf(TestUsers.email),
            updateColumns = emptyList()
        )
        assertTrue(upsert.contains("ON CONFLICT(\"email\") DO NOTHING"))
    }

    @Test
    fun `should generate INSERT OR REPLACE`() {
        val sql = SqliteDialect.insertOrReplace(TestUsers)
        assertEquals("INSERT OR REPLACE INTO \"users\"", sql)
    }

    @Test
    fun `should generate INSERT OR IGNORE`() {
        val sql = SqliteDialect.insertOrIgnore(TestUsers)
        assertEquals("INSERT OR IGNORE INTO \"users\"", sql)
    }

    @Test
    fun `should generate DROP TABLE IF EXISTS`() {
        val sql = SqliteDialect.dropTableIfExists(TestUsers)
        assertEquals("DROP TABLE IF EXISTS \"users\"", sql)
    }

    @Test
    fun `should generate EXPLAIN QUERY PLAN`() {
        val explain = SqliteDialect.explainQueryPlan("SELECT * FROM users")
        assertEquals("EXPLAIN QUERY PLAN SELECT * FROM users", explain)
    }

    @Test
    fun `should generate VACUUM`() {
        assertEquals("VACUUM", SqliteDialect.vacuum())
    }

    @Test
    fun `should generate ANALYZE`() {
        assertEquals("ANALYZE", SqliteDialect.analyze())
        assertEquals("ANALYZE \"users\"", SqliteDialect.analyze(TestUsers))
    }

    @Test
    fun `should generate PRAGMA statements`() {
        assertEquals("PRAGMA foreign_keys", SqliteDialect.pragma("foreign_keys"))
        assertEquals("PRAGMA foreign_keys = ON", SqliteDialect.pragma("foreign_keys", "ON"))
        assertEquals("PRAGMA cache_size = 10000", SqliteDialect.pragma("cache_size", 10000))
    }

    @Test
    fun `should provide common pragmas`() {
        assertEquals("PRAGMA journal_mode = WAL", SqliteDialect.Pragmas.journalModeWal)
        assertEquals("PRAGMA synchronous = NORMAL", SqliteDialect.Pragmas.synchronousNormal)
        assertEquals("PRAGMA foreign_keys = ON", SqliteDialect.Pragmas.foreignKeysOn)
        assertEquals("PRAGMA foreign_keys = OFF", SqliteDialect.Pragmas.foreignKeysOff)
        assertEquals("PRAGMA cache_size = 10000", SqliteDialect.Pragmas.cacheSize(10000))
        assertEquals("PRAGMA busy_timeout = 5000", SqliteDialect.Pragmas.busyTimeout(5000))
    }

    @Test
    fun `should generate connection URLs`() {
        assertEquals("jdbc:sqlite:test.db", SqliteDialect.connectionUrl("test.db"))
        assertEquals("jdbc:sqlite:/path/to/db.sqlite", SqliteDialect.connectionUrl("/path/to/db.sqlite"))
    }

    @Test
    fun `should generate in-memory URLs`() {
        assertEquals("jdbc:sqlite::memory:", SqliteDialect.inMemoryUrl())
        assertEquals("jdbc:sqlite:file:testdb?mode=memory&cache=shared", SqliteDialect.sharedMemoryUrl("testdb"))
    }

    @Test
    fun `should use datetime for current timestamp`() {
        assertEquals("datetime('now')", SqliteDialect.currentTimestampExpression())
    }

    @Test
    fun `should use date for current date`() {
        assertEquals("date('now')", SqliteDialect.currentDateExpression())
    }

    @Test
    fun `should use time for current time`() {
        assertEquals("time('now')", SqliteDialect.currentTimeExpression())
    }

    @Test
    fun `should generate unix timestamp`() {
        assertEquals("strftime('%s', 'now')", SqliteDialect.unixTimestamp())
    }
}
