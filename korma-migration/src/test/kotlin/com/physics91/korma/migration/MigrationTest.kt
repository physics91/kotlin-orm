package com.physics91.korma.migration

import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*

/**
 * Tests for the Migration module.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MigrationTest {

    private val dialect = object : BaseSqlDialect() {
        override val name = "H2"
        override val supportsReturning = true
        override fun autoIncrementType(baseType: ColumnType<*>): String = "BIGINT AUTO_INCREMENT"
    }

    // ============== TableBuilder Tests ==============

    @Test
    fun `should build table with columns`() {
        val builder = TableBuilder("users")
        builder.apply {
            long("id").primaryKey().autoIncrement()
            varchar("name", 100)
            varchar("email", 255).unique()
            integer("age").nullable()
        }

        val tableInfo = builder.build()

        assertEquals("users", tableInfo.tableName)
        assertEquals(4, tableInfo.columns.size)

        val idColumn = tableInfo.columns.find { it.name == "id" }
        assertNotNull(idColumn)
        assertTrue(idColumn!!.isPrimaryKey)
        assertTrue(idColumn.isAutoIncrement)

        val emailColumn = tableInfo.columns.find { it.name == "email" }
        assertNotNull(emailColumn)
        assertTrue(emailColumn!!.isUnique)
    }

    // ============== MigrationOperation Tests ==============

    @Test
    fun `should generate CREATE TABLE SQL`() {
        val builder = TableBuilder("posts")
        builder.apply {
            long("id").primaryKey().autoIncrement()
            varchar("title", 200)
            text("content")
        }

        val operation = MigrationOperation.CreateTable(builder.build())
        val sql = operation.toSql(dialect)

        assertTrue(sql.contains("CREATE TABLE"))
        assertTrue(sql.contains("\"posts\""))
        assertTrue(sql.contains("\"id\""))
        assertTrue(sql.contains("\"title\""))
        assertTrue(sql.contains("\"content\""))
    }

    @Test
    fun `should generate DROP TABLE SQL`() {
        val operation = MigrationOperation.DropTable("users")
        val sql = operation.toSql(dialect)

        assertEquals("DROP TABLE \"users\"", sql)
    }

    @Test
    fun `should generate RENAME TABLE SQL`() {
        val operation = MigrationOperation.RenameTable("old_users", "new_users")
        val sql = operation.toSql(dialect)

        assertEquals("ALTER TABLE \"old_users\" RENAME TO \"new_users\"", sql)
    }

    @Test
    fun `should generate CREATE INDEX SQL`() {
        val operation = MigrationOperation.CreateIndex(
            indexName = "idx_users_email",
            tableName = "users",
            columns = listOf("email"),
            unique = false
        )
        val sql = operation.toSql(dialect)

        assertEquals("CREATE INDEX \"idx_users_email\" ON \"users\" (\"email\")", sql)
    }

    @Test
    fun `should generate CREATE UNIQUE INDEX SQL`() {
        val operation = MigrationOperation.CreateIndex(
            indexName = "idx_users_email_unique",
            tableName = "users",
            columns = listOf("email"),
            unique = true
        )
        val sql = operation.toSql(dialect)

        assertEquals("CREATE UNIQUE INDEX \"idx_users_email_unique\" ON \"users\" (\"email\")", sql)
    }

    // ============== MigrationContext Tests ==============

    @Test
    fun `should collect operations in context`() {
        val executor = TestMigrationExecutor()
        val context = MigrationContext(dialect, executor)

        context.apply {
            createTable("users") {
                long("id").primaryKey().autoIncrement()
                varchar("name", 100)
            }
            createIndex("idx_users_name", "users", listOf("name"))
        }

        val operations = context.getOperations()
        assertEquals(2, operations.size)
        assertTrue(operations[0] is MigrationOperation.CreateTable)
        assertTrue(operations[1] is MigrationOperation.CreateIndex)
    }

    @Test
    fun `should generate SQL from context`() {
        val executor = TestMigrationExecutor()
        val context = MigrationContext(dialect, executor)

        context.apply {
            createTable("products") {
                long("id").primaryKey().autoIncrement()
                varchar("name", 200)
                decimal("price", 10, 2)
            }
        }

        val sqlList = context.generateSql()
        assertEquals(1, sqlList.size)
        assertTrue(sqlList[0].contains("CREATE TABLE"))
        assertTrue(sqlList[0].contains("\"products\""))
    }

    // ============== Migration Interface Tests ==============

    @Test
    fun `should define migration with up and down`() {
        val migration = object : BaseMigration("V1", "Create users table") {
            override fun MigrationContext.up() {
                createTable("users") {
                    long("id").primaryKey().autoIncrement()
                    varchar("email", 255).unique()
                }
            }

            override fun MigrationContext.down() {
                dropTable("users")
            }
        }

        assertEquals("V1", migration.version)
        assertEquals("Create users table", migration.description)

        // Test up
        val executor = TestMigrationExecutor()
        val upContext = MigrationContext(dialect, executor)
        upContext.apply { migration.run { up() } }
        assertEquals(1, upContext.getOperations().size)

        // Test down
        val downContext = MigrationContext(dialect, executor)
        downContext.apply { migration.run { down() } }
        assertEquals(1, downContext.getOperations().size)
        assertTrue(downContext.getOperations()[0] is MigrationOperation.DropTable)
    }

    // ============== ForeignKey Tests ==============

    @Test
    fun `should generate ADD FOREIGN KEY SQL`() {
        val operation = MigrationOperation.AddForeignKey(
            constraintName = "fk_posts_user",
            tableName = "posts",
            column = "user_id",
            referencedTable = "users",
            referencedColumn = "id",
            onDelete = ForeignKeyAction.CASCADE
        )
        val sql = operation.toSql(dialect)

        assertTrue(sql.contains("ALTER TABLE"))
        assertTrue(sql.contains("FOREIGN KEY"))
        assertTrue(sql.contains("REFERENCES"))
        assertTrue(sql.contains("ON DELETE CASCADE"))
    }

    // ============== ColumnBuilder Tests ==============

    @Test
    fun `should build column with DSL`() {
        val builder = ColumnBuilder("status")
        builder.varchar(50).notNull().default("active")

        val column = builder.build()

        assertEquals("status", column.name)
        assertFalse(column.isNullable)
        assertEquals("active", column.defaultValue)
    }

    // ============== Test Helper ==============

    class TestMigrationExecutor : MigrationExecutor {
        val executedSql = mutableListOf<String>()

        override fun execute(sql: String) {
            executedSql.add(sql)
        }

        override fun executeQuery(sql: String): List<Map<String, Any?>> {
            return emptyList()
        }
    }
}
