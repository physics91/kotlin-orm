package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import com.physics91.korma.codegen.config.NamingStrategy
import org.junit.jupiter.api.Test
import java.sql.Types
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KotlinCodeGeneratorTest {

    private fun createConfig(
        packageName: String = "com.example.generated",
        generateEntities: Boolean = true,
        generateKdoc: Boolean = true,
        useNullableTypes: Boolean = true,
        namingStrategy: NamingStrategy = NamingStrategy.Default
    ): CodegenConfig {
        return CodegenConfig(
            packageName = packageName,
            databaseConfig = DatabaseConfig(url = "jdbc:h2:mem:test"),
            generateEntities = generateEntities,
            generateKdoc = generateKdoc,
            useNullableTypes = useNullableTypes,
            namingStrategy = namingStrategy
        )
    }

    private fun createTestSchema(): DatabaseSchema {
        val usersTable = TableMetadata(
            name = "users",
            schema = null,
            remarks = "User accounts table",
            columns = listOf(
                ColumnMetadata(
                    name = "id",
                    sqlType = Types.BIGINT,
                    typeName = "BIGINT",
                    size = 0,
                    decimalDigits = 0,
                    nullable = false,
                    defaultValue = null,
                    remarks = "Primary key",
                    ordinalPosition = 1,
                    autoIncrement = true
                ),
                ColumnMetadata(
                    name = "username",
                    sqlType = Types.VARCHAR,
                    typeName = "VARCHAR",
                    size = 50,
                    decimalDigits = 0,
                    nullable = false,
                    defaultValue = null,
                    remarks = "Username",
                    ordinalPosition = 2,
                    autoIncrement = false
                ),
                ColumnMetadata(
                    name = "email",
                    sqlType = Types.VARCHAR,
                    typeName = "VARCHAR",
                    size = 100,
                    decimalDigits = 0,
                    nullable = true,
                    defaultValue = null,
                    remarks = null,
                    ordinalPosition = 3,
                    autoIncrement = false
                ),
                ColumnMetadata(
                    name = "is_active",
                    sqlType = Types.BOOLEAN,
                    typeName = "BOOLEAN",
                    size = 0,
                    decimalDigits = 0,
                    nullable = false,
                    defaultValue = "TRUE",
                    remarks = null,
                    ordinalPosition = 4,
                    autoIncrement = false
                ),
                ColumnMetadata(
                    name = "balance",
                    sqlType = Types.DECIMAL,
                    typeName = "DECIMAL",
                    size = 10,
                    decimalDigits = 2,
                    nullable = false,
                    defaultValue = "0.00",
                    remarks = null,
                    ordinalPosition = 5,
                    autoIncrement = false
                ),
                ColumnMetadata(
                    name = "created_at",
                    sqlType = Types.TIMESTAMP,
                    typeName = "TIMESTAMP",
                    size = 0,
                    decimalDigits = 0,
                    nullable = true,
                    defaultValue = null,
                    remarks = null,
                    ordinalPosition = 6,
                    autoIncrement = false
                )
            ),
            primaryKeyColumns = listOf("id"),
            foreignKeys = emptyList(),
            uniqueConstraints = emptyList(),
            indices = emptyList()
        )

        return DatabaseSchema(listOf(usersTable))
    }

    @Test
    fun `generates Table object with correct name`() {
        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)

        val tableFile = files.find { it.name == "Users" }
        assertTrue(tableFile != null)
        assertEquals("com.example.generated", tableFile.packageName)

        val code = tableFile.toString()
        assertContains(code, "object Users : Table(\"users\")")
    }

    @Test
    fun `generates column properties with correct types`() {
        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "Users" }!!
        val code = tableFile.toString()

        assertContains(code, "long(\"id\")")
        assertContains(code, "varchar(\"username\", 50)")
        assertContains(code, "boolean(\"is_active\")")
        assertContains(code, "decimal(\"balance\", 10, 2)")
        assertContains(code, "timestamp(\"created_at\")")
    }

    @Test
    fun `generates primary key modifier`() {
        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "Users" }!!
        val code = tableFile.toString()

        assertContains(code, ".primaryKey()")
    }

    @Test
    fun `generates autoIncrement modifier`() {
        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "Users" }!!
        val code = tableFile.toString()

        assertContains(code, ".autoIncrement()")
    }

    @Test
    fun `generates nullable modifier for nullable columns`() {
        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "Users" }!!
        val code = tableFile.toString()

        // email is nullable
        assertContains(code, "varchar(\"email\", 100).nullable()")
    }

    @Test
    fun `generates entity class when enabled`() {
        val config = createConfig(generateEntities = true)
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)

        val entityFile = files.find { it.name == "User" }
        assertTrue(entityFile != null)

        val code = entityFile.toString()
        assertContains(code, "data class User")
    }

    @Test
    fun `does not generate entity class when disabled`() {
        val config = createConfig(generateEntities = false)
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)

        assertEquals(1, files.size)
        assertEquals("Users", files.first().name)
    }

    @Test
    fun `entity class has nullable types for nullable columns`() {
        val config = createConfig(useNullableTypes = true)
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val entityFile = files.find { it.name == "User" }!!
        val code = entityFile.toString()

        assertContains(code, "email: String? = null")
    }

    @Test
    fun `generates KDoc when enabled`() {
        val config = createConfig(generateKdoc = true)
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "Users" }!!
        val code = tableFile.toString()

        assertContains(code, "Korma Table definition for `users`")
    }

    @Test
    fun `does not generate KDoc when disabled`() {
        val config = createConfig(generateKdoc = false)
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "Users" }!!
        val code = tableFile.toString()

        // Should not contain KDoc style comments
        val kdocPattern = Regex("/\\*\\*.*\\*/", RegexOption.DOT_MATCHES_ALL)
        val hasKdoc = kdocPattern.containsMatchIn(code)
        assertTrue(!hasKdoc || !code.contains("Korma Table definition"))
    }

    @Test
    fun `uses custom naming strategy`() {
        val config = createConfig(
            namingStrategy = NamingStrategy.custom(
                tableToClass = { "Tbl${NamingStrategy.Default.tableToClassName(it)}" },
                columnToProperty = { "col_${NamingStrategy.Default.columnToPropertyName(it)}" }
            )
        )
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "TblUsers" }

        assertTrue(tableFile != null)
        val code = tableFile.toString()
        assertContains(code, "col_id")
        assertContains(code, "col_username")
    }

    @Test
    fun `generates file header comment`() {
        val config = CodegenConfig(
            packageName = "com.example",
            databaseConfig = DatabaseConfig(url = "jdbc:h2:mem:test"),
            fileHeader = "Custom header comment"
        )
        val generator = KotlinCodeGenerator(config)
        val schema = createTestSchema()

        val files = generator.generate(schema)
        val code = files.first().toString()

        assertContains(code, "Custom header comment")
    }

    @Test
    fun `maps SQL types correctly`() {
        val columns = listOf(
            ColumnMetadata("int_col", Types.INTEGER, "INTEGER", 0, 0, false, null, null, 1, false),
            ColumnMetadata("float_col", Types.FLOAT, "FLOAT", 0, 0, false, null, null, 2, false),
            ColumnMetadata("double_col", Types.DOUBLE, "DOUBLE", 0, 0, false, null, null, 3, false),
            ColumnMetadata("date_col", Types.DATE, "DATE", 0, 0, false, null, null, 4, false),
            ColumnMetadata("time_col", Types.TIME, "TIME", 0, 0, false, null, null, 5, false),
            ColumnMetadata("blob_col", Types.BLOB, "BLOB", 0, 0, false, null, null, 6, false),
            ColumnMetadata("text_col", Types.CLOB, "CLOB", 0, 0, false, null, null, 7, false)
        )

        val table = TableMetadata(
            name = "type_test",
            schema = null,
            remarks = null,
            columns = columns,
            primaryKeyColumns = emptyList(),
            foreignKeys = emptyList(),
            uniqueConstraints = emptyList(),
            indices = emptyList()
        )

        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = DatabaseSchema(listOf(table))

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "TypeTest" }!!
        val code = tableFile.toString()

        assertContains(code, "integer(\"int_col\")")
        assertContains(code, "float(\"float_col\")")
        assertContains(code, "double(\"double_col\")")
        assertContains(code, "date(\"date_col\")")
        assertContains(code, "time(\"time_col\")")
        assertContains(code, "blob(\"blob_col\")")
        assertContains(code, "text(\"text_col\")")
    }

    @Test
    fun `handles UUID type`() {
        val columns = listOf(
            ColumnMetadata("id", Types.OTHER, "UUID", 0, 0, false, null, null, 1, false)
        )

        val table = TableMetadata(
            name = "uuid_test",
            schema = null,
            remarks = null,
            columns = columns,
            primaryKeyColumns = listOf("id"),
            foreignKeys = emptyList(),
            uniqueConstraints = emptyList(),
            indices = emptyList()
        )

        val config = createConfig()
        val generator = KotlinCodeGenerator(config)
        val schema = DatabaseSchema(listOf(table))

        val files = generator.generate(schema)
        val tableFile = files.find { it.name == "UuidTest" }!!
        val code = tableFile.toString()

        assertContains(code, "uuid(\"id\")")
    }

    @Test
    fun `generates multiple files for multiple tables`() {
        val table1 = TableMetadata(
            name = "users", schema = null, remarks = null,
            columns = listOf(
                ColumnMetadata("id", Types.INTEGER, "INTEGER", 0, 0, false, null, null, 1, false)
            ),
            primaryKeyColumns = listOf("id"),
            foreignKeys = emptyList(),
            uniqueConstraints = emptyList(),
            indices = emptyList()
        )

        val table2 = TableMetadata(
            name = "orders", schema = null, remarks = null,
            columns = listOf(
                ColumnMetadata("id", Types.INTEGER, "INTEGER", 0, 0, false, null, null, 1, false)
            ),
            primaryKeyColumns = listOf("id"),
            foreignKeys = emptyList(),
            uniqueConstraints = emptyList(),
            indices = emptyList()
        )

        val config = createConfig(generateEntities = true)
        val generator = KotlinCodeGenerator(config)
        val schema = DatabaseSchema(listOf(table1, table2))

        val files = generator.generate(schema)

        // 2 tables * 2 files each (table object + entity) = 4 files
        assertEquals(4, files.size)
        assertTrue(files.any { it.name == "Users" })  // Table object
        assertTrue(files.any { it.name == "Orders" }) // Table object
        assertTrue(files.any { it.name == "User" })   // Entity class (singularized)
        assertTrue(files.any { it.name == "Order" })  // Entity class (singularized)
    }
}
