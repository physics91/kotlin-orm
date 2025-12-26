package com.physics91.korma.codegen

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class KormaCodegenTest {

    private lateinit var connection: Connection
    private val dbUrl = "jdbc:h2:mem:testdb_codegen;DB_CLOSE_DELAY=-1"

    @TempDir
    lateinit var tempDir: Path

    @BeforeEach
    fun setup() {
        connection = DriverManager.getConnection(dbUrl)
        createTestSchema()
    }

    @AfterEach
    fun tearDown() {
        connection.createStatement().execute("DROP ALL OBJECTS")
        connection.close()
    }

    private fun createTestSchema() {
        connection.createStatement().execute("""
            CREATE TABLE users (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """.trimIndent())

        connection.createStatement().execute("""
            CREATE TABLE posts (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                title VARCHAR(200) NOT NULL,
                content TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """.trimIndent())
    }

    @Test
    fun `generate with DSL creates files`() {
        val paths = KormaCodegen.generate {
            packageName("com.example.generated")
            outputDirectory(tempDir)
            database {
                url(dbUrl)
            }
            generateEntities(true)
        }

        assertTrue(paths.isNotEmpty())
        // Should have 2 tables * 2 files (table + entity) = 4 files
        assertEquals(4, paths.size)

        // Verify files exist
        paths.forEach { path ->
            assertTrue(Files.exists(path), "File should exist: $path")
        }
    }

    @Test
    fun `preview returns code without writing files`() {
        val preview = KormaCodegen.preview {
            packageName("com.example.preview")
            outputDirectory(tempDir)
            database {
                url(dbUrl)
            }
            generateEntities(true)
        }

        assertTrue(preview.isNotEmpty())

        // Find Users table code
        val usersTableCode = preview.entries.find { it.key.contains("Users.kt") }
        assertNotNull(usersTableCode)
        assertTrue(usersTableCode.value.contains("object Users"))

        // Verify no files were actually written
        val generatedDir = tempDir.resolve("com/example/preview")
        assertTrue(!Files.exists(generatedDir) || Files.list(generatedDir).count() == 0L)
    }

    @Test
    fun `introspect returns schema metadata`() {
        val schema = KormaCodegen.introspect {
            url(dbUrl)
        }

        assertEquals(2, schema.tables.size)
        assertNotNull(schema.getTable("USERS"))
        assertNotNull(schema.getTable("POSTS"))

        val usersTable = schema.getTable("USERS")!!
        assertEquals(4, usersTable.columns.size)
        assertEquals(listOf("ID"), usersTable.primaryKeyColumns)

        val postsTable = schema.getTable("POSTS")!!
        assertEquals(1, postsTable.foreignKeys.size)
    }

    @Test
    fun `generate respects include patterns`() {
        val paths = KormaCodegen.generate {
            packageName("com.example.filtered")
            outputDirectory(tempDir)
            database { url(dbUrl) }
            include("USERS")
            generateEntities(true)
        }

        // Only users table and entity
        assertEquals(2, paths.size)
        assertTrue(paths.all { it.fileName.toString().contains("User") })
    }

    @Test
    fun `generate respects exclude patterns`() {
        val paths = KormaCodegen.generate {
            packageName("com.example.excluded")
            outputDirectory(tempDir)
            database { url(dbUrl) }
            exclude("POSTS")
            generateEntities(true)
        }

        // Only users table and entity
        assertEquals(2, paths.size)
        assertTrue(paths.all { it.fileName.toString().contains("User") })
    }

    @Test
    fun `generated Table object has correct structure`() {
        val preview = KormaCodegen.preview {
            packageName("com.example.structure")
            database { url(dbUrl) }
            generateEntities(false)
            generateKdoc(true)
        }

        val usersCode = preview.entries.find { it.key.contains("Users.kt") }!!.value

        // Check imports
        assertTrue(usersCode.contains("import com.physics91.korma.schema.Table"))

        // Check object declaration
        assertTrue(usersCode.contains("object Users : Table(\"USERS\")"))

        // Check column definitions
        assertTrue(usersCode.contains("long(\"ID\")"))
        assertTrue(usersCode.contains("varchar(\"USERNAME\""))
        assertTrue(usersCode.contains("timestamp(\"CREATED_AT\")"))

        // Check modifiers
        assertTrue(usersCode.contains(".primaryKey()"))
        assertTrue(usersCode.contains(".autoIncrement()"))
    }

    @Test
    fun `generated entity class has correct structure`() {
        val preview = KormaCodegen.preview {
            packageName("com.example.entity")
            database { url(dbUrl) }
            generateEntities(true)
            useNullableTypes(true)
        }

        val userEntityCode = preview.entries.find { it.key.contains("User.kt") }!!.value

        // Check data class declaration
        assertTrue(userEntityCode.contains("data class User"))

        // Check properties with correct types
        assertTrue(userEntityCode.contains("id: Long"))
        assertTrue(userEntityCode.contains("username: String"))
        assertTrue(userEntityCode.contains("email: String?"))
    }

    @Test
    fun `handles empty schema gracefully`() {
        // Drop all tables
        connection.createStatement().execute("DROP TABLE posts")
        connection.createStatement().execute("DROP TABLE users")

        val paths = KormaCodegen.generate {
            packageName("com.example.empty")
            outputDirectory(tempDir)
            database { url(dbUrl) }
        }

        assertTrue(paths.isEmpty())
    }

    @Test
    fun `file header is included in generated code`() {
        val preview = KormaCodegen.preview {
            packageName("com.example.header")
            database { url(dbUrl) }
            fileHeader("Custom Header - Do not modify")
        }

        val code = preview.values.first()
        assertTrue(code.contains("Custom Header - Do not modify"))
    }

    @Test
    fun `generates code without entities when disabled`() {
        val preview = KormaCodegen.preview {
            packageName("com.example.notables")
            database { url(dbUrl) }
            generateEntities(false)
        }

        // Only table objects, no entities
        assertEquals(2, preview.size)
        assertTrue(preview.keys.all { it.contains("Users.kt") || it.contains("Posts.kt") })
        // Singular names would be User.kt and Post.kt
        assertTrue(preview.keys.none { it.contains("User.kt") && !it.contains("Users.kt") })
    }
}
