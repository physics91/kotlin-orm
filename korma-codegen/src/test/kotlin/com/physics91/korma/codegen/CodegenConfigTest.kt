package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import com.physics91.korma.codegen.config.NamingStrategy
import com.physics91.korma.codegen.config.TypeMapping
import com.physics91.korma.codegen.config.codegenConfig
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CodegenConfigTest {

    @Test
    fun `creates config with default values`() {
        val config = CodegenConfig(
            packageName = "com.example",
            databaseConfig = DatabaseConfig(url = "jdbc:h2:mem:test")
        )

        assertEquals("com.example", config.packageName)
        assertEquals(Paths.get("src/main/kotlin"), config.outputDirectory)
        assertTrue(config.generateEntities)
        assertFalse(config.generateDaos)
        assertTrue(config.generateKdoc)
        assertTrue(config.useNullableTypes)
    }

    @Test
    fun `builder creates config correctly`() {
        val config = CodegenConfig.builder().apply {
            packageName("com.myapp.tables")
            outputDirectory("generated/kotlin")
            database {
                postgresql("localhost", 5432, "mydb")
                username("user")
                password("pass")
            }
            generateEntities(true)
            generateDaos(true)
            schema("public")
            include("users", "orders")
            exclude(".*_backup")
            generateKdoc(false)
            useNullableTypes(false)
            fileHeader("Custom header")
        }.build()

        assertEquals("com.myapp.tables", config.packageName)
        assertEquals(Paths.get("generated/kotlin"), config.outputDirectory)
        assertEquals("jdbc:postgresql://localhost:5432/mydb", config.databaseConfig.url)
        assertEquals("user", config.databaseConfig.username)
        assertEquals("pass", config.databaseConfig.password)
        assertTrue(config.generateEntities)
        assertTrue(config.generateDaos)
        assertEquals("public", config.schema)
        assertEquals(2, config.includePatterns.size)
        assertEquals(1, config.excludePatterns.size)
        assertFalse(config.generateKdoc)
        assertFalse(config.useNullableTypes)
        assertEquals("Custom header", config.fileHeader)
    }

    @Test
    fun `builder throws if database not configured`() {
        val builder = CodegenConfig.Builder().apply {
            packageName("com.example")
        }

        assertThrows<IllegalStateException> {
            builder.build()
        }
    }

    @Test
    fun `DSL function creates config`() {
        val config = codegenConfig {
            packageName("com.example.gen")
            database {
                h2("testdb")
            }
            generateEntities(true)
        }

        assertEquals("com.example.gen", config.packageName)
        assertEquals("jdbc:h2:mem:testdb", config.databaseConfig.url)
    }

    @Test
    fun `custom type mappings are stored`() {
        val config = CodegenConfig.builder().apply {
            packageName("com.example")
            database { h2("test") }
            mapType("JSONB", TypeMapping(
                kotlinType = "com.example.JsonNode",
                columnTypeFunction = "jsonb"
            ))
            mapType("INET", TypeMapping(
                kotlinType = "java.net.InetAddress"
            ))
        }.build()

        assertEquals(2, config.customTypeMappings.size)
        assertEquals("com.example.JsonNode", config.customTypeMappings["JSONB"]?.kotlinType)
        assertEquals("java.net.InetAddress", config.customTypeMappings["INET"]?.kotlinType)
    }

    @Test
    fun `naming strategy can be customized`() {
        val config = CodegenConfig.builder().apply {
            packageName("com.example")
            database { h2("test") }
            namingStrategy(NamingStrategy.SnakeCase)
        }.build()

        assertEquals(NamingStrategy.SnakeCase, config.namingStrategy)
    }
}

class DatabaseConfigTest {

    @Test
    fun `creates PostgreSQL config`() {
        val config = DatabaseConfig.builder().apply {
            postgresql("localhost", 5432, "mydb")
            username("user")
            password("pass")
        }.build()

        assertEquals("jdbc:postgresql://localhost:5432/mydb", config.url)
        assertEquals("org.postgresql.Driver", config.driverClassName)
        assertEquals("user", config.username)
        assertEquals("pass", config.password)
    }

    @Test
    fun `creates MySQL config`() {
        val config = DatabaseConfig.builder().apply {
            mysql("localhost", 3306, "mydb")
            username("root")
            password("secret")
        }.build()

        assertEquals("jdbc:mysql://localhost:3306/mydb", config.url)
        assertEquals("com.mysql.cj.jdbc.Driver", config.driverClassName)
    }

    @Test
    fun `creates H2 config`() {
        val config = DatabaseConfig.builder().apply {
            h2("testdb")
        }.build()

        assertEquals("jdbc:h2:mem:testdb", config.url)
        assertEquals("org.h2.Driver", config.driverClassName)
    }

    @Test
    fun `creates SQLite config`() {
        val config = DatabaseConfig.builder().apply {
            sqlite("/path/to/db.sqlite")
        }.build()

        assertEquals("jdbc:sqlite:/path/to/db.sqlite", config.url)
        assertEquals("org.sqlite.JDBC", config.driverClassName)
    }

    @Test
    fun `creates config with custom URL`() {
        val config = DatabaseConfig.builder().apply {
            url("jdbc:custom://server:1234/db?param=value")
            driver("com.custom.Driver")
        }.build()

        assertEquals("jdbc:custom://server:1234/db?param=value", config.url)
        assertEquals("com.custom.Driver", config.driverClassName)
    }
}
