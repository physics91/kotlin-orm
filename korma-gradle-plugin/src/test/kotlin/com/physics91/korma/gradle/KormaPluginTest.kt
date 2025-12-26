package com.physics91.korma.gradle

import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertTrue

class KormaPluginTest {

    @TempDir
    lateinit var testProjectDir: File

    private lateinit var buildFile: File
    private lateinit var settingsFile: File

    @BeforeEach
    fun setup() {
        settingsFile = File(testProjectDir, "settings.gradle.kts")
        settingsFile.writeText("""
            rootProject.name = "test-project"
        """.trimIndent())

        buildFile = File(testProjectDir, "build.gradle.kts")
    }

    @Test
    fun `plugin can be applied`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.generated")

                database {
                    url.set("jdbc:h2:mem:test")
                }
            }
        """.trimIndent())

        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("tasks", "--all")
            .build()

        assertTrue(result.output.contains("generateKormaTables"))
        assertTrue(result.output.contains("previewKormaTables"))
        assertTrue(result.output.contains("introspectKormaSchema"))
    }

    @Test
    fun `plugin registers tasks in korma group`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.generated")
                database {
                    url.set("jdbc:h2:mem:test")
                }
            }
        """.trimIndent())

        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("tasks", "--group=korma")
            .build()

        assertTrue(result.output.contains("generateKormaTables"))
    }

    @Test
    fun `extension can configure database with postgresql shortcut`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.tables")

                database {
                    postgresql("localhost", 5432, "mydb")
                    username.set("user")
                    password.set("pass")
                }

                codegen {
                    generateEntities.set(true)
                    generateKdoc.set(false)
                    include("users", "orders")
                    exclude(".*_backup")
                }
            }
        """.trimIndent())

        // Just verify it parses and configures without errors
        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("help")
            .build()

        // Should complete without errors
        assertTrue(result.output.contains("BUILD SUCCESSFUL"))
    }

    @Test
    fun `extension can configure database with mysql shortcut`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.tables")

                database {
                    mysql("localhost", 3306, "mydb")
                    username.set("root")
                    password.set("secret")
                }
            }
        """.trimIndent())

        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("help")
            .build()

        assertTrue(result.output.contains("BUILD SUCCESSFUL"))
    }

    @Test
    fun `extension can configure database with sqlite shortcut`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.tables")

                database {
                    sqlite("/path/to/db.sqlite")
                }
            }
        """.trimIndent())

        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("help")
            .build()

        assertTrue(result.output.contains("BUILD SUCCESSFUL"))
    }

    @Test
    fun `extension can configure database with h2 shortcut`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.tables")

                database {
                    h2("testdb")
                }
            }
        """.trimIndent())

        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("help")
            .build()

        assertTrue(result.output.contains("BUILD SUCCESSFUL"))
    }

    @Test
    fun `extension supports custom output directory`() {
        buildFile.writeText("""
            plugins {
                id("com.physics91.korma")
            }

            korma {
                packageName.set("com.example.generated")
                outputDirectory.set(file("build/generated/korma"))

                database {
                    url.set("jdbc:h2:mem:test")
                }
            }
        """.trimIndent())

        val result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withPluginClasspath()
            .withArguments("help")
            .build()

        assertTrue(result.output.contains("BUILD SUCCESSFUL"))
    }
}
