package com.physics91.korma.test.containers

import com.physics91.korma.dialect.mysql.MySqlDialect
import com.physics91.korma.jdbc.DatabaseConfig
import com.physics91.korma.jdbc.JdbcDatabase
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * MySQL TestContainer wrapper for Korma integration tests.
 *
 * Provides pre-configured MySQL container with easy access to
 * both JDBC and R2DBC database connections.
 *
 * Usage:
 * ```kotlin
 * @Testcontainers
 * class MyTest {
 *     companion object {
 *         @Container
 *         val mysql = KormaMysqlContainer()
 *     }
 *
 *     @Test
 *     fun `test database operations`() {
 *         val db = mysql.toJdbcDatabase()
 *         // ... use db
 *     }
 * }
 * ```
 */
class KormaMysqlContainer(
    dockerImageName: DockerImageName = DockerImageName.parse("mysql:8.0")
) : MySQLContainer<KormaMysqlContainer>(dockerImageName) {

    init {
        withDatabaseName("korma_test")
        withUsername("korma")
        withPassword("korma")
        withReuse(true)
        // MySQL specific settings for better test performance
        withCommand(
            "--character-set-server=utf8mb4",
            "--collation-server=utf8mb4_unicode_ci",
            "--max_connections=200"
        )
    }

    // Use extension functions from KormaContainerSupport (DRY)
    fun toDatabaseConfig(): DatabaseConfig = toKormaDatabaseConfig()
    fun toJdbcDatabase(): JdbcDatabase = toKormaJdbcDatabase(MySqlDialect)
    fun getR2dbcUrl(): String = getKormaR2dbcUrl(R2DBC_DRIVER, MYSQL_PORT)
    fun getR2dbcOptions(): Map<String, String> = getKormaR2dbcOptions(R2DBC_DRIVER, MYSQL_PORT)

    companion object {
        private const val R2DBC_DRIVER = "mysql"

        /**
         * Creates a new MySQL container with default settings.
         */
        fun create(): KormaMysqlContainer = KormaMysqlContainer()

        /**
         * Creates a MySQL container with a specific version.
         */
        fun withVersion(version: String): KormaMysqlContainer =
            KormaMysqlContainer(DockerImageName.parse("mysql:$version"))
    }
}
