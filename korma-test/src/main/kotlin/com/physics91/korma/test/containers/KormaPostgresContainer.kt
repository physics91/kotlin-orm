package com.physics91.korma.test.containers

import com.physics91.korma.dialect.postgresql.PostgreSqlDialect
import com.physics91.korma.jdbc.DatabaseConfig
import com.physics91.korma.jdbc.JdbcDatabase
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * PostgreSQL TestContainer wrapper for Korma integration tests.
 *
 * Provides pre-configured PostgreSQL container with easy access to
 * both JDBC and R2DBC database connections.
 *
 * Usage:
 * ```kotlin
 * @Testcontainers
 * class MyTest {
 *     companion object {
 *         @Container
 *         val postgres = KormaPostgresContainer()
 *     }
 *
 *     @Test
 *     fun `test database operations`() {
 *         val db = postgres.toJdbcDatabase()
 *         // ... use db
 *     }
 * }
 * ```
 */
class KormaPostgresContainer(
    dockerImageName: DockerImageName = DockerImageName.parse("postgres:16-alpine")
) : PostgreSQLContainer<KormaPostgresContainer>(dockerImageName) {

    init {
        withDatabaseName("korma_test")
        withUsername("korma")
        withPassword("korma")
        withReuse(true)
    }

    // Use extension functions from KormaContainerSupport (DRY)
    fun toDatabaseConfig(): DatabaseConfig = toKormaDatabaseConfig()
    fun toJdbcDatabase(): JdbcDatabase = toKormaJdbcDatabase(PostgreSqlDialect)
    fun getR2dbcUrl(): String = getKormaR2dbcUrl(R2DBC_DRIVER, POSTGRESQL_PORT)
    fun getR2dbcOptions(): Map<String, String> = getKormaR2dbcOptions(R2DBC_DRIVER, POSTGRESQL_PORT)

    companion object {
        private const val R2DBC_DRIVER = "postgresql"

        /**
         * Creates a new PostgreSQL container with default settings.
         */
        fun create(): KormaPostgresContainer = KormaPostgresContainer()

        /**
         * Creates a PostgreSQL container with a specific version.
         */
        fun withVersion(version: String): KormaPostgresContainer =
            KormaPostgresContainer(DockerImageName.parse("postgres:$version"))
    }
}
