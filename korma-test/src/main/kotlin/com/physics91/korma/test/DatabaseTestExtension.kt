package com.physics91.korma.test

import com.physics91.korma.dialect.h2.H2Dialect
import com.physics91.korma.jdbc.DatabaseConfig
import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.jdbc.PoolConfig
import com.physics91.korma.sql.SqlDialect
import com.physics91.korma.test.containers.KormaMysqlContainer
import com.physics91.korma.test.containers.KormaPostgresContainer
import org.junit.jupiter.api.extension.*
import kotlin.time.Duration.Companion.seconds

/**
 * JUnit 5 extension that provides database instances for integration tests.
 *
 * This extension manages the lifecycle of database connections and provides
 * them to test classes through parameter injection.
 *
 * Usage:
 * ```kotlin
 * @ExtendWith(DatabaseTestExtension::class)
 * @DatabaseTest(DatabaseType.H2)
 * class MyIntegrationTest {
 *     @Test
 *     fun `test with injected database`(database: JdbcDatabase) {
 *         // database is automatically provided
 *     }
 * }
 * ```
 */
class DatabaseTestExtension : BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private val namespace = ExtensionContext.Namespace.create(DatabaseTestExtension::class.java)

    override fun beforeAll(context: ExtensionContext) {
        val annotation = findDatabaseTestAnnotation(context)
        val databaseType = annotation?.value ?: DatabaseType.H2

        val database = when (databaseType) {
            DatabaseType.H2 -> createH2Database()
            DatabaseType.POSTGRESQL -> createPostgresDatabase(context)
            DatabaseType.MYSQL -> createMysqlDatabase(context)
        }

        context.getStore(namespace).put("database", database)
        context.getStore(namespace).put("databaseType", databaseType)
    }

    override fun afterAll(context: ExtensionContext) {
        val database = context.getStore(namespace).get("database") as? JdbcDatabase
        database?.close()

        // Clean up containers if needed
        context.getStore(namespace).get("postgresContainer")?.let {
            (it as? KormaPostgresContainer)?.stop()
        }
        context.getStore(namespace).get("mysqlContainer")?.let {
            (it as? KormaMysqlContainer)?.stop()
        }
    }

    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean {
        return parameterContext.parameter.type == JdbcDatabase::class.java ||
                parameterContext.parameter.type == SqlDialect::class.java
    }

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Any {
        return when (parameterContext.parameter.type) {
            JdbcDatabase::class.java -> extensionContext.getStore(namespace).get("database") as JdbcDatabase
            SqlDialect::class.java -> {
                val databaseType = extensionContext.getStore(namespace).get("databaseType") as DatabaseType
                databaseType.dialect
            }
            else -> throw IllegalArgumentException("Unsupported parameter type: ${parameterContext.parameter.type}")
        }
    }

    private fun findDatabaseTestAnnotation(context: ExtensionContext): DatabaseTest? {
        return context.testClass.orElse(null)?.getAnnotation(DatabaseTest::class.java)
    }

    private fun createH2Database(): JdbcDatabase {
        val config = DatabaseConfig(
            jdbcUrl = "jdbc:h2:mem:korma_test_${System.nanoTime()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            username = "sa",
            password = "",
            poolConfig = PoolConfig(maximumPoolSize = 5)
        )
        return JdbcDatabase(config, H2Dialect)
    }

    private fun createPostgresDatabase(context: ExtensionContext): JdbcDatabase {
        val container = KormaPostgresContainer.create()
        container.start()
        context.getStore(namespace).put("postgresContainer", container)
        return container.toJdbcDatabase()
    }

    private fun createMysqlDatabase(context: ExtensionContext): JdbcDatabase {
        val container = KormaMysqlContainer.create()
        container.start()
        context.getStore(namespace).put("mysqlContainer", container)
        return container.toJdbcDatabase()
    }
}

/**
 * Annotation to specify which database type to use for a test class.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(DatabaseTestExtension::class)
annotation class DatabaseTest(
    val value: DatabaseType = DatabaseType.H2
)

/**
 * Supported database types for testing.
 */
enum class DatabaseType(val dialect: SqlDialect) {
    H2(H2Dialect),
    POSTGRESQL(com.physics91.korma.dialect.postgresql.PostgreSqlDialect),
    MYSQL(com.physics91.korma.dialect.mysql.MySqlDialect)
}

/**
 * Annotation for parameterized database tests across multiple database types.
 *
 * Usage:
 * ```kotlin
 * @ParameterizedDatabaseTest
 * class CrossDatabaseTest {
 *     @Test
 *     fun `should work on all databases`(database: JdbcDatabase, type: DatabaseType) {
 *         // Test runs once for each database type
 *     }
 * }
 * ```
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class ParameterizedDatabaseTest(
    val databases: Array<DatabaseType> = [DatabaseType.H2, DatabaseType.POSTGRESQL, DatabaseType.MYSQL]
)
