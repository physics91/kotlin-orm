package com.physics91.korma.ktor

import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.sql.SqlDialect
import io.ktor.server.application.*
import io.ktor.util.*
import org.slf4j.LoggerFactory

/**
 * Ktor plugin for Korma ORM integration.
 *
 * This plugin provides seamless integration of Korma with Ktor applications,
 * supporting both JDBC and coroutine-based database operations.
 *
 * ## Installation
 *
 * ```kotlin
 * fun Application.module() {
 *     install(Korma) {
 *         jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"
 *         dialect = H2Dialect
 *         showSql = true
 *     }
 *
 *     routing {
 *         get("/users") {
 *             val users = korma.selectAll(Users) { row ->
 *                 User(row[Users.id], row[Users.name])
 *             }
 *             call.respond(users)
 *         }
 *     }
 * }
 * ```
 *
 * ## Features
 *
 * - Automatic database connection management
 * - Connection pooling via HikariCP
 * - Coroutine-friendly database operations
 * - Transaction support
 * - SQL logging
 * - Multiple dialect support (H2, PostgreSQL, MySQL, SQLite)
 */
val Korma = createApplicationPlugin(
    name = "Korma",
    createConfiguration = ::KormaConfig
) {
    val logger = LoggerFactory.getLogger("com.physics91.korma.ktor.Korma")
    val config = pluginConfig

    // Resolve dialect
    val dialect = config.resolveDialect()
    logger.info("Korma initializing with dialect: ${dialect.name}")

    // Create database instance
    val database = if (config.dataSource != null) {
        JdbcDatabase(config.dataSource!!, dialect)
    } else {
        val dbConfig = config.buildDatabaseConfig()
            ?: throw IllegalStateException("Either dataSource or jdbcUrl must be configured")
        JdbcDatabase(dbConfig, dialect)
    }

    logger.info("Korma database initialized successfully")

    // Store database in application attributes
    application.attributes.put(KormaDatabaseKey, database)
    application.attributes.put(KormaConfigKey, config)
    application.attributes.put(KormaDialectKey, dialect)

    // Register shutdown hook
    application.monitor.subscribe(ApplicationStopped) {
        logger.info("Shutting down Korma database connection pool")
        try {
            database.close()
        } catch (e: Exception) {
            logger.error("Error closing database", e)
        }
    }
}

/**
 * Attribute key for the Korma database instance.
 */
val KormaDatabaseKey = AttributeKey<JdbcDatabase>("KormaDatabase")

/**
 * Attribute key for the Korma configuration.
 */
val KormaConfigKey = AttributeKey<KormaConfig>("KormaConfig")

/**
 * Attribute key for the SQL dialect.
 */
val KormaDialectKey = AttributeKey<SqlDialect>("KormaDialect")

/**
 * Attribute key for the cached KormaDsl instance.
 */
internal val KormaDslKey = AttributeKey<KormaDsl>("KormaDsl")

/**
 * Get the Korma database instance from the application.
 *
 * @throws IllegalStateException if Korma plugin is not installed
 */
val Application.kormaDatabase: JdbcDatabase
    get() = attributes.getOrNull(KormaDatabaseKey)
        ?: throw IllegalStateException("Korma plugin is not installed. Call install(Korma) first.")

/**
 * Get the Korma configuration from the application.
 *
 * @throws IllegalStateException if Korma plugin is not installed
 */
val Application.kormaConfig: KormaConfig
    get() = attributes.getOrNull(KormaConfigKey)
        ?: throw IllegalStateException("Korma plugin is not installed. Call install(Korma) first.")

/**
 * Get the SQL dialect from the application.
 *
 * @throws IllegalStateException if Korma plugin is not installed
 */
val Application.kormaDialect: SqlDialect
    get() = attributes.getOrNull(KormaDialectKey)
        ?: throw IllegalStateException("Korma plugin is not installed. Call install(Korma) first.")

/**
 * Get the Korma database instance from a call.
 */
val ApplicationCall.kormaDatabase: JdbcDatabase
    get() = application.kormaDatabase

/**
 * Get the Korma configuration from a call.
 */
val ApplicationCall.kormaConfig: KormaConfig
    get() = application.kormaConfig

/**
 * Get the SQL dialect from a call.
 */
val ApplicationCall.kormaDialect: SqlDialect
    get() = application.kormaDialect
