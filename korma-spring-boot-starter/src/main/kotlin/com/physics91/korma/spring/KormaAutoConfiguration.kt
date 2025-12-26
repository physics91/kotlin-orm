package com.physics91.korma.spring

import com.physics91.korma.dialect.h2.H2Dialect
import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.sql.SqlDialect
import com.physics91.korma.transaction.TransactionManager
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource

/**
 * Auto-configuration for Korma ORM.
 *
 * This configuration automatically sets up Korma when:
 * - A DataSource is available
 * - korma-jdbc is on the classpath
 *
 * Configuration can be customized via application.yml:
 * ```yaml
 * korma:
 *   show-sql: true
 *   format-sql: true
 *   dialect: auto
 *   batch-size: 100
 * ```
 */
@AutoConfiguration(after = [DataSourceAutoConfiguration::class])
@ConditionalOnClass(JdbcDatabase::class)
@EnableConfigurationProperties(KormaProperties::class)
class KormaAutoConfiguration {

    private val logger = LoggerFactory.getLogger(KormaAutoConfiguration::class.java)

    /**
     * Create the SQL dialect based on configuration or auto-detection.
     */
    @Bean
    @ConditionalOnMissingBean(SqlDialect::class)
    fun sqlDialect(dataSource: DataSource, properties: KormaProperties): SqlDialect {
        val dialect = when (properties.dialect.lowercase()) {
            "auto" -> detectDialect(dataSource)
            "h2" -> H2Dialect
            "postgresql", "postgres" -> loadDialect("com.physics91.korma.dialect.postgresql.PostgreSqlDialect")
            "mysql", "mariadb" -> loadDialect("com.physics91.korma.dialect.mysql.MySqlDialect")
            "sqlite" -> loadDialect("com.physics91.korma.dialect.sqlite.SqliteDialect")
            else -> throw IllegalArgumentException("Unknown dialect: ${properties.dialect}")
        }
        logger.info("Korma configured with dialect: ${dialect.name}")
        return dialect
    }

    /**
     * Create the JdbcDatabase instance.
     */
    @Bean
    @ConditionalOnMissingBean(JdbcDatabase::class)
    fun jdbcDatabase(dataSource: DataSource, dialect: SqlDialect): JdbcDatabase {
        return JdbcDatabase(dataSource, dialect)
    }

    /**
     * Create the Spring-integrated transaction manager for Korma.
     */
    @Bean
    @ConditionalOnMissingBean(SpringTransactionManager::class)
    fun springTransactionManager(
        dataSource: DataSource,
        platformTransactionManager: PlatformTransactionManager
    ): SpringTransactionManager {
        return SpringTransactionManager(dataSource, platformTransactionManager)
    }

    /**
     * Create the KormaTemplate for convenient database operations.
     */
    @Bean
    @ConditionalOnMissingBean(KormaTemplate::class)
    fun kormaTemplate(database: JdbcDatabase, properties: KormaProperties): KormaTemplate {
        return KormaTemplate(database, properties)
    }

    /**
     * Detect dialect from JDBC URL.
     */
    private fun detectDialect(dataSource: DataSource): SqlDialect {
        return try {
            dataSource.connection.use { conn ->
                val url = conn.metaData.url.lowercase()
                when {
                    url.contains("h2") -> H2Dialect
                    url.contains("postgresql") || url.contains("postgres") ->
                        loadDialect("com.physics91.korma.dialect.postgresql.PostgreSqlDialect")
                    url.contains("mysql") || url.contains("mariadb") ->
                        loadDialect("com.physics91.korma.dialect.mysql.MySqlDialect")
                    url.contains("sqlite") ->
                        loadDialect("com.physics91.korma.dialect.sqlite.SqliteDialect")
                    else -> {
                        logger.warn("Could not detect dialect from URL: $url, defaulting to H2")
                        H2Dialect
                    }
                }
            }
        } catch (e: Exception) {
            logger.warn("Failed to detect dialect: ${e.message}, defaulting to H2")
            H2Dialect
        }
    }

    /**
     * Load a dialect class by name.
     */
    private fun loadDialect(className: String): SqlDialect {
        return try {
            val clazz = Class.forName(className)
            val field = clazz.getDeclaredField("INSTANCE")
            field.get(null) as SqlDialect
        } catch (e: ClassNotFoundException) {
            throw IllegalStateException(
                "Dialect class not found: $className. " +
                "Make sure the corresponding dialect module is on the classpath."
            )
        } catch (e: Exception) {
            throw IllegalStateException("Failed to load dialect: $className", e)
        }
    }
}

/**
 * Additional configuration for Korma repositories.
 */
@Configuration
@ConditionalOnClass(JdbcDatabase::class)
class KormaRepositoryConfiguration {

    /**
     * Register the TransactionManager interface bean.
     */
    @Bean
    @ConditionalOnMissingBean(TransactionManager::class)
    fun transactionManager(springTransactionManager: SpringTransactionManager): TransactionManager {
        return springTransactionManager
    }
}
