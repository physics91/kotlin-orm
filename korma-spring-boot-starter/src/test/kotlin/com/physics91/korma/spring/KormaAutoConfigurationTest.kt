package com.physics91.korma.spring

import com.physics91.korma.dialect.h2.H2Dialect
import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.sql.SqlDialect
import com.physics91.korma.transaction.TransactionManager
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for Korma Spring Boot Auto-configuration.
 */
class KormaAutoConfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DataSourceAutoConfiguration::class.java,
                DataSourceTransactionManagerAutoConfiguration::class.java,
                KormaAutoConfiguration::class.java,
                KormaRepositoryConfiguration::class.java
            )
        )

    @Test
    fun `should auto-configure with H2 datasource`() {
        contextRunner
            .withPropertyValues(
                "spring.datasource.url=jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver"
            )
            .run { context ->
                // Verify beans are created
                assertNotNull(context.getBean(SqlDialect::class.java))
                assertNotNull(context.getBean(JdbcDatabase::class.java))
                assertNotNull(context.getBean(SpringTransactionManager::class.java))
                assertNotNull(context.getBean(KormaTemplate::class.java))
                assertNotNull(context.getBean(TransactionManager::class.java))

                // Verify dialect is H2
                val dialect = context.getBean(SqlDialect::class.java)
                assertEquals(H2Dialect, dialect)
            }
    }

    @Test
    fun `should detect H2 dialect from URL`() {
        contextRunner
            .withPropertyValues(
                "spring.datasource.url=jdbc:h2:mem:testdb",
                "korma.dialect=auto"
            )
            .run { context ->
                val dialect = context.getBean(SqlDialect::class.java)
                assertEquals("H2", dialect.name)
            }
    }

    @Test
    fun `should use configured dialect`() {
        contextRunner
            .withPropertyValues(
                "spring.datasource.url=jdbc:h2:mem:testdb",
                "korma.dialect=h2"
            )
            .run { context ->
                val dialect = context.getBean(SqlDialect::class.java)
                assertEquals(H2Dialect, dialect)
            }
    }

    @Test
    fun `should configure korma properties`() {
        contextRunner
            .withPropertyValues(
                "spring.datasource.url=jdbc:h2:mem:testdb",
                "korma.show-sql=true",
                "korma.format-sql=true",
                "korma.batch-size=200"
            )
            .run { context ->
                val properties = context.getBean(KormaProperties::class.java)
                assertTrue(properties.showSql)
                assertTrue(properties.formatSql)
                assertEquals(200, properties.batchSize)
            }
    }

    @Test
    fun `should not configure when JdbcDatabase class is missing`() {
        ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KormaAutoConfiguration::class.java))
            .withClassLoader(FilteredClassLoader(JdbcDatabase::class.java))
            .run { context ->
                assertTrue(!context.containsBean("jdbcDatabase"))
            }
    }

    @Test
    fun `should allow custom dialect bean`() {
        contextRunner
            .withPropertyValues(
                "spring.datasource.url=jdbc:h2:mem:testdb"
            )
            .withBean(SqlDialect::class.java, { H2Dialect })
            .run { context ->
                val dialect = context.getBean(SqlDialect::class.java)
                assertEquals(H2Dialect, dialect)
            }
    }
}

/**
 * ClassLoader that filters out specific classes for testing conditional configuration.
 */
private class FilteredClassLoader(vararg filteredClasses: Class<*>) : ClassLoader() {
    private val filteredClassNames = filteredClasses.map { it.name }.toSet()

    override fun loadClass(name: String): Class<*> {
        if (name in filteredClassNames) {
            throw ClassNotFoundException(name)
        }
        return super.loadClass(name)
    }
}
