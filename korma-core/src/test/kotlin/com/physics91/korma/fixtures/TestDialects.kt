package com.physics91.korma.fixtures

import com.physics91.korma.schema.ColumnType
import com.physics91.korma.sql.BaseSqlDialect

/**
 * Common test dialects used across all test modules.
 * Provides consistent SQL dialect behavior for testing.
 */

/**
 * Standard test dialect with common features enabled.
 * Use this for general DSL and builder tests.
 */
object TestDialect : BaseSqlDialect() {
    override val name = "TestDialect"
    override val supportsReturning = true
    override val supportsOnConflict = true
    override val supportsILike = true

    override fun autoIncrementType(baseType: ColumnType<*>): String = "BIGINT AUTO_INCREMENT"
}

/**
 * PostgreSQL-like test dialect for testing PostgreSQL-specific features.
 */
object TestPostgresDialect : BaseSqlDialect() {
    override val name = "TestPostgreSQL"
    override val supportsReturning = true
    override val supportsOnConflict = true
    override val supportsILike = true

    override fun autoIncrementType(baseType: ColumnType<*>): String = "BIGSERIAL"
}

/**
 * MySQL-like test dialect for testing MySQL-specific features.
 */
object TestMySqlDialect : BaseSqlDialect() {
    override val name = "TestMySQL"
    override val supportsReturning = false
    override val supportsOnConflict = false
    override val supportsILike = false
    override val identifierQuoteChar: Char = '`'

    override fun autoIncrementType(baseType: ColumnType<*>): String = "BIGINT AUTO_INCREMENT"
}

/**
 * SQLite-like test dialect for testing SQLite-specific features.
 */
object TestSqliteDialect : BaseSqlDialect() {
    override val name = "TestSQLite"
    override val supportsReturning = true
    override val supportsOnConflict = true
    override val supportsILike = false
    override val supportsBooleanType = false

    override fun autoIncrementType(baseType: ColumnType<*>): String = "INTEGER"
}

/**
 * Minimal test dialect with all features disabled.
 * Use this for testing fallback behavior.
 */
object MinimalTestDialect : BaseSqlDialect() {
    override val name = "MinimalDialect"
    override val supportsReturning = false
    override val supportsOnConflict = false
    override val supportsILike = false
    override val supportsCTE = false
    override val supportsWindowFunctions = false

    override fun autoIncrementType(baseType: ColumnType<*>): String = "INTEGER AUTO_INCREMENT"
}
