package com.physics91.korma.migration

import com.physics91.korma.sql.SqlDialect
import java.security.MessageDigest
import java.time.Instant

/**
 * Tracks migration execution state in the database.
 * Manages the _korma_migrations tracking table.
 */
class MigrationTracker(
    private val executor: MigrationExecutor,
    private val dialect: SqlDialect,
    private val tableName: String = "_korma_migrations"
) {
    /**
     * Ensure the migrations tracking table exists.
     */
    fun ensureTableExists() {
        val sql = buildString {
            append("CREATE TABLE IF NOT EXISTS ")
            append(dialect.quoteIdentifier(tableName))
            append(" (")
            append("${dialect.quoteIdentifier("version")} VARCHAR(255) PRIMARY KEY, ")
            append("${dialect.quoteIdentifier("description")} VARCHAR(500), ")
            append("${dialect.quoteIdentifier("checksum")} VARCHAR(64) NOT NULL, ")
            append("${dialect.quoteIdentifier("executed_at")} TIMESTAMP NOT NULL, ")
            append("${dialect.quoteIdentifier("execution_time_ms")} BIGINT NOT NULL, ")
            append("${dialect.quoteIdentifier("success")} BOOLEAN NOT NULL")
            append(")")
        }
        executor.execute(sql)
    }

    /**
     * Get all applied migrations.
     */
    fun getAppliedMigrations(): List<MigrationRecord> {
        val sql = """
            SELECT
                ${dialect.quoteIdentifier("version")},
                ${dialect.quoteIdentifier("description")},
                ${dialect.quoteIdentifier("checksum")},
                ${dialect.quoteIdentifier("executed_at")},
                ${dialect.quoteIdentifier("execution_time_ms")},
                ${dialect.quoteIdentifier("success")}
            FROM ${dialect.quoteIdentifier(tableName)}
            ORDER BY ${dialect.quoteIdentifier("executed_at")} ASC
        """.trimIndent()

        return executor.executeQuery(sql).map { row ->
            MigrationRecord(
                version = row["version"] as String,
                description = row["description"] as? String ?: "",
                checksum = row["checksum"] as String,
                executedAt = parseTimestamp(row["executed_at"]),
                executionTimeMs = (row["execution_time_ms"] as Number).toLong(),
                success = row["success"] as Boolean
            )
        }
    }

    /**
     * Check if a specific migration has been applied.
     */
    fun isMigrationApplied(version: String): Boolean {
        val sql = """
            SELECT COUNT(*) as cnt
            FROM ${dialect.quoteIdentifier(tableName)}
            WHERE ${dialect.quoteIdentifier("version")} = ? AND ${dialect.quoteIdentifier("success")} = TRUE
        """.trimIndent()

        // Note: This is a simplified implementation. Real usage would need parameter binding.
        val result = executor.executeQuery(
            sql.replace("?", "'${escapeString(version)}'")
        )
        return (result.firstOrNull()?.get("cnt") as? Number)?.toLong() ?: 0L > 0
    }

    /**
     * Get the migration record for a specific version.
     */
    fun getMigrationRecord(version: String): MigrationRecord? {
        val sql = """
            SELECT
                ${dialect.quoteIdentifier("version")},
                ${dialect.quoteIdentifier("description")},
                ${dialect.quoteIdentifier("checksum")},
                ${dialect.quoteIdentifier("executed_at")},
                ${dialect.quoteIdentifier("execution_time_ms")},
                ${dialect.quoteIdentifier("success")}
            FROM ${dialect.quoteIdentifier(tableName)}
            WHERE ${dialect.quoteIdentifier("version")} = '${escapeString(version)}'
        """.trimIndent()

        return executor.executeQuery(sql).firstOrNull()?.let { row ->
            MigrationRecord(
                version = row["version"] as String,
                description = row["description"] as? String ?: "",
                checksum = row["checksum"] as String,
                executedAt = parseTimestamp(row["executed_at"]),
                executionTimeMs = (row["execution_time_ms"] as Number).toLong(),
                success = row["success"] as Boolean
            )
        }
    }

    /**
     * Record a migration as applied.
     */
    fun recordMigration(
        version: String,
        description: String,
        checksum: String,
        executionTimeMs: Long,
        success: Boolean
    ) {
        val sql = """
            INSERT INTO ${dialect.quoteIdentifier(tableName)}
            (${dialect.quoteIdentifier("version")},
             ${dialect.quoteIdentifier("description")},
             ${dialect.quoteIdentifier("checksum")},
             ${dialect.quoteIdentifier("executed_at")},
             ${dialect.quoteIdentifier("execution_time_ms")},
             ${dialect.quoteIdentifier("success")})
            VALUES
            ('${escapeString(version)}',
             '${escapeString(description)}',
             '${escapeString(checksum)}',
             ${dialect.currentTimestampExpression()},
             $executionTimeMs,
             ${if (success) "TRUE" else "FALSE"})
        """.trimIndent()

        executor.execute(sql)
    }

    /**
     * Remove a migration record (for rollback/undo).
     */
    fun removeMigration(version: String) {
        val sql = """
            DELETE FROM ${dialect.quoteIdentifier(tableName)}
            WHERE ${dialect.quoteIdentifier("version")} = '${escapeString(version)}'
        """.trimIndent()

        executor.execute(sql)
    }

    /**
     * Mark a migration as failed.
     */
    fun markFailed(version: String, description: String, checksum: String, executionTimeMs: Long) {
        recordMigration(version, description, checksum, executionTimeMs, success = false)
    }

    /**
     * Update failed migration to success (for retry scenarios).
     */
    fun markSuccess(version: String, executionTimeMs: Long) {
        val sql = """
            UPDATE ${dialect.quoteIdentifier(tableName)}
            SET ${dialect.quoteIdentifier("success")} = TRUE,
                ${dialect.quoteIdentifier("execution_time_ms")} = $executionTimeMs,
                ${dialect.quoteIdentifier("executed_at")} = ${dialect.currentTimestampExpression()}
            WHERE ${dialect.quoteIdentifier("version")} = '${escapeString(version)}'
        """.trimIndent()

        executor.execute(sql)
    }

    /**
     * Calculate checksum for a migration.
     */
    fun calculateChecksum(migration: Migration, context: MigrationContext): String {
        // Generate SQL for the migration
        context.apply { migration.run { up() } }
        val sql = context.generateSql().joinToString("\n")

        // Calculate SHA-256 hash
        val digest = MessageDigest.getInstance("SHA-256")
        val hashBytes = digest.digest(sql.toByteArray(Charsets.UTF_8))
        return hashBytes.joinToString("") { "%02x".format(it) }
    }

    /**
     * Verify checksum matches for a migration.
     */
    fun verifyChecksum(migration: Migration, storedChecksum: String, context: MigrationContext): Boolean {
        val currentChecksum = calculateChecksum(migration, context)
        return currentChecksum == storedChecksum
    }

    private fun parseTimestamp(value: Any?): Instant {
        return when (value) {
            is Instant -> value
            is java.sql.Timestamp -> value.toInstant()
            is java.time.LocalDateTime -> value.atZone(java.time.ZoneId.systemDefault()).toInstant()
            is String -> Instant.parse(value)
            else -> Instant.now()
        }
    }

    private fun escapeString(value: String): String {
        return value.replace("'", "''")
    }
}

/**
 * Extension to create a new MigrationContext for checksum calculation.
 */
fun createChecksumContext(dialect: SqlDialect): MigrationContext {
    val noOpExecutor = object : MigrationExecutor {
        override fun execute(sql: String) {}
        override fun executeQuery(sql: String): List<Map<String, Any?>> = emptyList()
    }
    return MigrationContext(dialect, noOpExecutor)
}
