package com.physics91.korma.migration

import com.physics91.korma.schema.Column
import com.physics91.korma.schema.Table
import java.time.Instant

/**
 * Represents a database migration.
 *
 * Migrations are versioned changes to the database schema that can be
 * applied (up) or reverted (down).
 */
interface Migration {
    /** Unique version identifier (e.g., "V1", "V2_add_users_table") */
    val version: String

    /** Human-readable description of the migration */
    val description: String

    /** Apply the migration (create tables, add columns, etc.) */
    fun MigrationContext.up()

    /** Revert the migration (drop tables, remove columns, etc.) */
    fun MigrationContext.down()
}

/**
 * Abstract base class for migrations with automatic version extraction.
 */
abstract class BaseMigration(
    override val version: String,
    override val description: String
) : Migration

/**
 * Migration metadata stored in the tracking table.
 */
data class MigrationRecord(
    val version: String,
    val description: String,
    val checksum: String,
    val executedAt: Instant,
    val executionTimeMs: Long,
    val success: Boolean
)

/**
 * Migration status for a specific version.
 */
enum class MigrationStatus {
    /** Migration has not been applied yet */
    PENDING,
    /** Migration has been successfully applied */
    APPLIED,
    /** Migration failed during execution */
    FAILED,
    /** Migration checksum doesn't match (schema changed) */
    CHECKSUM_MISMATCH
}

/**
 * Information about a migration and its current status.
 */
data class MigrationInfo(
    val migration: Migration,
    val status: MigrationStatus,
    val record: MigrationRecord? = null
)

/**
 * Result of a migration operation.
 */
sealed class MigrationResult {
    data class Success(
        val version: String,
        val executionTimeMs: Long
    ) : MigrationResult()

    data class Failure(
        val version: String,
        val error: Throwable,
        val partiallyApplied: Boolean = false
    ) : MigrationResult()

    data class Skipped(
        val version: String,
        val reason: String
    ) : MigrationResult()
}

/**
 * Direction of migration execution.
 */
enum class MigrationDirection {
    UP,
    DOWN
}
