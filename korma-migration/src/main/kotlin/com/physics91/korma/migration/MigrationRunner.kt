package com.physics91.korma.migration

import com.physics91.korma.sql.SqlDialect
import org.slf4j.LoggerFactory
import kotlin.system.measureTimeMillis

/**
 * Executes database migrations in order.
 * Handles up/down migrations with version tracking.
 */
class MigrationRunner(
    private val executor: MigrationExecutor,
    private val dialect: SqlDialect,
    private val migrations: List<Migration>,
    trackingTableName: String = "_korma_migrations"
) {
    private val logger = LoggerFactory.getLogger(MigrationRunner::class.java)
    private val tracker = MigrationTracker(executor, dialect, trackingTableName)

    /**
     * Initialize the migration system.
     * Creates the tracking table if it doesn't exist.
     */
    fun initialize() {
        logger.info("Initializing migration system")
        tracker.ensureTableExists()
    }

    /**
     * Run all pending migrations.
     */
    fun migrate(): List<MigrationResult> {
        initialize()
        val results = mutableListOf<MigrationResult>()

        val sortedMigrations = migrations.sortedBy { it.version }
        val appliedVersions = tracker.getAppliedMigrations()
            .filter { it.success }
            .map { it.version }
            .toSet()

        for (migration in sortedMigrations) {
            if (migration.version in appliedVersions) {
                logger.debug("Skipping already applied migration: ${migration.version}")
                continue
            }

            // Check for failed migrations
            val existingRecord = tracker.getMigrationRecord(migration.version)
            if (existingRecord != null && !existingRecord.success) {
                logger.warn("Migration ${migration.version} previously failed. Retrying...")
            }

            val result = runMigration(migration, MigrationDirection.UP)
            results.add(result)

            if (result is MigrationResult.Failure && !result.partiallyApplied) {
                logger.error("Migration failed, stopping execution: ${migration.version}")
                break
            }
        }

        return results
    }

    /**
     * Run migrations up to a specific version.
     */
    fun migrateTo(targetVersion: String): List<MigrationResult> {
        initialize()
        val results = mutableListOf<MigrationResult>()

        val sortedMigrations = migrations.sortedBy { it.version }
        val appliedVersions = tracker.getAppliedMigrations()
            .filter { it.success }
            .map { it.version }
            .toSet()

        for (migration in sortedMigrations) {
            if (migration.version > targetVersion) {
                break
            }

            if (migration.version in appliedVersions) {
                continue
            }

            val result = runMigration(migration, MigrationDirection.UP)
            results.add(result)

            if (result is MigrationResult.Failure) {
                break
            }
        }

        return results
    }

    /**
     * Rollback the last N migrations.
     */
    fun rollback(count: Int = 1): List<MigrationResult> {
        initialize()
        val results = mutableListOf<MigrationResult>()

        val appliedMigrations = tracker.getAppliedMigrations()
            .filter { it.success }
            .sortedByDescending { it.executedAt }
            .take(count)

        for (record in appliedMigrations) {
            val migration = migrations.find { it.version == record.version }
            if (migration == null) {
                logger.error("Migration ${record.version} not found in migration list")
                results.add(MigrationResult.Failure(
                    record.version,
                    IllegalStateException("Migration not found"),
                    partiallyApplied = false
                ))
                continue
            }

            val result = runMigration(migration, MigrationDirection.DOWN)
            results.add(result)

            if (result is MigrationResult.Failure) {
                break
            }
        }

        return results
    }

    /**
     * Rollback to a specific version (exclusive).
     */
    fun rollbackTo(targetVersion: String): List<MigrationResult> {
        initialize()
        val results = mutableListOf<MigrationResult>()

        val appliedMigrations = tracker.getAppliedMigrations()
            .filter { it.success && it.version > targetVersion }
            .sortedByDescending { it.executedAt }

        for (record in appliedMigrations) {
            val migration = migrations.find { it.version == record.version }
            if (migration == null) {
                logger.error("Migration ${record.version} not found")
                continue
            }

            val result = runMigration(migration, MigrationDirection.DOWN)
            results.add(result)

            if (result is MigrationResult.Failure) {
                break
            }
        }

        return results
    }

    /**
     * Get the status of all migrations.
     */
    fun getStatus(): List<MigrationInfo> {
        initialize()
        val appliedMigrations = tracker.getAppliedMigrations().associateBy { it.version }

        return migrations.sortedBy { it.version }.map { migration ->
            val record = appliedMigrations[migration.version]

            val status = when {
                record == null -> MigrationStatus.PENDING
                !record.success -> MigrationStatus.FAILED
                else -> {
                    // Verify checksum
                    val context = createChecksumContext(dialect)
                    val isChecksumValid = tracker.verifyChecksum(migration, record.checksum, context)
                    if (isChecksumValid) MigrationStatus.APPLIED else MigrationStatus.CHECKSUM_MISMATCH
                }
            }

            MigrationInfo(migration, status, record)
        }
    }

    /**
     * Get pending migrations.
     */
    fun getPendingMigrations(): List<Migration> {
        val appliedVersions = tracker.getAppliedMigrations()
            .filter { it.success }
            .map { it.version }
            .toSet()

        return migrations
            .filter { it.version !in appliedVersions }
            .sortedBy { it.version }
    }

    /**
     * Generate SQL for all pending migrations without executing.
     */
    fun generatePendingSql(): Map<String, List<String>> {
        val pending = getPendingMigrations()
        return pending.associate { migration ->
            val context = createChecksumContext(dialect)
            context.apply { migration.run { up() } }
            migration.version to context.generateSql()
        }
    }

    /**
     * Validate all migrations (check for issues without executing).
     */
    fun validate(): List<MigrationValidationResult> {
        val results = mutableListOf<MigrationValidationResult>()
        val versions = mutableSetOf<String>()

        for (migration in migrations) {
            val issues = mutableListOf<String>()

            // Check for duplicate versions
            if (migration.version in versions) {
                issues.add("Duplicate version: ${migration.version}")
            }
            versions.add(migration.version)

            // Check if up() can generate SQL
            try {
                val upContext = createChecksumContext(dialect)
                upContext.apply { migration.run { up() } }
                val upSql = upContext.generateSql()
                if (upSql.isEmpty()) {
                    issues.add("up() generates no SQL")
                }
            } catch (e: Exception) {
                issues.add("up() throws exception: ${e.message}")
            }

            // Check if down() can generate SQL
            try {
                val downContext = createChecksumContext(dialect)
                downContext.apply { migration.run { down() } }
                val downSql = downContext.generateSql()
                if (downSql.isEmpty()) {
                    issues.add("down() generates no SQL (may be intentional)")
                }
            } catch (e: Exception) {
                issues.add("down() throws exception: ${e.message}")
            }

            results.add(MigrationValidationResult(
                version = migration.version,
                isValid = issues.isEmpty(),
                issues = issues
            ))
        }

        return results
    }

    // ============== Private Methods ==============

    private fun runMigration(migration: Migration, direction: MigrationDirection): MigrationResult {
        val context = MigrationContext(dialect, executor)
        val version = migration.version

        logger.info("Running migration ${direction.name}: $version - ${migration.description}")

        var executionTimeMs = 0L
        try {
            executionTimeMs = measureTimeMillis {
                when (direction) {
                    MigrationDirection.UP -> context.apply { migration.run { up() } }
                    MigrationDirection.DOWN -> context.apply { migration.run { down() } }
                }
                context.execute()
            }

            when (direction) {
                MigrationDirection.UP -> {
                    val checksumContext = createChecksumContext(dialect)
                    val checksum = tracker.calculateChecksum(migration, checksumContext)

                    // Check if there's a failed record to update
                    val existingRecord = tracker.getMigrationRecord(version)
                    if (existingRecord != null) {
                        tracker.markSuccess(version, executionTimeMs)
                    } else {
                        tracker.recordMigration(
                            version = version,
                            description = migration.description,
                            checksum = checksum,
                            executionTimeMs = executionTimeMs,
                            success = true
                        )
                    }
                }
                MigrationDirection.DOWN -> {
                    tracker.removeMigration(version)
                }
            }

            logger.info("Migration $version completed in ${executionTimeMs}ms")
            return MigrationResult.Success(version, executionTimeMs)

        } catch (e: Exception) {
            logger.error("Migration $version failed: ${e.message}", e)

            if (direction == MigrationDirection.UP) {
                val checksumContext = createChecksumContext(dialect)
                val checksum = tracker.calculateChecksum(migration, checksumContext)
                tracker.markFailed(version, migration.description, checksum, executionTimeMs)
            }

            return MigrationResult.Failure(version, e, partiallyApplied = executionTimeMs > 0)
        }
    }
}

/**
 * Result of migration validation.
 */
data class MigrationValidationResult(
    val version: String,
    val isValid: Boolean,
    val issues: List<String>
)
