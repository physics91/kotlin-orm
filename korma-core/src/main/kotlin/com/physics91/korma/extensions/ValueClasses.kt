package com.physics91.korma.extensions

/**
 * Kotlin value classes for Korma.
 *
 * Value classes provide type-safe wrappers with zero runtime overhead,
 * preventing common mistakes like mixing up parameters of the same primitive type.
 *
 * Usage:
 * ```kotlin
 * // Instead of error-prone primitives:
 * fun findPage(page: Int, size: Int) // Easy to mix up!
 *
 * // Use type-safe value classes:
 * fun findPage(page: PageNumber, size: PageSize) // Compile-time safety!
 *
 * // Usage
 * findPage(PageNumber(2), PageSize(20))
 * ```
 */

// ============== Pagination Value Classes ==============

/**
 * Represents a page number in pagination (1-indexed).
 *
 * @throws IllegalArgumentException if page number is less than 1
 */
@JvmInline
value class PageNumber(val value: Int) {
    init {
        require(value >= 1) { "Page number must be >= 1, got $value" }
    }

    /**
     * Converts to zero-based offset for SQL queries.
     */
    fun toOffset(pageSize: PageSize): Long = ((value - 1) * pageSize.value).toLong()

    operator fun plus(other: Int): PageNumber = PageNumber(value + other)
    operator fun minus(other: Int): PageNumber = PageNumber(value - other)

    override fun toString(): String = value.toString()
}

/**
 * Represents a page size in pagination.
 *
 * @throws IllegalArgumentException if page size is less than 1 or greater than max
 */
@JvmInline
value class PageSize(val value: Int) {
    init {
        require(value in 1..MAX_PAGE_SIZE) { "Page size must be between 1 and $MAX_PAGE_SIZE, got $value" }
    }

    override fun toString(): String = value.toString()

    companion object {
        const val MAX_PAGE_SIZE = 1000
        val DEFAULT = PageSize(20)
        val SMALL = PageSize(10)
        val MEDIUM = PageSize(50)
        val LARGE = PageSize(100)
    }
}

/**
 * Represents a row limit for queries.
 *
 * @throws IllegalArgumentException if limit is less than 0
 */
@JvmInline
value class Limit(val value: Int) {
    init {
        require(value >= 0) { "Limit must be >= 0, got $value" }
    }

    override fun toString(): String = value.toString()

    companion object {
        val NONE = Limit(0)
        val ONE = Limit(1)
        val TEN = Limit(10)
        val HUNDRED = Limit(100)
    }
}

/**
 * Represents a row offset for queries.
 *
 * @throws IllegalArgumentException if offset is negative
 */
@JvmInline
value class Offset(val value: Long) {
    init {
        require(value >= 0) { "Offset must be >= 0, got $value" }
    }

    override fun toString(): String = value.toString()

    companion object {
        val ZERO = Offset(0)
    }
}

// ============== Database Identifier Value Classes ==============

/**
 * Represents a table name in the database.
 * Ensures valid naming and provides quoting utilities.
 */
@JvmInline
value class TableName(val value: String) {
    init {
        require(value.isNotBlank()) { "Table name cannot be blank" }
        require(value.length <= MAX_IDENTIFIER_LENGTH) {
            "Table name exceeds maximum length of $MAX_IDENTIFIER_LENGTH characters"
        }
    }

    override fun toString(): String = value

    companion object {
        const val MAX_IDENTIFIER_LENGTH = 128
    }
}

/**
 * Represents a column name in the database.
 */
@JvmInline
value class ColumnName(val value: String) {
    init {
        require(value.isNotBlank()) { "Column name cannot be blank" }
        require(value.length <= MAX_IDENTIFIER_LENGTH) {
            "Column name exceeds maximum length of $MAX_IDENTIFIER_LENGTH characters"
        }
    }

    override fun toString(): String = value

    companion object {
        const val MAX_IDENTIFIER_LENGTH = 128
    }
}

/**
 * Represents a database schema name.
 */
@JvmInline
value class SchemaName(val value: String) {
    init {
        require(value.isNotBlank()) { "Schema name cannot be blank" }
    }

    override fun toString(): String = value

    companion object {
        val PUBLIC = SchemaName("public")
        val DBO = SchemaName("dbo")
    }
}

// ============== Query Result Value Classes ==============

/**
 * Represents a count of affected rows from INSERT/UPDATE/DELETE.
 */
@JvmInline
value class AffectedRows(val value: Long) {
    val wasSuccessful: Boolean get() = value > 0

    operator fun plus(other: AffectedRows): AffectedRows = AffectedRows(value + other.value)

    override fun toString(): String = value.toString()
}

/**
 * Represents a count of rows from COUNT queries.
 */
@JvmInline
value class RowCount(val value: Long) {
    val isEmpty: Boolean get() = value == 0L
    val isNotEmpty: Boolean get() = value > 0

    override fun toString(): String = value.toString()
}

// ============== Database ID Value Classes ==============

/**
 * Represents a database-generated ID (Long).
 * Commonly used for auto-increment primary keys.
 */
@JvmInline
value class EntityId(val value: Long) {
    init {
        require(value > 0) { "Entity ID must be positive, got $value" }
    }

    override fun toString(): String = value.toString()
}

/**
 * Represents a UUID-based entity identifier.
 */
@JvmInline
value class EntityUuid(val value: java.util.UUID) {
    override fun toString(): String = value.toString()

    companion object {
        fun random(): EntityUuid = EntityUuid(java.util.UUID.randomUUID())
        fun fromString(uuid: String): EntityUuid = EntityUuid(java.util.UUID.fromString(uuid))
    }
}

// ============== Connection Pool Value Classes ==============

/**
 * Represents connection pool size.
 */
@JvmInline
value class PoolSize(val value: Int) {
    init {
        require(value in 1..MAX_POOL_SIZE) { "Pool size must be between 1 and $MAX_POOL_SIZE, got $value" }
    }

    override fun toString(): String = value.toString()

    companion object {
        const val MAX_POOL_SIZE = 1000
        val DEFAULT = PoolSize(10)
        val SMALL = PoolSize(5)
        val LARGE = PoolSize(50)
    }
}

// ============== Pagination Result Helpers ==============

/**
 * Represents pagination information for query results.
 */
data class PageInfo(
    val pageNumber: PageNumber,
    val pageSize: PageSize,
    val totalItems: Long,
    val totalPages: Int = calculateTotalPages(totalItems, pageSize.value)
) {
    val hasNext: Boolean get() = pageNumber.value < totalPages
    val hasPrevious: Boolean get() = pageNumber.value > 1
    val isFirst: Boolean get() = pageNumber.value == 1
    val isLast: Boolean get() = pageNumber.value == totalPages || totalPages == 0

    companion object {
        private fun calculateTotalPages(totalItems: Long, pageSize: Int): Int =
            ((totalItems + pageSize - 1) / pageSize).toInt()
    }
}

/**
 * Represents a paginated result set.
 */
data class Page<T>(
    val content: List<T>,
    val pageInfo: PageInfo
) {
    val isEmpty: Boolean get() = content.isEmpty()
    val size: Int get() = content.size

    fun <R> map(transform: (T) -> R): Page<R> = Page(content.map(transform), pageInfo)

    companion object {
        fun <T> empty(pageSize: PageSize = PageSize.DEFAULT): Page<T> = Page(
            content = emptyList(),
            pageInfo = PageInfo(PageNumber(1), pageSize, 0)
        )
    }
}
