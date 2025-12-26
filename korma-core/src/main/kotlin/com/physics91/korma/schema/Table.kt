package com.physics91.korma.schema

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import java.math.BigDecimal
import java.util.UUID

/**
 * Base class for defining database tables.
 *
 * Tables are defined as singleton objects extending this class.
 *
 * Example:
 * ```kotlin
 * object Users : Table("users") {
 *     val id = long("id").primaryKey().autoIncrement()
 *     val email = varchar("email", 255).unique().notNull()
 *     val name = varchar("name", 100).notNull()
 *     val age = integer("age").nullable()
 *     val createdAt = timestamp("created_at").default("CURRENT_TIMESTAMP")
 * }
 * ```
 *
 * @property tableName The name of the table in the database
 */
abstract class Table(val tableName: String) {

    /** All columns defined in this table */
    private val _columns = mutableListOf<Column<*>>()
    val columns: List<Column<*>> get() = _columns.toList()

    /** Primary key columns */
    private val _primaryKeyColumns = mutableListOf<Column<*>>()
    val primaryKey: List<Column<*>> get() = _primaryKeyColumns.toList()

    /** Indexes defined on this table */
    private val _indexes = mutableListOf<Index>()
    val indexes: List<Index> get() = _indexes.toList()

    /** Composite primary key definition */
    open val compositeKey: CompositeKey? = null

    // ============== Column Registration ==============

    /**
     * Register a column with this table.
     * Called internally by column factory methods.
     */
    protected fun <T> registerColumn(name: String, type: ColumnType<T>): Column<T> {
        val column = Column(this, name, type)
        _columns.add(column)
        return column
    }

    /**
     * Register a custom column type with this table.
     * This is for use by dialect-specific extensions.
     *
     * Example:
     * ```kotlin
     * fun Table.json(name: String): Column<String> =
     *     registerCustomColumn(name, JsonColumnType())
     * ```
     */
    fun <T> registerCustomColumn(name: String, type: ColumnType<T>): Column<T> =
        registerColumn(name, type)

    /**
     * Add an index to this table.
     * Called internally by column modifier methods.
     */
    internal fun addIndex(index: Index) {
        // Remove existing index with same name if present
        _indexes.removeIf { it.name == index.name }
        _indexes.add(index)
    }

    /**
     * Register a column as part of the primary key.
     * Called when primaryKey() is invoked on a column.
     */
    internal fun registerPrimaryKeyColumn(column: Column<*>) {
        if (column !in _primaryKeyColumns) {
            _primaryKeyColumns.add(column)
        }
    }

    // ============== Integer Types ==============

    /** Create an INT column */
    protected fun integer(name: String): Column<Int> =
        registerColumn(name, IntColumnType)

    /** Create a BIGINT column */
    protected fun long(name: String): Column<Long> =
        registerColumn(name, LongColumnType)

    /** Create a SMALLINT column */
    protected fun short(name: String): Column<Short> =
        registerColumn(name, ShortColumnType)

    // ============== Floating Point Types ==============

    /** Create a REAL column */
    protected fun float(name: String): Column<Float> =
        registerColumn(name, FloatColumnType)

    /** Create a DOUBLE PRECISION column */
    protected fun double(name: String): Column<Double> =
        registerColumn(name, DoubleColumnType)

    /** Create a DECIMAL column with precision and scale */
    protected fun decimal(name: String, precision: Int, scale: Int): Column<BigDecimal> =
        registerColumn(name, DecimalColumnType(precision, scale))

    // ============== Boolean Type ==============

    /** Create a BOOLEAN column */
    protected fun boolean(name: String): Column<Boolean> =
        registerColumn(name, BooleanColumnType)

    // ============== String Types ==============

    /** Create a VARCHAR column with specified length */
    protected fun varchar(name: String, length: Int): Column<String> =
        registerColumn(name, VarcharColumnType(length))

    /** Create a CHAR column with specified length */
    protected fun char(name: String, length: Int): Column<String> =
        registerColumn(name, CharColumnType(length))

    /** Create a TEXT column for unlimited text */
    protected fun text(name: String): Column<String> =
        registerColumn(name, TextColumnType)

    // ============== Date/Time Types ==============

    /** Create a TIMESTAMP WITH TIME ZONE column (kotlinx-datetime Instant) */
    protected fun timestamp(name: String): Column<Instant> =
        registerColumn(name, TimestampColumnType)

    /** Create a DATE column (kotlinx-datetime LocalDate) */
    protected fun date(name: String): Column<LocalDate> =
        registerColumn(name, DateColumnType)

    /** Create a TIME column (kotlinx-datetime LocalTime) */
    protected fun time(name: String): Column<LocalTime> =
        registerColumn(name, TimeColumnType)

    /** Create a TIMESTAMP column without timezone (kotlinx-datetime LocalDateTime) */
    protected fun datetime(name: String): Column<LocalDateTime> =
        registerColumn(name, DateTimeColumnType)

    // ============== Binary Types ==============

    /** Create a BYTEA/BINARY column */
    protected fun binary(name: String): Column<ByteArray> =
        registerColumn(name, BinaryColumnType)

    /** Create a BLOB column */
    protected fun blob(name: String): Column<ByteArray> =
        registerColumn(name, BlobColumnType)

    // ============== UUID Type ==============

    /** Create a UUID column */
    protected fun uuid(name: String): Column<UUID> =
        registerColumn(name, UUIDColumnType)

    // ============== Enum Types ==============

    /** Create a VARCHAR column storing enum by name */
    protected inline fun <reified T : Enum<T>> enumeration(name: String): Column<T> =
        registerColumn(name, EnumColumnType(T::class.java))

    /** Create an INT column storing enum by ordinal */
    protected inline fun <reified T : Enum<T>> enumerationByOrdinal(name: String): Column<T> =
        registerColumn(name, EnumOrdinalColumnType(T::class.java))

    // ============== Nullable Column Creation ==============

    /**
     * Extension function to make a column nullable.
     * Returns a new column with nullable type.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> Column<T>.nullable(): Column<T?> {
        // Create new column with nullable type
        val nullableColumn = Column(
            table = this@Table,
            name = this.name,
            type = this.type.asNullable() as ColumnType<T?>
        )

        // Copy properties
        nullableColumn.isPrimaryKey = this.isPrimaryKey
        nullableColumn.isAutoIncrement = this.isAutoIncrement
        nullableColumn.isUnique = this.isUnique
        nullableColumn.isNotNull = false
        nullableColumn.defaultValue = this.defaultValue
        nullableColumn.clientDefaultValue = this.clientDefaultValue
        nullableColumn.foreignKey = this.foreignKey
        nullableColumn.indexName = this.indexName

        // Replace in-place to preserve column order
        val columnIndex = _columns.indexOf(this)
        if (columnIndex >= 0) {
            _columns[columnIndex] = nullableColumn
        } else {
            _columns.add(nullableColumn)
        }

        // Update primary key tracking if necessary
        val pkIndex = _primaryKeyColumns.indexOfFirst { it === this }
        if (pkIndex >= 0) {
            _primaryKeyColumns[pkIndex] = nullableColumn
        } else if (nullableColumn.isPrimaryKey) {
            _primaryKeyColumns.add(nullableColumn)
        }

        // Update indexes referencing this column
        for (i in _indexes.indices) {
            val index = _indexes[i]
            if (index.columns.any { it === this }) {
                val updatedColumns = index.columns.map { col ->
                    if (col === this) nullableColumn else col
                }
                _indexes[i] = index.copy(columns = updatedColumns)
            }
        }

        return nullableColumn
    }

    // ============== Composite Primary Key ==============

    /**
     * Define a composite primary key.
     *
     * Example:
     * ```kotlin
     * object OrderItems : Table("order_items") {
     *     val orderId = long("order_id")
     *     val productId = long("product_id")
     *     val quantity = integer("quantity")
     *
     *     override val compositeKey = primaryKey(orderId, productId)
     * }
     * ```
     */
    protected fun primaryKey(vararg columns: Column<*>): CompositeKey {
        columns.forEach {
            it.isPrimaryKey = true
            registerPrimaryKeyColumn(it)
        }
        return CompositeKey(columns.toList())
    }

    // ============== Index Creation ==============

    /**
     * Create a multi-column index.
     *
     * Example:
     * ```kotlin
     * init {
     *     index("idx_name_email", name, email)
     * }
     * ```
     */
    protected fun index(name: String, vararg columns: Column<*>, unique: Boolean = false) {
        addIndex(Index(name, columns.toList(), unique))
    }

    /**
     * Create a multi-column unique index.
     */
    protected fun uniqueIndex(name: String, vararg columns: Column<*>) {
        index(name, *columns, unique = true)
    }

    // ============== Utility Methods ==============

    /** Get a column by name */
    fun getColumn(name: String): Column<*>? = _columns.find { it.name == name }

    /** Check if a column exists */
    fun hasColumn(name: String): Boolean = _columns.any { it.name == name }

    override fun toString(): String = tableName

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Table) return false
        return tableName == other.tableName
    }

    override fun hashCode(): Int = tableName.hashCode()
}

/**
 * Represents a composite primary key.
 */
data class CompositeKey(
    val columns: List<Column<*>>
) {
    val columnNames: List<String>
        get() = columns.map { it.name }
}

// ============== Common Table Patterns ==============

/**
 * Table with auto-incrementing integer ID.
 */
abstract class IntIdTable(tableName: String, idColumnName: String = "id") : Table(tableName) {
    val id: Column<Int> = integer(idColumnName).primaryKey().autoIncrement()
}

/**
 * Table with auto-incrementing long ID.
 */
abstract class LongIdTable(tableName: String, idColumnName: String = "id") : Table(tableName) {
    val id: Column<Long> = long(idColumnName).primaryKey().autoIncrement()
}

/**
 * Table with UUID primary key.
 */
abstract class UUIDTable(tableName: String, idColumnName: String = "id") : Table(tableName) {
    val id: Column<UUID> = uuid(idColumnName).primaryKey().clientDefault { UUID.randomUUID() }
}
