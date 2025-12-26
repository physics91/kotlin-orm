package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.ResultSet

/**
 * Introspects database schema to extract table and column metadata.
 *
 * Usage:
 * ```kotlin
 * val introspector = SchemaIntrospector(config)
 * val schema = introspector.introspect()
 *
 * schema.tables.forEach { table ->
 *     println("Table: ${table.name}")
 *     table.columns.forEach { column ->
 *         println("  Column: ${column.name} (${column.type})")
 *     }
 * }
 * ```
 */
class SchemaIntrospector(private val config: CodegenConfig) {

    private val logger = LoggerFactory.getLogger(SchemaIntrospector::class.java)

    /**
     * Introspect the database schema and return metadata.
     */
    fun introspect(): DatabaseSchema {
        logger.info("Introspecting database schema from ${config.databaseConfig.url}")

        val tables = mutableListOf<TableMetadata>()

        getConnection().use { connection ->
            val metaData = connection.metaData
            val catalog = connection.catalog
            val schema = config.schema

            // Get all tables
            val tablesRs = metaData.getTables(catalog, schema, null, arrayOf("TABLE"))

            while (tablesRs.next()) {
                val tableName = tablesRs.getString("TABLE_NAME")
                val tableSchema = tablesRs.getString("TABLE_SCHEM")
                val tableRemarks = tablesRs.getString("REMARKS")

                // Skip system schemas (H2, PostgreSQL, MySQL system tables)
                if (isSystemSchema(tableSchema)) {
                    logger.debug("Skipping system table: $tableSchema.$tableName")
                    continue
                }

                // Check if table matches include/exclude patterns
                if (!shouldIncludeTable(tableName)) {
                    logger.debug("Skipping table: $tableName (excluded by pattern)")
                    continue
                }

                logger.debug("Processing table: $tableName")

                val columns = getColumns(metaData, catalog, tableSchema, tableName)
                val primaryKeys = getPrimaryKeys(metaData, catalog, tableSchema, tableName)
                val foreignKeys = getForeignKeys(metaData, catalog, tableSchema, tableName)
                val uniqueConstraints = getUniqueConstraints(metaData, catalog, tableSchema, tableName)
                val indices = getIndices(metaData, catalog, tableSchema, tableName)

                tables.add(
                    TableMetadata(
                        name = tableName,
                        schema = tableSchema,
                        remarks = tableRemarks,
                        columns = columns,
                        primaryKeyColumns = primaryKeys,
                        foreignKeys = foreignKeys,
                        uniqueConstraints = uniqueConstraints,
                        indices = indices
                    )
                )
            }
        }

        logger.info("Introspection complete. Found ${tables.size} tables")
        return DatabaseSchema(tables)
    }

    private fun shouldIncludeTable(tableName: String): Boolean {
        // Case-insensitive pattern matching
        val matchesInclude = config.includePatterns.any { pattern ->
            Regex(pattern.pattern, RegexOption.IGNORE_CASE).matches(tableName)
        }
        val matchesExclude = config.excludePatterns.any { pattern ->
            Regex(pattern.pattern, RegexOption.IGNORE_CASE).matches(tableName)
        }
        return matchesInclude && !matchesExclude
    }

    private fun isSystemSchema(schemaName: String?): Boolean {
        if (schemaName == null) return false
        val systemSchemas = setOf(
            // H2 system schemas
            "INFORMATION_SCHEMA",
            // PostgreSQL system schemas
            "pg_catalog",
            "pg_toast",
            "information_schema",
            // MySQL system schemas
            "mysql",
            "performance_schema",
            "sys",
            "information_schema"
        )
        return schemaName.uppercase() in systemSchemas.map { it.uppercase() }
    }

    private fun getColumns(
        metaData: DatabaseMetaData,
        catalog: String?,
        schema: String?,
        tableName: String
    ): List<ColumnMetadata> {
        val columns = mutableListOf<ColumnMetadata>()

        val columnsRs = metaData.getColumns(catalog, schema, tableName, null)
        while (columnsRs.next()) {
            columns.add(
                ColumnMetadata(
                    name = columnsRs.getString("COLUMN_NAME"),
                    sqlType = columnsRs.getInt("DATA_TYPE"),
                    typeName = columnsRs.getString("TYPE_NAME"),
                    size = columnsRs.getInt("COLUMN_SIZE"),
                    decimalDigits = columnsRs.getInt("DECIMAL_DIGITS"),
                    nullable = columnsRs.getInt("NULLABLE") == DatabaseMetaData.columnNullable,
                    defaultValue = columnsRs.getString("COLUMN_DEF"),
                    remarks = columnsRs.getString("REMARKS"),
                    ordinalPosition = columnsRs.getInt("ORDINAL_POSITION"),
                    autoIncrement = columnsRs.getString("IS_AUTOINCREMENT") == "YES"
                )
            )
        }

        return columns.sortedBy { it.ordinalPosition }
    }

    private fun getPrimaryKeys(
        metaData: DatabaseMetaData,
        catalog: String?,
        schema: String?,
        tableName: String
    ): List<String> {
        val primaryKeys = mutableListOf<Pair<String, Int>>()

        val pkRs = metaData.getPrimaryKeys(catalog, schema, tableName)
        while (pkRs.next()) {
            val columnName = pkRs.getString("COLUMN_NAME")
            val keySeq = pkRs.getInt("KEY_SEQ")
            primaryKeys.add(columnName to keySeq)
        }

        return primaryKeys.sortedBy { it.second }.map { it.first }
    }

    private fun getForeignKeys(
        metaData: DatabaseMetaData,
        catalog: String?,
        schema: String?,
        tableName: String
    ): List<ForeignKeyMetadata> {
        val foreignKeys = mutableListOf<ForeignKeyMetadata>()

        val fkRs = metaData.getImportedKeys(catalog, schema, tableName)
        val fkMap = mutableMapOf<String, MutableList<ForeignKeyColumn>>()

        while (fkRs.next()) {
            val fkName = fkRs.getString("FK_NAME") ?: "fk_${tableName}_${fkRs.getString("FKCOLUMN_NAME")}"
            val fkColumn = ForeignKeyColumn(
                localColumn = fkRs.getString("FKCOLUMN_NAME"),
                foreignTable = fkRs.getString("PKTABLE_NAME"),
                foreignColumn = fkRs.getString("PKCOLUMN_NAME"),
                keySeq = fkRs.getInt("KEY_SEQ")
            )
            fkMap.getOrPut(fkName) { mutableListOf() }.add(fkColumn)
        }

        fkMap.forEach { (name, columns) ->
            val sortedColumns = columns.sortedBy { it.keySeq }
            foreignKeys.add(
                ForeignKeyMetadata(
                    name = name,
                    localColumns = sortedColumns.map { it.localColumn },
                    foreignTable = sortedColumns.first().foreignTable,
                    foreignColumns = sortedColumns.map { it.foreignColumn }
                )
            )
        }

        return foreignKeys
    }

    private fun getUniqueConstraints(
        metaData: DatabaseMetaData,
        catalog: String?,
        schema: String?,
        tableName: String
    ): List<UniqueConstraintMetadata> {
        val constraints = mutableMapOf<String, MutableList<Pair<String, Int>>>()

        val indexRs = metaData.getIndexInfo(catalog, schema, tableName, true, false)
        while (indexRs.next()) {
            val indexName = indexRs.getString("INDEX_NAME") ?: continue
            val columnName = indexRs.getString("COLUMN_NAME") ?: continue
            val ordinalPosition = indexRs.getInt("ORDINAL_POSITION")

            constraints.getOrPut(indexName) { mutableListOf() }
                .add(columnName to ordinalPosition)
        }

        return constraints.map { (name, columns) ->
            UniqueConstraintMetadata(
                name = name,
                columns = columns.sortedBy { it.second }.map { it.first }
            )
        }
    }

    private fun getIndices(
        metaData: DatabaseMetaData,
        catalog: String?,
        schema: String?,
        tableName: String
    ): List<IndexMetadata> {
        val indices = mutableMapOf<String, MutableList<IndexColumn>>()

        val indexRs = metaData.getIndexInfo(catalog, schema, tableName, false, false)
        while (indexRs.next()) {
            val indexName = indexRs.getString("INDEX_NAME") ?: continue
            val columnName = indexRs.getString("COLUMN_NAME") ?: continue
            val unique = !indexRs.getBoolean("NON_UNIQUE")
            val ordinalPosition = indexRs.getInt("ORDINAL_POSITION")
            val ascending = indexRs.getString("ASC_OR_DESC") != "D"

            indices.getOrPut(indexName) { mutableListOf() }.add(
                IndexColumn(
                    name = columnName,
                    ordinalPosition = ordinalPosition,
                    ascending = ascending,
                    unique = unique
                )
            )
        }

        return indices.map { (name, columns) ->
            IndexMetadata(
                name = name,
                columns = columns.sortedBy { it.ordinalPosition }.map { it.name },
                unique = columns.firstOrNull()?.unique ?: false
            )
        }
    }

    private fun getConnection(): Connection {
        val dbConfig = config.databaseConfig

        // Load driver if specified
        dbConfig.driverClassName?.let { driverClass ->
            try {
                Class.forName(driverClass)
            } catch (e: ClassNotFoundException) {
                logger.warn("Could not load driver class: $driverClass")
            }
        }

        return if (dbConfig.username != null) {
            DriverManager.getConnection(dbConfig.url, dbConfig.username, dbConfig.password)
        } else {
            DriverManager.getConnection(dbConfig.url)
        }
    }

    private data class ForeignKeyColumn(
        val localColumn: String,
        val foreignTable: String,
        val foreignColumn: String,
        val keySeq: Int
    )

    private data class IndexColumn(
        val name: String,
        val ordinalPosition: Int,
        val ascending: Boolean,
        val unique: Boolean
    )
}

/**
 * Represents the complete database schema.
 */
data class DatabaseSchema(
    val tables: List<TableMetadata>
) {
    fun getTable(name: String): TableMetadata? {
        return tables.find { it.name.equals(name, ignoreCase = true) }
    }
}

/**
 * Metadata for a database table.
 */
data class TableMetadata(
    val name: String,
    val schema: String?,
    val remarks: String?,
    val columns: List<ColumnMetadata>,
    val primaryKeyColumns: List<String>,
    val foreignKeys: List<ForeignKeyMetadata>,
    val uniqueConstraints: List<UniqueConstraintMetadata>,
    val indices: List<IndexMetadata>
) {
    val hasPrimaryKey: Boolean get() = primaryKeyColumns.isNotEmpty()
    val hasCompositePrimaryKey: Boolean get() = primaryKeyColumns.size > 1
    val primaryKeyColumn: ColumnMetadata?
        get() = primaryKeyColumns.singleOrNull()?.let { pk ->
            columns.find { it.name == pk }
        }
}

/**
 * Metadata for a database column.
 */
data class ColumnMetadata(
    val name: String,
    val sqlType: Int,
    val typeName: String,
    val size: Int,
    val decimalDigits: Int,
    val nullable: Boolean,
    val defaultValue: String?,
    val remarks: String?,
    val ordinalPosition: Int,
    val autoIncrement: Boolean
)

/**
 * Metadata for a foreign key constraint.
 */
data class ForeignKeyMetadata(
    val name: String,
    val localColumns: List<String>,
    val foreignTable: String,
    val foreignColumns: List<String>
) {
    val isSimple: Boolean get() = localColumns.size == 1
}

/**
 * Metadata for a unique constraint.
 */
data class UniqueConstraintMetadata(
    val name: String,
    val columns: List<String>
)

/**
 * Metadata for an index.
 */
data class IndexMetadata(
    val name: String,
    val columns: List<String>,
    val unique: Boolean
)
