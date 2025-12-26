package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.NamingStrategy
import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.sql.Types

/**
 * Generates Kotlin code from database schema metadata.
 *
 * Usage:
 * ```kotlin
 * val generator = KotlinCodeGenerator(config)
 * val files = generator.generate(schema)
 *
 * // Write files to disk
 * files.forEach { it.writeTo(config.outputDirectory) }
 * ```
 */
class KotlinCodeGenerator(private val config: CodegenConfig) {

    private val logger = LoggerFactory.getLogger(KotlinCodeGenerator::class.java)
    private val namingStrategy: NamingStrategy = config.namingStrategy

    /**
     * Generate all code files from the database schema.
     */
    fun generate(schema: DatabaseSchema): List<FileSpec> {
        val files = mutableListOf<FileSpec>()

        schema.tables.forEach { table ->
            logger.debug("Generating code for table: ${table.name}")

            // Generate Table object
            files.add(generateTableObject(table))

            // Generate Entity class if enabled
            if (config.generateEntities) {
                files.add(generateEntityClass(table))
            }
        }

        logger.info("Generated ${files.size} files for ${schema.tables.size} tables")
        return files
    }

    /**
     * Generate all files and write to disk.
     */
    fun generateAndWrite(schema: DatabaseSchema): List<Path> {
        val files = generate(schema)
        val writtenPaths = mutableListOf<Path>()

        files.forEach { fileSpec ->
            fileSpec.writeTo(config.outputDirectory)
            val path = config.outputDirectory
                .resolve(fileSpec.packageName.replace('.', '/'))
                .resolve("${fileSpec.name}.kt")
            writtenPaths.add(path)
            logger.info("Wrote: $path")
        }

        return writtenPaths
    }

    /**
     * Generate a Korma Table object for a database table.
     */
    private fun generateTableObject(table: TableMetadata): FileSpec {
        val className = namingStrategy.tableToClassName(table.name)
        val tableClass = ClassName("com.physics91.korma.schema", "Table")

        val objectBuilder = TypeSpec.objectBuilder(className)
            .superclass(tableClass)
            .addSuperclassConstructorParameter("%S", table.name)

        // Add KDoc if enabled
        if (config.generateKdoc) {
            val kdoc = buildString {
                appendLine("Korma Table definition for `${table.name}`.")
                table.remarks?.let { appendLine("\n$it") }
            }
            objectBuilder.addKdoc(kdoc)
        }

        // Generate column properties
        table.columns.forEach { column ->
            val columnProperty = generateColumnProperty(column, table)
            objectBuilder.addProperty(columnProperty)
        }

        // Add file header comment
        val fileBuilder = FileSpec.builder(config.packageName, className)

        config.fileHeader?.let { header ->
            fileBuilder.addFileComment(header)
        }

        return fileBuilder
            .addType(objectBuilder.build())
            .build()
    }

    /**
     * Generate a property for a table column.
     */
    private fun generateColumnProperty(
        column: ColumnMetadata,
        table: TableMetadata
    ): PropertySpec {
        val propertyName = namingStrategy.columnToPropertyName(column.name)
        val isPrimaryKey = table.primaryKeyColumns.contains(column.name)
        val columnType = mapSqlTypeToKorma(column)

        // Build the column initialization chain
        val initializerBuilder = StringBuilder()
        initializerBuilder.append(columnType.functionCall)

        // Add column modifiers
        if (isPrimaryKey) {
            initializerBuilder.append(".primaryKey()")
        }
        if (column.autoIncrement) {
            initializerBuilder.append(".autoIncrement()")
        }
        if (column.nullable && config.useNullableTypes && !isPrimaryKey) {
            initializerBuilder.append(".nullable()")
        }
        if (column.defaultValue != null && !column.autoIncrement) {
            // Handle default value - this is tricky as we need to convert SQL default to Kotlin
            // For now, skip complex defaults
        }

        val propertyBuilder = PropertySpec.builder(
            propertyName,
            columnType.propertyType
        ).initializer(initializerBuilder.toString())

        // Add KDoc for column
        if (config.generateKdoc) {
            val kdoc = buildString {
                append("Column `${column.name}`")
                if (column.remarks != null) {
                    append(" - ${column.remarks}")
                }
                append(" (${column.typeName}")
                if (column.size > 0) {
                    append("($column.size)")
                }
                append(")")
            }
            propertyBuilder.addKdoc(kdoc)
        }

        return propertyBuilder.build()
    }

    /**
     * Generate an entity data class for a database table.
     */
    private fun generateEntityClass(table: TableMetadata): FileSpec {
        val className = namingStrategy.tableToEntityName(table.name)

        val constructorBuilder = FunSpec.constructorBuilder()
        val classBuilder = TypeSpec.classBuilder(className)
            .addModifiers(KModifier.DATA)

        // Add KDoc if enabled
        if (config.generateKdoc) {
            val kdoc = buildString {
                appendLine("Entity class for table `${table.name}`.")
                table.remarks?.let { appendLine("\n$it") }
            }
            classBuilder.addKdoc(kdoc)
        }

        // Generate properties for each column
        table.columns.forEach { column ->
            val propertyName = namingStrategy.columnToPropertyName(column.name)
            val kotlinType = mapSqlTypeToKotlin(column)

            // Add constructor parameter
            constructorBuilder.addParameter(
                ParameterSpec.builder(propertyName, kotlinType)
                    .apply {
                        // Add default value for nullable types
                        if (column.nullable && config.useNullableTypes) {
                            defaultValue("null")
                        }
                    }
                    .build()
            )

            // Add property
            classBuilder.addProperty(
                PropertySpec.builder(propertyName, kotlinType)
                    .initializer(propertyName)
                    .build()
            )
        }

        classBuilder.primaryConstructor(constructorBuilder.build())

        // Add file header comment
        val fileBuilder = FileSpec.builder(config.packageName, className)

        config.fileHeader?.let { header ->
            fileBuilder.addFileComment(header)
        }

        return fileBuilder
            .addType(classBuilder.build())
            .build()
    }

    /**
     * Map SQL type to Korma column type function and Kotlin property type.
     */
    private fun mapSqlTypeToKorma(column: ColumnMetadata): KormaColumnType {
        // Check for custom type mappings first
        config.customTypeMappings[column.typeName.uppercase()]?.let { mapping ->
            val kotlinType = ClassName.bestGuess(mapping.kotlinType)
            return KormaColumnType(
                functionCall = mapping.columnTypeFunction ?: "column<${mapping.kotlinType}>(\"${column.name}\")",
                propertyType = ClassName("com.physics91.korma.schema", "Column")
                    .parameterizedBy(kotlinType)
            )
        }

        return when (column.sqlType) {
            // String types
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR,
            Types.NVARCHAR, Types.LONGNVARCHAR -> {
                val size = if (column.size > 0) column.size else 255
                KormaColumnType(
                    functionCall = "varchar(\"${column.name}\", $size)",
                    propertyType = columnType(String::class)
                )
            }
            Types.CLOB, Types.NCLOB -> KormaColumnType(
                functionCall = "text(\"${column.name}\")",
                propertyType = columnType(String::class)
            )

            // Integer types
            Types.BIT, Types.BOOLEAN -> KormaColumnType(
                functionCall = "boolean(\"${column.name}\")",
                propertyType = columnType(Boolean::class)
            )
            Types.TINYINT, Types.SMALLINT -> KormaColumnType(
                functionCall = "integer(\"${column.name}\")",
                propertyType = columnType(Int::class)
            )
            Types.INTEGER -> KormaColumnType(
                functionCall = "integer(\"${column.name}\")",
                propertyType = columnType(Int::class)
            )
            Types.BIGINT -> KormaColumnType(
                functionCall = "long(\"${column.name}\")",
                propertyType = columnType(Long::class)
            )

            // Floating point types
            Types.REAL, Types.FLOAT -> KormaColumnType(
                functionCall = "float(\"${column.name}\")",
                propertyType = columnType(Float::class)
            )
            Types.DOUBLE -> KormaColumnType(
                functionCall = "double(\"${column.name}\")",
                propertyType = columnType(Double::class)
            )
            Types.NUMERIC, Types.DECIMAL -> {
                val precision = if (column.size > 0) column.size else 10
                val scale = if (column.decimalDigits > 0) column.decimalDigits else 2
                KormaColumnType(
                    functionCall = "decimal(\"${column.name}\", $precision, $scale)",
                    propertyType = columnType(java.math.BigDecimal::class)
                )
            }

            // Date/Time types
            Types.DATE -> KormaColumnType(
                functionCall = "date(\"${column.name}\")",
                propertyType = ClassName("com.physics91.korma.schema", "Column")
                    .parameterizedBy(ClassName("kotlinx.datetime", "LocalDate"))
            )
            Types.TIME, Types.TIME_WITH_TIMEZONE -> KormaColumnType(
                functionCall = "time(\"${column.name}\")",
                propertyType = ClassName("com.physics91.korma.schema", "Column")
                    .parameterizedBy(ClassName("kotlinx.datetime", "LocalTime"))
            )
            Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> KormaColumnType(
                functionCall = "timestamp(\"${column.name}\")",
                propertyType = ClassName("com.physics91.korma.schema", "Column")
                    .parameterizedBy(ClassName("kotlinx.datetime", "Instant"))
            )

            // Binary types
            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> KormaColumnType(
                functionCall = "blob(\"${column.name}\")",
                propertyType = columnType(ByteArray::class)
            )

            // UUID type (vendor-specific but common)
            Types.OTHER -> {
                if (column.typeName.equals("UUID", ignoreCase = true)) {
                    KormaColumnType(
                        functionCall = "uuid(\"${column.name}\")",
                        propertyType = columnType(java.util.UUID::class)
                    )
                } else {
                    // Default to varchar for unknown types
                    KormaColumnType(
                        functionCall = "varchar(\"${column.name}\", 255)",
                        propertyType = columnType(String::class)
                    )
                }
            }

            // Default fallback
            else -> KormaColumnType(
                functionCall = "varchar(\"${column.name}\", 255)",
                propertyType = columnType(String::class)
            )
        }
    }

    /**
     * Map SQL type to Kotlin type for entity classes.
     */
    private fun mapSqlTypeToKotlin(column: ColumnMetadata): TypeName {
        val baseType = when (column.sqlType) {
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR,
            Types.NVARCHAR, Types.LONGNVARCHAR, Types.CLOB, Types.NCLOB -> String::class.asTypeName()

            Types.BIT, Types.BOOLEAN -> Boolean::class.asTypeName()

            Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Int::class.asTypeName()

            Types.BIGINT -> Long::class.asTypeName()

            Types.REAL, Types.FLOAT -> Float::class.asTypeName()

            Types.DOUBLE -> Double::class.asTypeName()

            Types.NUMERIC, Types.DECIMAL -> ClassName("java.math", "BigDecimal")

            Types.DATE -> ClassName("kotlinx.datetime", "LocalDate")

            Types.TIME, Types.TIME_WITH_TIMEZONE -> ClassName("kotlinx.datetime", "LocalTime")

            Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> ClassName("kotlinx.datetime", "Instant")

            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> ByteArray::class.asTypeName()

            Types.OTHER -> {
                if (column.typeName.equals("UUID", ignoreCase = true)) {
                    ClassName("java.util", "UUID")
                } else {
                    String::class.asTypeName()
                }
            }

            else -> String::class.asTypeName()
        }

        return if (column.nullable && config.useNullableTypes) {
            baseType.copy(nullable = true)
        } else {
            baseType
        }
    }

    private fun columnType(klass: kotlin.reflect.KClass<*>): TypeName {
        return ClassName("com.physics91.korma.schema", "Column")
            .parameterizedBy(klass.asTypeName())
    }

    private data class KormaColumnType(
        val functionCall: String,
        val propertyType: TypeName
    )
}
