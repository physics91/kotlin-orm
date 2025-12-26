package com.physics91.korma.codegen.config

/**
 * Naming strategy for converting database names to Kotlin names.
 *
 * Provides conversion methods for:
 * - Table names → Kotlin class/object names
 * - Column names → Kotlin property names
 * - Foreign key names → Kotlin reference names
 */
interface NamingStrategy {
    /**
     * Convert a database table name to a Kotlin Table object name.
     *
     * @param tableName The database table name (e.g., "user_accounts")
     * @return The Kotlin object name (e.g., "UserAccounts")
     */
    fun tableToClassName(tableName: String): String

    /**
     * Convert a database table name to an entity class name.
     *
     * @param tableName The database table name (e.g., "user_accounts")
     * @return The Kotlin entity class name (e.g., "UserAccount")
     */
    fun tableToEntityName(tableName: String): String

    /**
     * Convert a database column name to a Kotlin property name.
     *
     * @param columnName The database column name (e.g., "first_name")
     * @return The Kotlin property name (e.g., "firstName")
     */
    fun columnToPropertyName(columnName: String): String

    /**
     * Convert a foreign key constraint to a reference property name.
     *
     * @param constraintName The foreign key constraint name
     * @param referencedTable The referenced table name
     * @return The Kotlin property name for the reference
     */
    fun foreignKeyToPropertyName(constraintName: String, referencedTable: String): String

    /**
     * Default naming strategy using common conventions.
     */
    object Default : NamingStrategy {
        override fun tableToClassName(tableName: String): String {
            return toPascalCase(tableName)
        }

        override fun tableToEntityName(tableName: String): String {
            val className = toPascalCase(tableName)
            // Singularize entity names
            return singularize(className)
        }

        private fun singularize(name: String): String {
            return when {
                name.endsWith("ies") && name.length > 3 -> name.dropLast(3) + "y"
                name.endsWith("ses") && name.length > 3 -> name.dropLast(2)
                name.endsWith("es") && name.length > 2 -> name.dropLast(2)
                name.endsWith("s") && name.length > 1 -> name.dropLast(1)
                else -> name
            }
        }

        override fun columnToPropertyName(columnName: String): String {
            return toCamelCase(columnName)
        }

        override fun foreignKeyToPropertyName(constraintName: String, referencedTable: String): String {
            // Use referenced table name for the property
            return tableToEntityName(referencedTable).replaceFirstChar { it.lowercase() }
        }

        private fun toPascalCase(name: String): String {
            return name.split("_", "-", " ")
                .filter { it.isNotEmpty() }
                .joinToString("") { part ->
                    part.lowercase().replaceFirstChar { it.uppercase() }
                }
        }

        private fun toCamelCase(name: String): String {
            val parts = name.split("_", "-", " ").filter { it.isNotEmpty() }
            if (parts.isEmpty()) return name

            return parts.first().lowercase() +
                    parts.drop(1).joinToString("") { part ->
                        part.lowercase().replaceFirstChar { it.uppercase() }
                    }
        }
    }

    /**
     * Snake case naming strategy - keeps underscores in property names.
     */
    object SnakeCase : NamingStrategy {
        override fun tableToClassName(tableName: String): String {
            return Default.tableToClassName(tableName)
        }

        override fun tableToEntityName(tableName: String): String {
            return Default.tableToEntityName(tableName)
        }

        override fun columnToPropertyName(columnName: String): String {
            // Keep snake_case as-is (use backticks if needed)
            return if (columnName.contains(Regex("[^a-zA-Z0-9_]"))) {
                "`$columnName`"
            } else {
                columnName.lowercase()
            }
        }

        override fun foreignKeyToPropertyName(constraintName: String, referencedTable: String): String {
            return Default.foreignKeyToPropertyName(constraintName, referencedTable)
        }
    }

    /**
     * Prefix-based naming strategy that removes common prefixes.
     */
    class PrefixRemover(
        private val prefixesToRemove: List<String>,
        private val delegate: NamingStrategy = Default
    ) : NamingStrategy {
        override fun tableToClassName(tableName: String): String {
            val cleanedName = removePrefix(tableName)
            return delegate.tableToClassName(cleanedName)
        }

        override fun tableToEntityName(tableName: String): String {
            val cleanedName = removePrefix(tableName)
            return delegate.tableToEntityName(cleanedName)
        }

        override fun columnToPropertyName(columnName: String): String {
            return delegate.columnToPropertyName(columnName)
        }

        override fun foreignKeyToPropertyName(constraintName: String, referencedTable: String): String {
            return delegate.foreignKeyToPropertyName(constraintName, referencedTable)
        }

        private fun removePrefix(name: String): String {
            for (prefix in prefixesToRemove) {
                if (name.lowercase().startsWith(prefix.lowercase())) {
                    return name.substring(prefix.length)
                }
            }
            return name
        }
    }

    /**
     * Custom naming strategy using lambdas.
     */
    class Custom(
        private val tableToClass: (String) -> String = Default::tableToClassName,
        private val tableToEntity: (String) -> String = Default::tableToEntityName,
        private val columnToProperty: (String) -> String = Default::columnToPropertyName,
        private val fkToProperty: (String, String) -> String = Default::foreignKeyToPropertyName
    ) : NamingStrategy {
        override fun tableToClassName(tableName: String): String = tableToClass(tableName)
        override fun tableToEntityName(tableName: String): String = tableToEntity(tableName)
        override fun columnToPropertyName(columnName: String): String = columnToProperty(columnName)
        override fun foreignKeyToPropertyName(constraintName: String, referencedTable: String): String =
            fkToProperty(constraintName, referencedTable)
    }

    companion object {
        /**
         * Create a naming strategy that removes common table prefixes.
         *
         * @param prefixes Prefixes to remove (e.g., "tbl_", "t_")
         */
        fun withPrefixRemoval(vararg prefixes: String): NamingStrategy {
            return PrefixRemover(prefixes.toList())
        }

        /**
         * Create a custom naming strategy.
         */
        fun custom(
            tableToClass: (String) -> String = Default::tableToClassName,
            tableToEntity: (String) -> String = Default::tableToEntityName,
            columnToProperty: (String) -> String = Default::columnToPropertyName,
            fkToProperty: (String, String) -> String = Default::foreignKeyToPropertyName
        ): NamingStrategy = Custom(tableToClass, tableToEntity, columnToProperty, fkToProperty)
    }
}
