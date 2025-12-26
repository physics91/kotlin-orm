package com.physics91.korma.gradle

import com.physics91.korma.codegen.KormaCodegen
import com.physics91.korma.codegen.config.CodegenConfig
import com.physics91.korma.codegen.config.DatabaseConfig
import org.gradle.api.DefaultTask
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction

/**
 * Gradle task to preview generated Korma table definitions without writing files.
 */
abstract class PreviewKormaTablesTask : DefaultTask() {

    init {
        group = "korma"
        description = "Preview generated Korma table definitions without writing files"
    }

    @get:Input
    abstract val packageName: Property<String>

    @get:Input
    abstract val databaseUrl: Property<String>

    @get:Input
    @get:Optional
    abstract val databaseUsername: Property<String>

    @get:Input
    @get:Optional
    abstract val databasePassword: Property<String>

    @get:Input
    @get:Optional
    abstract val driverClassName: Property<String>

    @get:Input
    @get:Optional
    abstract val databaseSchema: Property<String>

    @get:Input
    abstract val generateEntities: Property<Boolean>

    @get:Input
    abstract val generateKdoc: Property<Boolean>

    @get:Input
    abstract val useNullableTypes: Property<Boolean>

    @get:Input
    @get:Optional
    abstract val fileHeader: Property<String>

    @get:Input
    abstract val includePatterns: ListProperty<String>

    @get:Input
    abstract val excludePatterns: ListProperty<String>

    @TaskAction
    fun preview() {
        val dbConfig = DatabaseConfig(
            url = databaseUrl.get(),
            username = databaseUsername.orNull,
            password = databasePassword.orNull,
            driverClassName = driverClassName.orNull
        )

        val config = CodegenConfig(
            packageName = packageName.get(),
            databaseConfig = dbConfig,
            schema = databaseSchema.orNull,
            generateEntities = generateEntities.get(),
            generateKdoc = generateKdoc.get(),
            useNullableTypes = useNullableTypes.get(),
            fileHeader = fileHeader.orNull,
            includePatterns = includePatterns.get().map { Regex(it) },
            excludePatterns = excludePatterns.get().map { Regex(it) }
        )

        val preview = KormaCodegen.preview(config)

        if (preview.isEmpty()) {
            logger.warn("No tables found matching the configuration")
        } else {
            preview.forEach { (fileName, code) ->
                println("\n// ========== $fileName ==========")
                println(code)
            }
        }
    }
}
