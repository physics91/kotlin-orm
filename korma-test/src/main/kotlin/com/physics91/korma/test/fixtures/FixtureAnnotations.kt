package com.physics91.korma.test.fixtures

import kotlin.reflect.KClass

/**
 * Annotation to specify fixtures that should be loaded before tests.
 *
 * Fixtures are loaded in the order specified and cleaned up after tests
 * based on the cleanup mode.
 *
 * Usage:
 * ```kotlin
 * @WithFixtures([UserFixture::class, PostFixture::class])
 * class BlogServiceTest {
 *     @Test
 *     fun `should list user posts`() {
 *         // Fixtures are automatically loaded
 *     }
 * }
 * ```
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithFixtures(
    val value: Array<KClass<out Fixture>>,
    val cleanupMode: CleanupMode = CleanupMode.AFTER_EACH
)

/**
 * Cleanup mode for test fixtures.
 */
enum class CleanupMode {
    /**
     * Clean up fixtures after each test method.
     */
    AFTER_EACH,

    /**
     * Clean up fixtures only after all tests in the class.
     */
    AFTER_ALL,

    /**
     * Don't clean up fixtures automatically (manual cleanup required).
     */
    NONE
}

/**
 * Base interface for test fixtures.
 *
 * Fixtures define setup and teardown logic for test data.
 */
interface Fixture {
    /**
     * Sets up the fixture data.
     * Called before tests that use this fixture.
     */
    suspend fun setup(context: FixtureContext)

    /**
     * Tears down the fixture data.
     * Called after tests based on the cleanup mode.
     */
    suspend fun teardown(context: FixtureContext)

    /**
     * Optional: Returns the order in which this fixture should be loaded.
     * Lower values are loaded first. Default is 0.
     */
    fun order(): Int = 0
}

/**
 * Context provided to fixtures during setup and teardown.
 */
interface FixtureContext {
    /**
     * Executes a SQL statement.
     */
    suspend fun execute(sql: String)

    /**
     * Executes a SQL query and returns the results.
     */
    suspend fun <T> query(sql: String, mapper: (Map<String, Any?>) -> T): List<T>

    /**
     * Stores a value that can be retrieved by other fixtures or tests.
     */
    fun <T : Any> store(key: String, value: T)

    /**
     * Retrieves a stored value.
     */
    fun <T : Any> get(key: String, type: KClass<T>): T?

    /**
     * Retrieves a stored value with inline reified type.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> get(key: String): T? = get(key, Any::class) as? T
}

/**
 * DSL for creating fixtures inline.
 *
 * Usage:
 * ```kotlin
 * val userFixture = fixture {
 *     setup {
 *         execute("INSERT INTO users (id, name) VALUES (1, 'Test User')")
 *         store("userId", 1L)
 *     }
 *     teardown {
 *         execute("DELETE FROM users WHERE id = 1")
 *     }
 * }
 * ```
 */
fun fixture(block: FixtureBuilder.() -> Unit): Fixture {
    val builder = FixtureBuilder()
    builder.block()
    return builder.build()
}

class FixtureBuilder {
    private var setupBlock: (suspend FixtureContext.() -> Unit)? = null
    private var teardownBlock: (suspend FixtureContext.() -> Unit)? = null
    private var fixtureOrder: Int = 0

    fun setup(block: suspend FixtureContext.() -> Unit) {
        setupBlock = block
    }

    fun teardown(block: suspend FixtureContext.() -> Unit) {
        teardownBlock = block
    }

    fun order(value: Int) {
        fixtureOrder = value
    }

    fun build(): Fixture = object : Fixture {
        override suspend fun setup(context: FixtureContext) {
            setupBlock?.invoke(context)
        }

        override suspend fun teardown(context: FixtureContext) {
            teardownBlock?.invoke(context)
        }

        override fun order(): Int = fixtureOrder
    }
}
