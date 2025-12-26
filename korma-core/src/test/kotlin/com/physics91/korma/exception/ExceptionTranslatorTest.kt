package com.physics91.korma.exception

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.sql.SQLTimeoutException

class ExceptionTranslatorTest {

    @Nested
    inner class SqlExceptionTranslationTests {

        @Test
        fun `should translate SQLTimeoutException to QueryTimeoutException`() {
            val sqlException = SQLTimeoutException("Query timed out")

            val result = ExceptionTranslator.translate(sqlException, "SELECT * FROM users")

            result.shouldBeInstanceOf<QueryTimeoutException>()
            result.message shouldContain "timed out"
            (result as QueryTimeoutException).sql shouldBe "SELECT * FROM users"
        }

        @Test
        fun `should translate connection error 08001 to ConnectionAcquisitionException`() {
            val sqlException = SQLException("Connection refused", "08001")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<ConnectionAcquisitionException>()
            result.message shouldContain "establish connection"
        }

        @Test
        fun `should translate connection error 08003 to ConnectionLostException`() {
            val sqlException = SQLException("Connection closed", "08003")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<ConnectionLostException>()
            result.message shouldContain "does not exist"
        }

        @Test
        fun `should translate connection error 08006 to ConnectionLostException`() {
            val sqlException = SQLException("Connection failure", "08006")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<ConnectionLostException>()
            result.message shouldContain "failure"
        }

        @Test
        fun `should translate unique constraint violation 23505`() {
            val sqlException = SQLException("Duplicate key", "23505")

            val result = ExceptionTranslator.translate(
                sqlException,
                "INSERT INTO users (email) VALUES (?)",
                listOf("test@example.com")
            )

            result.shouldBeInstanceOf<QueryExecutionException>()
            result.message shouldContain "Unique constraint violation"
            (result as QueryExecutionException).sql shouldBe "INSERT INTO users (email) VALUES (?)"
            result.params shouldBe listOf("test@example.com")
        }

        @Test
        fun `should translate foreign key violation 23503`() {
            val sqlException = SQLException("Foreign key constraint", "23503")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<QueryExecutionException>()
            result.message shouldContain "Foreign key violation"
        }

        @Test
        fun `should translate not null violation 23502`() {
            val sqlException = SQLException("Not null constraint", "23502")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<QueryExecutionException>()
            result.message shouldContain "Not null violation"
        }

        @Test
        fun `should translate syntax error 42xxx to QueryBuildException`() {
            val sqlException = SQLException("Syntax error", "42601")

            val result = ExceptionTranslator.translate(sqlException, "SELCT * FROM users")

            result.shouldBeInstanceOf<QueryBuildException>()
            result.message shouldContain "SQL syntax"
            (result as QueryBuildException).sql shouldBe "SELCT * FROM users"
        }

        @Test
        fun `should translate transaction rollback 40001 to DeadlockException`() {
            val sqlException = SQLException("Serialization failure", "40001")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<DeadlockException>()
            result.message shouldContain "deadlock"
        }

        @Test
        fun `should translate PostgreSQL deadlock 40P01 to DeadlockException`() {
            val sqlException = SQLException("deadlock detected", "40P01")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<DeadlockException>()
        }

        @Test
        fun `should translate MySQL deadlock error code 1213`() {
            val sqlException = object : SQLException("Deadlock found") {
                override fun getErrorCode(): Int = 1213
            }

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<DeadlockException>()
        }

        @Test
        fun `should translate PostgreSQL lock timeout 55P03`() {
            val sqlException = SQLException("Lock not available", "55P03")

            val result = ExceptionTranslator.translate(sqlException)

            result.shouldBeInstanceOf<QueryTimeoutException>()
            result.message shouldContain "Lock wait timeout"
        }

        @Test
        fun `should translate unknown SQLException to QueryExecutionException`() {
            val sqlException = SQLException("Unknown error", "99999", 12345)

            val result = ExceptionTranslator.translate(sqlException, "SELECT 1")

            result.shouldBeInstanceOf<QueryExecutionException>()
            (result as QueryExecutionException).sqlState shouldBe "99999"
            result.vendorCode shouldBe 12345
        }
    }

    @Nested
    inner class GenericExceptionTranslationTests {

        @Test
        fun `should pass through KormaException unchanged`() {
            val kormaException = QueryBuildException("Original error")

            val result = ExceptionTranslator.translate(kormaException, "SELECT 1")

            result shouldBe kormaException
        }

        @Test
        fun `should wrap generic exception in QueryExecutionException`() {
            val genericException = RuntimeException("Something went wrong")

            val result = ExceptionTranslator.translate(
                genericException,
                "SELECT * FROM users",
                listOf(1L),
                "fetch operation"
            )

            result.shouldBeInstanceOf<QueryExecutionException>()
            result.message shouldContain "fetch operation"
            result.cause shouldBe genericException
        }
    }

    @Nested
    inner class TranslateExceptionsFunctionTests {

        @Test
        fun `translateExceptions should return value on success`() {
            val result = translateExceptions(sql = "SELECT 1") {
                42
            }

            result shouldBe 42
        }

        @Test
        fun `translateExceptions should translate SQLException`() {
            var caught: KormaException? = null

            try {
                translateExceptions(sql = "SELECT * FROM users", context = "query") {
                    throw SQLException("Error", "42601")
                }
            } catch (e: KormaException) {
                caught = e
            }

            caught.shouldBeInstanceOf<QueryBuildException>()
        }

        @Test
        fun `translateExceptions should pass through KormaException`() {
            val original = DeadlockException("Deadlock!")
            var caught: KormaException? = null

            try {
                translateExceptions {
                    throw original
                }
            } catch (e: KormaException) {
                caught = e
            }

            caught shouldBe original
        }

        @Test
        fun `translateExceptions should wrap generic exception`() {
            var caught: KormaException? = null

            try {
                translateExceptions(context = "custom operation") {
                    throw IllegalStateException("Bad state")
                }
            } catch (e: KormaException) {
                caught = e
            }

            caught.shouldBeInstanceOf<QueryExecutionException>()
            caught?.message shouldContain "custom operation"
        }
    }
}
