package com.physics91.korma.extensions

import com.physics91.korma.dsl.SelectBuilder
import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import java.math.BigDecimal
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime

// Test tables
object ExtTestUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val age = integer("age")
    val balance = decimal("balance", 10, 2)
    val score = double("score")
    val active = boolean("active")
    val createdAt = timestamp("created_at")
    val birthDate = date("birth_date")
    val lastLoginAt = datetime("last_login_at")
}

class ExtensionsTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    context("String Extensions") {

        test("contains should generate LIKE with wildcards") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.name.contains("john") }
                .build(dialect)

            query.sql shouldContain "LIKE"
            query.params shouldBe listOf("%john%")
        }

        test("startsWith should generate LIKE with suffix wildcard") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.email.startsWith("admin") }
                .build(dialect)

            query.sql shouldContain "LIKE"
            query.params shouldBe listOf("admin%")
        }

        test("endsWith should generate LIKE with prefix wildcard") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.email.endsWith("@example.com") }
                .build(dialect)

            query.sql shouldContain "LIKE"
            query.params shouldBe listOf("%@example.com")
        }

        test("containsIgnoreCase should generate case-insensitive LIKE") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.name.containsIgnoreCase("JOHN") }
                .build(dialect)

            query.sql shouldContain "LIKE"
        }

        test("notContains should generate NOT LIKE") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.name.notContains("spam") }
                .build(dialect)

            query.sql shouldContain "NOT LIKE"
        }

        test("isEmpty should compare with empty string") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.name.isEmpty() }
                .build(dialect)

            query.sql shouldContain "= ?"
            query.params shouldBe listOf("")
        }

        test("escapeLikePattern should escape special characters") {
            "50%".escapeLikePattern() shouldBe "50\\%"
            "test_data".escapeLikePattern() shouldBe "test\\_data"
            "100%_done".escapeLikePattern() shouldBe "100\\%\\_done"
        }

        test("containsSafe should escape special characters") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.name.containsSafe("50%") }
                .build(dialect)

            query.params shouldBe listOf("%50\\%%")
        }
    }

    context("Numeric Extensions - Int") {

        test("isPositive should generate > 0") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.age.isPositive() }
                .build(dialect)

            query.sql shouldContain "> ?"
            query.params shouldBe listOf(0)
        }

        test("isNegative should generate < 0") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.age.isNegative() }
                .build(dialect)

            query.sql shouldContain "< ?"
            query.params shouldBe listOf(0)
        }

        test("isZero should generate = 0") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.age.isZero() }
                .build(dialect)

            query.sql shouldContain "= ?"
            query.params shouldBe listOf(0)
        }

        test("isInRange should generate BETWEEN") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.age.isInRange(18..65) }
                .build(dialect)

            query.sql shouldContain "BETWEEN"
            query.params shouldBe listOf(18, 65)
        }

        test("isOneOf should generate IN clause") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.age.isOneOf(18, 21, 25, 30) }
                .build(dialect)

            query.sql shouldContain "IN"
        }
    }

    context("Numeric Extensions - BigDecimal") {

        test("isPositive for BigDecimal should generate > 0") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.balance.isPositive() }
                .build(dialect)

            query.sql shouldContain "> ?"
        }

        test("isInRange for BigDecimal should generate BETWEEN") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.balance.isInRange(BigDecimal("0"), BigDecimal("1000")) }
                .build(dialect)

            query.sql shouldContain "BETWEEN"
        }
    }

    context("Boolean Extensions") {

        test("isTrue should generate = true") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.active.isTrue() }
                .build(dialect)

            query.sql shouldContain "= ?"
            query.params shouldBe listOf(true)
        }

        test("isFalse should generate = false") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.active.isFalse() }
                .build(dialect)

            query.sql shouldContain "= ?"
            query.params shouldBe listOf(false)
        }
    }

    context("DateTime Extensions - LocalDate") {

        test("isToday should generate = current date") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.birthDate.isToday() }
                .build(dialect)

            query.sql shouldContain "= ?"
            query.params.size shouldBe 1
        }

        test("isThisMonth should generate BETWEEN for current month") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.birthDate.isThisMonth() }
                .build(dialect)

            query.sql shouldContain "BETWEEN"
            query.params.size shouldBe 2
        }

        test("isInLastDays should generate BETWEEN for last N days") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.birthDate.isInLastDays(7) }
                .build(dialect)

            query.sql shouldContain "BETWEEN"
            query.params.size shouldBe 2
        }

        test("isPast should generate < current date") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.birthDate.isPast() }
                .build(dialect)

            query.sql shouldContain "< ?"
        }

        test("isFuture should generate >= tomorrow") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.birthDate.isFuture() }
                .build(dialect)

            query.sql shouldContain ">= ?"
        }
    }

    context("DateTime Extensions - LocalDateTime") {

        test("isInLastHours should generate BETWEEN for last N hours") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.lastLoginAt.isInLastHours(24) }
                .build(dialect)

            query.sql shouldContain "BETWEEN"
            query.params.size shouldBe 2
        }

        test("isInLastMinutes should generate BETWEEN for last N minutes") {
            val query = SelectBuilder(ExtTestUsers)
                .selectAll()
                .where { ExtTestUsers.lastLoginAt.isInLastMinutes(30) }
                .build(dialect)

            query.sql shouldContain "BETWEEN"
            query.params.size shouldBe 2
        }
    }

    context("Value Classes - PageNumber") {

        test("should create valid page number") {
            val page = PageNumber(1)
            page.value shouldBe 1
        }

        test("should throw for invalid page number") {
            shouldThrow<IllegalArgumentException> {
                PageNumber(0)
            }
            shouldThrow<IllegalArgumentException> {
                PageNumber(-1)
            }
        }

        test("should calculate offset correctly") {
            val page = PageNumber(3)
            val size = PageSize(20)
            page.toOffset(size) shouldBe 40L
        }

        test("should support arithmetic operations") {
            val page = PageNumber(2)
            (page + 1).value shouldBe 3
            (page - 1).value shouldBe 1
        }
    }

    context("Value Classes - PageSize") {

        test("should create valid page size") {
            val size = PageSize(50)
            size.value shouldBe 50
        }

        test("should throw for invalid page size") {
            shouldThrow<IllegalArgumentException> {
                PageSize(0)
            }
            shouldThrow<IllegalArgumentException> {
                PageSize(1001)
            }
        }

        test("should provide preset sizes") {
            PageSize.DEFAULT.value shouldBe 20
            PageSize.SMALL.value shouldBe 10
            PageSize.MEDIUM.value shouldBe 50
            PageSize.LARGE.value shouldBe 100
        }
    }

    context("Value Classes - Limit and Offset") {

        test("should create valid limit") {
            val limit = Limit(100)
            limit.value shouldBe 100
        }

        test("should allow zero limit") {
            val limit = Limit(0)
            limit.value shouldBe 0
        }

        test("should throw for negative limit") {
            shouldThrow<IllegalArgumentException> {
                Limit(-1)
            }
        }

        test("should create valid offset") {
            val offset = Offset(50L)
            offset.value shouldBe 50L
        }

        test("should throw for negative offset") {
            shouldThrow<IllegalArgumentException> {
                Offset(-1L)
            }
        }
    }

    context("Value Classes - EntityId") {

        test("should create valid entity id") {
            val id = EntityId(123L)
            id.value shouldBe 123L
        }

        test("should throw for non-positive entity id") {
            shouldThrow<IllegalArgumentException> {
                EntityId(0L)
            }
            shouldThrow<IllegalArgumentException> {
                EntityId(-1L)
            }
        }
    }

    context("Value Classes - EntityUuid") {

        test("should create random UUID") {
            val uuid1 = EntityUuid.random()
            val uuid2 = EntityUuid.random()
            uuid1.value shouldBe uuid1.value  // Same instance
            uuid1.value.toString().length shouldBe 36  // UUID format
        }

        test("should parse UUID from string") {
            val uuidStr = "550e8400-e29b-41d4-a716-446655440000"
            val uuid = EntityUuid.fromString(uuidStr)
            uuid.toString() shouldBe uuidStr
        }
    }

    context("Value Classes - PageInfo and Page") {

        test("should calculate pagination info correctly") {
            val pageInfo = PageInfo(
                pageNumber = PageNumber(2),
                pageSize = PageSize(20),
                totalItems = 95
            )

            pageInfo.totalPages shouldBe 5
            pageInfo.hasNext shouldBe true
            pageInfo.hasPrevious shouldBe true
            pageInfo.isFirst shouldBe false
            pageInfo.isLast shouldBe false
        }

        test("should handle last page correctly") {
            val pageInfo = PageInfo(
                pageNumber = PageNumber(5),
                pageSize = PageSize(20),
                totalItems = 95
            )

            pageInfo.isLast shouldBe true
            pageInfo.hasNext shouldBe false
        }

        test("should handle first page correctly") {
            val pageInfo = PageInfo(
                pageNumber = PageNumber(1),
                pageSize = PageSize(20),
                totalItems = 95
            )

            pageInfo.isFirst shouldBe true
            pageInfo.hasPrevious shouldBe false
        }

        test("Page should map content") {
            val page = Page(
                content = listOf(1, 2, 3),
                pageInfo = PageInfo(PageNumber(1), PageSize(10), 3)
            )

            val mapped = page.map { it * 2 }
            mapped.content shouldBe listOf(2, 4, 6)
        }

        test("Page.empty should create empty page") {
            val empty = Page.empty<String>()
            empty.isEmpty shouldBe true
            empty.content shouldBe emptyList()
        }
    }

    context("Identifier Value Classes") {

        test("TableName should validate non-blank") {
            shouldThrow<IllegalArgumentException> {
                TableName("")
            }
            shouldThrow<IllegalArgumentException> {
                TableName("   ")
            }
        }

        test("TableName should accept valid name") {
            val name = TableName("users")
            name.value shouldBe "users"
        }

        test("ColumnName should validate non-blank") {
            shouldThrow<IllegalArgumentException> {
                ColumnName("")
            }
        }

        test("SchemaName should provide presets") {
            SchemaName.PUBLIC.value shouldBe "public"
            SchemaName.DBO.value shouldBe "dbo"
        }
    }
})
