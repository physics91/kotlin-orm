package com.physics91.korma.dsl

import com.physics91.korma.schema.ColumnType
import com.physics91.korma.schema.Table
import com.physics91.korma.sql.BaseSqlDialect
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain

// Test tables for dynamic query tests
object DynamicTestUsers : Table("users") {
    val id = long("id").primaryKey()
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val age = integer("age")
    val status = varchar("status", 50)
    val active = boolean("active")
}

// Sample filter DTO for testing
data class UserSearchFilter(
    val name: String? = null,
    val email: String? = null,
    val minAge: Int? = null,
    val maxAge: Int? = null,
    val status: String? = null,
    val active: Boolean? = null,
    val statuses: List<String>? = null
)

class DynamicQueryTest : FunSpec({

    val dialect = object : BaseSqlDialect() {
        override val name = "Test"
        override fun autoIncrementType(baseType: ColumnType<*>): String = "AUTO_INCREMENT"
    }

    context("dynamicWhere - basic conditions") {

        test("should add single condition when value is not null") {
            val searchName = "John"

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    searchName?.let { +DynamicTestUsers.name.like("%$it%") }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "LIKE"
        }

        test("should not add WHERE clause when all values are null") {
            val searchName: String? = null
            val minAge: Int? = null

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    searchName?.let { +DynamicTestUsers.name.like("%$it%") }
                    minAge?.let { +DynamicTestUsers.age.gte(it) }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("should combine multiple conditions with AND") {
            val searchName = "John"
            val minAge = 18

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    searchName?.let { +DynamicTestUsers.name.like("%$it%") }
                    minAge?.let { +DynamicTestUsers.age.gte(it) }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "AND"
            query.params.size shouldBe 2
        }

        test("should skip null values in mixed conditions") {
            val searchName = "John"
            val searchEmail: String? = null
            val minAge = 18

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    searchName?.let { +DynamicTestUsers.name.like("%$it%") }
                    searchEmail?.let { +DynamicTestUsers.email.like("%$it%") }
                    minAge?.let { +DynamicTestUsers.age.gte(it) }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "AND"
            query.sql shouldContain "\"name\""
            query.sql shouldContain "\"age\""
            query.sql shouldNotContain "\"email\""
        }
    }

    context("dynamicWhere - helper functions") {

        test("ifNotNull should add condition for non-null value") {
            val searchName = "John"

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotNull(searchName) { DynamicTestUsers.name.like("%$it%") }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "LIKE"
        }

        test("ifNotNull should skip null value") {
            val searchName: String? = null

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotNull(searchName) { DynamicTestUsers.name.like("%$it%") }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("ifTrue should add condition when true") {
            val includeInactive = true

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifTrue(includeInactive) { DynamicTestUsers.active eq false }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "\"active\""
        }

        test("ifTrue should skip condition when false") {
            val includeInactive = false

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifTrue(includeInactive) { DynamicTestUsers.active eq false }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("ifNotEmpty should add condition for non-empty collection") {
            val statuses = listOf("active", "pending")

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotEmpty(statuses) { DynamicTestUsers.status inList it }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "IN"
        }

        test("ifNotEmpty should skip null or empty collection") {
            val emptyStatuses = emptyList<String>()
            val nullStatuses: List<String>? = null

            val query1 = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotEmpty(emptyStatuses) { DynamicTestUsers.status inList it }
                }
                .build(dialect)

            val query2 = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotEmpty(nullStatuses) { DynamicTestUsers.status inList it }
                }
                .build(dialect)

            query1.sql shouldNotContain "WHERE"
            query2.sql shouldNotContain "WHERE"
        }

        test("ifNotBlank should add condition for non-blank string") {
            val searchTerm = "John"

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotBlank(searchTerm) { DynamicTestUsers.name.like("%$it%") }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
        }

        test("ifNotBlank should skip blank or null strings") {
            val blankTerm = "   "
            val nullTerm: String? = null

            val query1 = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotBlank(blankTerm) { DynamicTestUsers.name.like("%$it%") }
                }
                .build(dialect)

            val query2 = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    ifNotBlank(nullTerm) { DynamicTestUsers.name.like("%$it%") }
                }
                .build(dialect)

            query1.sql shouldNotContain "WHERE"
            query2.sql shouldNotContain "WHERE"
        }
    }

    context("dynamicWhere - OR grouping") {

        test("or block should combine conditions with OR") {
            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    or {
                        +DynamicTestUsers.status.eq("admin")
                        +DynamicTestUsers.status.eq("moderator")
                    }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "OR"
        }

        test("should mix AND and OR conditions") {
            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    +DynamicTestUsers.active.eq(true)
                    or {
                        +DynamicTestUsers.status.eq("admin")
                        +DynamicTestUsers.status.eq("moderator")
                    }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "AND"
            query.sql shouldContain "OR"
        }

        test("nested and block within or should work") {
            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    or {
                        and {
                            +DynamicTestUsers.status.eq("user")
                            +DynamicTestUsers.active.eq(true)
                        }
                        +DynamicTestUsers.status.eq("admin")
                    }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "OR"
        }
    }

    context("applyFilter - filter object pattern") {

        test("should apply all non-null filter fields") {
            val filter = UserSearchFilter(
                name = "John",
                minAge = 18,
                active = true
            )

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .applyFilter(filter) { f ->
                    f.name?.let { +DynamicTestUsers.name.like("%$it%") }
                    f.minAge?.let { +DynamicTestUsers.age.gte(it) }
                    f.active?.let { +DynamicTestUsers.active.eq(it) }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "\"name\""
            query.sql shouldContain "\"age\""
            query.sql shouldContain "\"active\""
        }

        test("should skip null filter fields") {
            val filter = UserSearchFilter(
                name = "John",
                email = null,
                active = true
            )

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .applyFilter(filter) { f ->
                    f.name?.let { +DynamicTestUsers.name.like("%$it%") }
                    f.email?.let { +DynamicTestUsers.email.like("%$it%") }
                    f.active?.let { +DynamicTestUsers.active.eq(it) }
                }
                .build(dialect)

            query.sql shouldContain "\"name\""
            query.sql shouldContain "\"active\""
            query.sql shouldNotContain "\"email\""
        }

        test("should not add WHERE when filter is empty") {
            val filter = UserSearchFilter()

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .applyFilter(filter) { f ->
                    f.name?.let { +DynamicTestUsers.name.like("%$it%") }
                    f.email?.let { +DynamicTestUsers.email.like("%$it%") }
                    f.active?.let { +DynamicTestUsers.active.eq(it) }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("should handle list filter fields") {
            val filter = UserSearchFilter(
                statuses = listOf("active", "pending")
            )

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .applyFilter(filter) { f ->
                    ifNotEmpty(f.statuses) { DynamicTestUsers.status inList it }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "IN"
        }
    }

    context("null-safe column extensions") {

        test("eqOrNull should return predicate for non-null value") {
            val status = "active"

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.status.eqOrNull(status)?.let { +it }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "\"status\""
        }

        test("eqOrNull should return null for null value") {
            val status: String? = null

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.status.eqOrNull(status)?.let { +it }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("containsOrNull should work for non-blank strings") {
            val searchTerm = "john"

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.name.containsOrNull(searchTerm)?.let { +it }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "LIKE"
        }

        test("containsOrNull should return null for blank strings") {
            val searchTerm = "   "

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.name.containsOrNull(searchTerm)?.let { +it }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("inListOrNull should work for non-empty collections") {
            val statuses = listOf("active", "pending")

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.status.inListOrNull(statuses)?.let { +it }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "IN"
        }

        test("inListOrNull should return null for empty collections") {
            val statuses = emptyList<String>()

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.status.inListOrNull(statuses)?.let { +it }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }

        test("betweenOrNull should work when both bounds are provided") {
            val minAge = 18
            val maxAge = 65

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.age.betweenOrNull(minAge, maxAge)?.let { +it }
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "BETWEEN"
        }

        test("betweenOrNull should return null when either bound is null") {
            val minAge = 18
            val maxAge: Int? = null

            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .dynamicWhere {
                    DynamicTestUsers.age.betweenOrNull(minAge, maxAge)?.let { +it }
                }
                .build(dialect)

            query.sql shouldNotContain "WHERE"
        }
    }

    context("integration with existing query methods") {

        test("should work with other SelectBuilder methods") {
            val searchName = "John"

            val query = SelectBuilder(DynamicTestUsers)
                .select(DynamicTestUsers.id, DynamicTestUsers.name)
                .dynamicWhere {
                    searchName?.let { +DynamicTestUsers.name.like("%$it%") }
                }
                .orderBy(DynamicTestUsers.name.asc())
                .limit(10)
                .build(dialect)

            query.sql shouldContain "SELECT"
            query.sql shouldContain "WHERE"
            query.sql shouldContain "ORDER BY"
            query.sql shouldContain "LIMIT"
        }

        test("dynamicAndWhere should add to existing WHERE") {
            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .where { DynamicTestUsers.active eq true }
                .dynamicAndWhere {
                    +DynamicTestUsers.status.eq("verified")
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "AND"
            query.sql shouldContain "\"active\""
            query.sql shouldContain "\"status\""
        }

        test("dynamicOrWhere should add to existing WHERE with OR") {
            val query = SelectBuilder(DynamicTestUsers)
                .selectAll()
                .where { DynamicTestUsers.status eq "admin" }
                .dynamicOrWhere {
                    +DynamicTestUsers.status.eq("moderator")
                }
                .build(dialect)

            query.sql shouldContain "WHERE"
            query.sql shouldContain "OR"
        }
    }
})
