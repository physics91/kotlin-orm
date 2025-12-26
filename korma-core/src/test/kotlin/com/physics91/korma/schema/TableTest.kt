package com.physics91.korma.schema

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

// Define tables at file level
object TestUsers : Table("users") {
    val id = long("id").primaryKey().autoIncrement()
    val name = varchar("name", 100).notNull()
    val email = varchar("email", 255).unique().notNull()
    val age = integer("age").nullable()
    val active = boolean("active").default("true")
}

object TestArticles : LongIdTable("articles") {
    val title = varchar("title", 200)
    val content = text("content")
}

object TestCategories : IntIdTable("categories") {
    val name = varchar("name", 100)
}

object TestSessions : UUIDTable("sessions") {
    val userId = long("user_id")
    val token = varchar("token", 255)
}

object TestAllTypes : Table("all_types") {
    val intCol = integer("int_col")
    val longCol = long("long_col")
    val shortCol = short("short_col")
    val floatCol = float("float_col")
    val doubleCol = double("double_col")
    val boolCol = boolean("bool_col")
    val varcharCol = varchar("varchar_col", 100)
    val charCol = char("char_col", 10)
    val textCol = text("text_col")
    val decimalCol = decimal("decimal_col", 10, 2)
    val timestampCol = timestamp("timestamp_col")
    val dateCol = date("date_col")
    val timeCol = time("time_col")
    val binaryCol = binary("binary_col")
    val blobCol = blob("blob_col")
    val uuidCol = uuid("uuid_col")
}

object SimpleTable : Table("test_table")

object FkUsers : Table("fk_users") {
    val id = long("id").primaryKey().autoIncrement()
    val name = varchar("name", 100)
}

object FkPosts : Table("fk_posts") {
    val id = long("id").primaryKey().autoIncrement()
    val userId = long("user_id").references(FkUsers.id)
    val title = varchar("title", 200)
}

class TableTest : FunSpec({

    test("should create table with name") {
        SimpleTable.tableName shouldBe "test_table"
    }

    test("should create columns with different types") {
        TestUsers.columns shouldHaveSize 5
        TestUsers.primaryKey shouldHaveSize 1
        TestUsers.primaryKey.first().name shouldBe "id"

        TestUsers.id.isAutoIncrement shouldBe true
        TestUsers.id.isPrimaryKey shouldBe true

        TestUsers.name.isNotNull shouldBe true
        TestUsers.email.isUnique shouldBe true
        TestUsers.age.nullable shouldBe true
        TestUsers.active.defaultValue shouldBe "true"
    }

    test("should create foreign key reference") {
        FkPosts.userId.foreignKey shouldNotBe null
        FkPosts.userId.foreignKey?.targetColumn shouldBe FkUsers.id
    }

    test("should create LongIdTable with auto-increment id") {
        TestArticles.id.name shouldBe "id"
        TestArticles.id.isAutoIncrement shouldBe true
        TestArticles.id.isPrimaryKey shouldBe true
        TestArticles.columns shouldContain TestArticles.id
    }

    test("should create IntIdTable with auto-increment id") {
        TestCategories.id.name shouldBe "id"
        TestCategories.id.isAutoIncrement shouldBe true
        TestCategories.id.isPrimaryKey shouldBe true
    }

    test("should create UUIDTable with uuid id") {
        TestSessions.id.name shouldBe "id"
        TestSessions.id.isPrimaryKey shouldBe true
    }

    test("should support all column types") {
        TestAllTypes.columns shouldHaveSize 16
    }

    test("should get qualified column name") {
        TestUsers.id.qualifiedName shouldBe "users.id"
        TestUsers.name.qualifiedName shouldBe "users.name"
    }
})
