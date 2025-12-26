package com.physics91.korma.dialect.h2

import com.physics91.korma.schema.LongIdTable
import com.physics91.korma.schema.Table
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

// Define tables at file level
object H2TestTable : Table("test") {
    val intCol = integer("int_col")
    val longCol = long("long_col")
    val boolCol = boolean("bool_col")
    val varcharCol = varchar("varchar_col", 100)
    val textCol = text("text_col")
    val decimalCol = decimal("decimal_col", 10, 2)
    val timestampCol = timestamp("timestamp_col")
    val dateCol = date("date_col")
    val uuidCol = uuid("uuid_col")
}

object H2TestAutoIncTable : Table("test_auto") {
    val intId = integer("int_id").autoIncrement()
    val longId = long("long_id").autoIncrement()
}

object H2TestUsers : LongIdTable("users") {
    val name = varchar("name", 100).notNull()
    val email = varchar("email", 255).unique().notNull()
    val age = integer("age").nullable()
}

object H2ConflictUsers : Table("users") {
    val id = long("id").primaryKey()
    val email = varchar("email", 255).unique()
    val name = varchar("name", 100)
}

object DropTestTable : Table("test_table")

class H2DialectTest : FunSpec({

    test("should have correct dialect name") {
        H2Dialect.name shouldBe "H2"
    }

    test("should support correct features") {
        H2Dialect.supportsReturning shouldBe false
        H2Dialect.supportsOnConflict shouldBe true
        H2Dialect.supportsILike shouldBe true
        H2Dialect.supportsCTE shouldBe true
        H2Dialect.supportsWindowFunctions shouldBe true
        H2Dialect.supportsLimitOffset shouldBe true
        H2Dialect.supportsBooleanType shouldBe true
    }

    test("should quote identifiers with double quotes") {
        H2Dialect.quoteIdentifier("users") shouldBe "\"users\""
        H2Dialect.quoteIdentifier("column_name") shouldBe "\"column_name\""
    }

    test("should generate correct SQL types") {
        H2Dialect.sqlTypeName(H2TestTable.intCol.type) shouldBe "INT"
        H2Dialect.sqlTypeName(H2TestTable.longCol.type) shouldBe "BIGINT"
        H2Dialect.sqlTypeName(H2TestTable.boolCol.type) shouldBe "BOOLEAN"
        H2Dialect.sqlTypeName(H2TestTable.varcharCol.type) shouldBe "VARCHAR(100)"
        H2Dialect.sqlTypeName(H2TestTable.textCol.type) shouldBe "CLOB"
        H2Dialect.sqlTypeName(H2TestTable.decimalCol.type) shouldBe "DECIMAL(10, 2)"
        H2Dialect.sqlTypeName(H2TestTable.timestampCol.type) shouldBe "TIMESTAMP WITH TIME ZONE"
        H2Dialect.sqlTypeName(H2TestTable.dateCol.type) shouldBe "DATE"
        H2Dialect.sqlTypeName(H2TestTable.uuidCol.type) shouldBe "UUID"
    }

    test("should generate auto increment types") {
        H2Dialect.autoIncrementType(H2TestAutoIncTable.intId.type) shouldBe "INT AUTO_INCREMENT"
        H2Dialect.autoIncrementType(H2TestAutoIncTable.longId.type) shouldBe "BIGINT AUTO_INCREMENT"
    }

    test("should generate CREATE TABLE statement") {
        val sql = H2Dialect.createTableStatement(H2TestUsers, ifNotExists = true)

        sql shouldContain "CREATE TABLE IF NOT EXISTS \"users\""
        sql shouldContain "\"id\" BIGINT AUTO_INCREMENT PRIMARY KEY"
        sql shouldContain "\"name\" VARCHAR(100) NOT NULL"
        sql shouldContain "\"email\" VARCHAR(255) NOT NULL UNIQUE"
        sql shouldContain "\"age\" INT"
    }

    test("should generate DROP TABLE statement") {
        H2Dialect.dropTableStatement(DropTestTable, ifExists = true) shouldBe "DROP TABLE IF EXISTS \"test_table\""
        H2Dialect.dropTableStatement(DropTestTable, ifExists = false) shouldBe "DROP TABLE \"test_table\""
    }

    test("should generate LIMIT OFFSET clause") {
        H2Dialect.limitOffsetClause(10, null) shouldBe "LIMIT 10"
        H2Dialect.limitOffsetClause(10, 20) shouldBe "LIMIT 10 OFFSET 20"
        H2Dialect.limitOffsetClause(null, 20) shouldBe "OFFSET 20"
        H2Dialect.limitOffsetClause(null, null) shouldBe ""
    }

    test("should generate ON CONFLICT clause") {
        val conflictClause = H2Dialect.onConflictClause(
            listOf(H2ConflictUsers.email),
            listOf(H2ConflictUsers.name)
        )

        conflictClause shouldContain "ON DUPLICATE KEY UPDATE"
        conflictClause shouldContain "\"name\""
    }

    test("should generate RETURNING clause") {
        // H2 doesn't support RETURNING, so this should return empty string
        val clause = H2Dialect.returningClause(listOf(H2ConflictUsers.id, H2ConflictUsers.name))
        clause shouldBe ""
    }

    test("should generate current timestamp expression") {
        H2Dialect.currentTimestampExpression() shouldBe "CURRENT_TIMESTAMP"
        H2Dialect.currentDateExpression() shouldBe "CURRENT_DATE"
    }

    test("should generate in-memory URL") {
        H2Dialect.inMemoryUrl("test") shouldBe "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
    }

    test("should generate file URL") {
        H2Dialect.fileUrl("/data/mydb") shouldBe "jdbc:h2:file:/data/mydb"
    }
})
