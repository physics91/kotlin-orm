package com.physics91.korma.codegen

import com.physics91.korma.codegen.config.NamingStrategy
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class NamingStrategyTest {

    @Test
    fun `Default strategy converts table name to PascalCase`() {
        val strategy = NamingStrategy.Default

        assertEquals("Users", strategy.tableToClassName("users"))
        assertEquals("UserAccounts", strategy.tableToClassName("user_accounts"))
        assertEquals("OrderItems", strategy.tableToClassName("order_items"))
        assertEquals("Api", strategy.tableToClassName("API"))
    }

    @Test
    fun `Default strategy converts table name to entity name`() {
        val strategy = NamingStrategy.Default

        assertEquals("User", strategy.tableToEntityName("users"))
        assertEquals("UserAccount", strategy.tableToEntityName("user_accounts"))
        assertEquals("OrderItem", strategy.tableToEntityName("order_items"))
        assertEquals("Category", strategy.tableToEntityName("categories"))
        assertEquals("Address", strategy.tableToEntityName("addresses"))
        assertEquals("Bus", strategy.tableToEntityName("buses"))
    }

    @Test
    fun `Default strategy converts column name to camelCase`() {
        val strategy = NamingStrategy.Default

        assertEquals("id", strategy.columnToPropertyName("id"))
        assertEquals("firstName", strategy.columnToPropertyName("first_name"))
        assertEquals("createdAt", strategy.columnToPropertyName("created_at"))
        assertEquals("userId", strategy.columnToPropertyName("user_id"))
    }

    @Test
    fun `Default strategy converts foreign key to property name`() {
        val strategy = NamingStrategy.Default

        assertEquals("user", strategy.foreignKeyToPropertyName("fk_orders_user", "users"))
        assertEquals("orderItem", strategy.foreignKeyToPropertyName("fk_invoice_item", "order_items"))
    }

    @Test
    fun `SnakeCase strategy preserves snake_case for columns`() {
        val strategy = NamingStrategy.SnakeCase

        assertEquals("first_name", strategy.columnToPropertyName("first_name"))
        assertEquals("created_at", strategy.columnToPropertyName("created_at"))
        assertEquals("id", strategy.columnToPropertyName("ID"))
    }

    @Test
    fun `SnakeCase strategy uses backticks for special characters`() {
        val strategy = NamingStrategy.SnakeCase

        assertEquals("`column-name`", strategy.columnToPropertyName("column-name"))
        assertEquals("`column.name`", strategy.columnToPropertyName("column.name"))
    }

    @Test
    fun `PrefixRemover removes specified prefixes`() {
        val strategy = NamingStrategy.withPrefixRemoval("tbl_", "t_")

        assertEquals("Users", strategy.tableToClassName("tbl_users"))
        assertEquals("Orders", strategy.tableToClassName("t_orders"))
        assertEquals("Categories", strategy.tableToClassName("categories"))
    }

    @Test
    fun `Custom strategy uses provided lambdas`() {
        val strategy = NamingStrategy.custom(
            tableToClass = { "Table$it" },
            tableToEntity = { "Entity$it" },
            columnToProperty = { "prop_$it" }
        )

        assertEquals("Tableusers", strategy.tableToClassName("users"))
        assertEquals("Entityorders", strategy.tableToEntityName("orders"))
        assertEquals("prop_first_name", strategy.columnToPropertyName("first_name"))
    }

    @Test
    fun `handles hyphenated names`() {
        val strategy = NamingStrategy.Default

        assertEquals("UserProfile", strategy.tableToClassName("user-profile"))
        assertEquals("firstName", strategy.columnToPropertyName("first-name"))
    }

    @Test
    fun `handles spaces in names`() {
        val strategy = NamingStrategy.Default

        assertEquals("UserProfile", strategy.tableToClassName("user profile"))
        assertEquals("firstName", strategy.columnToPropertyName("first name"))
    }

    @Test
    fun `handles empty parts`() {
        val strategy = NamingStrategy.Default

        assertEquals("UserProfile", strategy.tableToClassName("user__profile"))
        assertEquals("firstName", strategy.columnToPropertyName("first__name"))
    }
}
