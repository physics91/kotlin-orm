package com.physics91.korma.fixtures

import com.physics91.korma.schema.LongIdTable
import com.physics91.korma.schema.Table

/**
 * Common test tables used across all test modules.
 * Provides consistent test data structures for DRY testing.
 */

/**
 * Users table for testing basic CRUD operations.
 */
object TestUsers : LongIdTable("test_users") {
    val name = varchar("name", 100)
    val email = varchar("email", 255)
    val age = integer("age")
    val active = boolean("active")
}

/**
 * Posts table for testing relationships (OneToMany with Users).
 */
object TestPosts : LongIdTable("test_posts") {
    val authorId = long("author_id")
    val title = varchar("title", 200)
    val content = text("content")
    val published = boolean("published")
}

/**
 * Tags table for testing many-to-many relationships.
 */
object TestTags : LongIdTable("test_tags") {
    val name = varchar("name", 50)
}

/**
 * Post-Tags junction table for testing composite keys.
 */
object TestPostTags : Table("test_post_tags") {
    val postId = long("post_id")
    val tagId = long("tag_id")

    override val compositeKey = primaryKey(postId, tagId)
}

/**
 * Comments table for testing nested relationships.
 */
object TestComments : LongIdTable("test_comments") {
    val postId = long("post_id")
    val authorId = long("author_id")
    val content = text("content")
}

/**
 * Profiles table for testing OneToOne relationships.
 */
object TestProfiles : LongIdTable("test_profiles") {
    val userId = long("user_id")
    val bio = text("bio")
    val avatarUrl = varchar("avatar_url", 500).nullable()
}
