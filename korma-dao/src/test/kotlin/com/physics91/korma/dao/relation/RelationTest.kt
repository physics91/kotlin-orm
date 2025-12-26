package com.physics91.korma.dao.relation

import com.physics91.korma.dao.entity.*
import com.physics91.korma.schema.Column
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/**
 * Tests for relationship definitions.
 */
class RelationTest {

    // ============== Test Entity Implementations ==============

    object Users : LongEntityTable<User>("users") {
        val name = varchar("name", 100)

        override fun createEntity() = User()
        override fun entityToMap(entity: User): Map<Column<*>, Any?> = mapOf(name to entity.name)
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: User) {
            entity.name = row[name] as String
        }
    }

    class User(id: Long = 0L) : LongEntity(id) {
        var name: String = ""
        override val entityTable get() = Users
    }

    object Profiles : LongEntityTable<Profile>("profiles") {
        val userId = long("user_id")
        val bio = varchar("bio", 500)

        override fun createEntity() = Profile()
        override fun entityToMap(entity: Profile): Map<Column<*>, Any?> = mapOf(
            userId to entity.userId,
            bio to entity.bio
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: Profile) {
            entity.userId = row[userId] as Long
            entity.bio = row[bio] as String
        }
    }

    class Profile(id: Long = 0L) : LongEntity(id) {
        var userId: Long = 0L
        var bio: String = ""
        override val entityTable get() = Profiles
    }

    object Posts : LongEntityTable<Post>("posts") {
        val authorId = long("author_id")
        val title = varchar("title", 200)

        override fun createEntity() = Post()
        override fun entityToMap(entity: Post): Map<Column<*>, Any?> = mapOf(
            authorId to entity.authorId,
            title to entity.title
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: Post) {
            entity.authorId = row[authorId] as Long
            entity.title = row[title] as String
        }
    }

    class Post(id: Long = 0L) : LongEntity(id) {
        var authorId: Long = 0L
        var title: String = ""
        override val entityTable get() = Posts
    }

    object Roles : LongEntityTable<Role>("roles") {
        val name = varchar("name", 50)

        override fun createEntity() = Role()
        override fun entityToMap(entity: Role): Map<Column<*>, Any?> = mapOf(name to entity.name)
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: Role) {
            entity.name = row[name] as String
        }
    }

    class Role(id: Long = 0L) : LongEntity(id) {
        var name: String = ""
        override val entityTable get() = Roles
    }

    object UserRoles : LongEntityTable<UserRole>("user_roles") {
        val userId = long("user_id")
        val roleId = long("role_id")

        override fun createEntity() = UserRole()
        override fun entityToMap(entity: UserRole): Map<Column<*>, Any?> = mapOf(
            userId to entity.userId,
            roleId to entity.roleId
        )
        override fun mapToEntity(row: Map<Column<*>, Any?>, entity: UserRole) {
            entity.userId = row[userId] as Long
            entity.roleId = row[roleId] as Long
        }
    }

    class UserRole(id: Long = 0L) : LongEntity(id) {
        var userId: Long = 0L
        var roleId: Long = 0L
        override val entityTable get() = UserRoles
    }

    // ============== OneToOne Tests ==============

    @Test
    fun `OneToOne relation is created correctly`() {
        val relation = OneToOne(
            sourceTable = Users,
            targetTable = Profiles,
            foreignKey = Profiles.userId
        )

        assertEquals(Users, relation.sourceTable)
        assertEquals(Profiles, relation.targetTable)
        assertEquals(Profiles.userId, relation.foreignKey)
        assertEquals(FetchType.LAZY, relation.fetchType)
        assertTrue(relation.cascadeTypes.isEmpty())
    }

    @Test
    fun `OneToOne with eager loading`() {
        val relation = OneToOne(
            sourceTable = Users,
            targetTable = Profiles,
            foreignKey = Profiles.userId,
            fetchType = FetchType.EAGER
        )

        assertEquals(FetchType.EAGER, relation.fetchType)
    }

    @Test
    fun `OneToOne with cascade`() {
        val relation = OneToOne(
            sourceTable = Users,
            targetTable = Profiles,
            foreignKey = Profiles.userId,
            cascadeTypes = setOf(CascadeType.PERSIST, CascadeType.REMOVE)
        )

        assertTrue(CascadeType.PERSIST in relation.cascadeTypes)
        assertTrue(CascadeType.REMOVE in relation.cascadeTypes)
        assertFalse(CascadeType.MERGE in relation.cascadeTypes)
    }

    // ============== OneToMany Tests ==============

    @Test
    fun `OneToMany relation is created correctly`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId
        )

        assertEquals(Users, relation.sourceTable)
        assertEquals(Posts, relation.targetTable)
        assertEquals(Posts.authorId, relation.foreignKey)
    }

    @Test
    fun `OneToMany with cascade all`() {
        val relation = OneToMany(
            sourceTable = Users,
            targetTable = Posts,
            foreignKey = Posts.authorId,
            cascadeTypes = setOf(CascadeType.ALL)
        )

        assertTrue(CascadeType.ALL in relation.cascadeTypes)
    }

    // ============== ManyToOne Tests ==============

    @Test
    fun `ManyToOne relation is created correctly`() {
        val relation = ManyToOne(
            sourceTable = Posts,
            targetTable = Users,
            foreignKey = Posts.authorId
        )

        assertEquals(Posts, relation.sourceTable)
        assertEquals(Users, relation.targetTable)
        assertEquals(Posts.authorId, relation.foreignKey)
    }

    // ============== ManyToMany Tests ==============

    @Test
    fun `ManyToMany relation is created correctly`() {
        val relation = ManyToMany(
            sourceTable = Users,
            targetTable = Roles,
            joinTable = UserRoles,
            sourceKey = UserRoles.userId,
            targetKey = UserRoles.roleId
        )

        assertEquals(Users, relation.sourceTable)
        assertEquals(Roles, relation.targetTable)
        assertEquals(UserRoles, relation.joinTable)
        assertEquals(UserRoles.userId, relation.sourceKey)
        assertEquals(UserRoles.roleId, relation.targetKey)
    }

    // ============== FetchType Tests ==============

    @Test
    fun `FetchType values are correct`() {
        assertEquals(2, FetchType.entries.size)
        assertNotNull(FetchType.LAZY)
        assertNotNull(FetchType.EAGER)
    }

    // ============== CascadeType Tests ==============

    @Test
    fun `CascadeType values are correct`() {
        assertEquals(5, CascadeType.entries.size)
        assertNotNull(CascadeType.PERSIST)
        assertNotNull(CascadeType.MERGE)
        assertNotNull(CascadeType.REMOVE)
        assertNotNull(CascadeType.REFRESH)
        assertNotNull(CascadeType.ALL)
    }
}
