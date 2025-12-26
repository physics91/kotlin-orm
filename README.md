# Korma

A modern Kotlin ORM framework with type-safe DSL, reactive R2DBC support, and production-grade features.

[![Build](https://github.com/physics91/kotlin-orm/actions/workflows/ci.yml/badge.svg)](https://github.com/physics91/kotlin-orm/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/physics91/kotlin-orm/branch/master/graph/badge.svg)](https://codecov.io/gh/physics91/kotlin-orm)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Type-Safe DSL** - Compile-time checked queries with Kotlin's type system
- **Reactive Support** - Full R2DBC integration with Kotlin Coroutines and Flow
- **Multi-Database** - PostgreSQL, MySQL, H2, SQLite dialects
- **Caching** - Multi-level caching with Caffeine and Redis support
- **Resilience** - Retry policies, circuit breakers, and timeout configuration
- **Migrations** - Schema versioning and migration management
- **Code Generation** - Generate table definitions from existing databases
- **Framework Integration** - Spring Boot and Ktor plugins
- **Metrics** - Micrometer integration for monitoring

## Installation

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    // Core
    implementation("com.physics91.korma:korma-core:0.1.0")
    implementation("com.physics91.korma:korma-jdbc:0.1.0")

    // Database dialect (choose one)
    implementation("com.physics91.korma:korma-dialect-postgresql:0.1.0")
    implementation("com.physics91.korma:korma-dialect-mysql:0.1.0")
    implementation("com.physics91.korma:korma-dialect-h2:0.1.0")

    // Optional: Reactive support
    implementation("com.physics91.korma:korma-r2dbc:0.1.0")

    // Optional: Caching
    implementation("com.physics91.korma:korma-cache-caffeine:0.1.0")

    // Optional: Spring Boot
    implementation("com.physics91.korma:korma-spring-boot-starter:0.1.0")
}
```

### Using BOM

```kotlin
dependencies {
    implementation(platform("com.physics91.korma:korma-bom:0.1.0"))
    implementation("com.physics91.korma:korma-core")
    implementation("com.physics91.korma:korma-jdbc")
}
```

## Quick Start

### Define Tables

```kotlin
import com.physics91.korma.schema.Table

object Users : Table("users") {
    val id = long("id").primaryKey().autoIncrement()
    val username = varchar("username", 50)
    val email = varchar("email", 100).nullable()
    val age = integer("age").nullable()
    val active = boolean("active")
    val createdAt = timestamp("created_at").nullable()
}

object Posts : Table("posts") {
    val id = long("id").primaryKey().autoIncrement()
    val authorId = long("author_id")
    val title = varchar("title", 200)
    val content = text("content")
    val published = boolean("published")
}
```

### Create Database Connection

```kotlin
import com.physics91.korma.jdbc.JdbcDatabase
import com.physics91.korma.dialect.postgresql.PostgreSqlDialect

val database = JdbcDatabase(
    url = "jdbc:postgresql://localhost:5432/mydb",
    username = "user",
    password = "password",
    dialect = PostgreSqlDialect
)
```

### Insert Data

```kotlin
// Single insert
val userId = database.transaction {
    insertInto(Users) {
        set(Users.username, "john_doe")
        set(Users.email, "john@example.com")
        set(Users.age, 25)
        set(Users.active, true)
    }.executeAndGetId()
}

// Batch insert
val users = listOf(
    Triple("alice", "alice@example.com", 28),
    Triple("bob", "bob@example.com", 32),
    Triple("charlie", "charlie@example.com", 24)
)

database.transaction {
    batchInsertInto(Users, users) { (name, email, age) ->
        set(Users.username, name)
        set(Users.email, email)
        set(Users.age, age)
        set(Users.active, true)
    }.execute()
}
```

### Query Data

```kotlin
// Simple select
val activeUsers = database.transaction {
    from(Users)
        .where { Users.active eq true }
        .orderBy(Users.username.asc())
        .fetch { row ->
            User(
                id = row[Users.id]!!,
                username = row[Users.username]!!,
                email = row[Users.email],
                age = row[Users.age]
            )
        }
}

// Select with conditions
val adults = database.transaction {
    from(Users)
        .where { (Users.age gte 18) and (Users.active eq true) }
        .fetch { row -> row[Users.username]!! }
}

// Select specific columns
val emails = database.transaction {
    from(Users)
        .select(Users.email)
        .where { Users.email.isNotNull() }
        .fetch { row -> row[Users.email]!! }
}

// Pagination
val page = database.transaction {
    from(Users)
        .orderBy(Users.id.asc())
        .limit(10)
        .offset(20)
        .fetch { mapUser(it) }
}

// Count and exists
val count = database.transaction { from(Users).count() }
val exists = database.transaction {
    from(Users).where { Users.username eq "john_doe" }.exists()
}
```

### Update Data

```kotlin
// Update single record
database.transaction {
    update(Users)
        .set(Users.email, "newemail@example.com")
        .set(Users.age, 26)
        .where { Users.id eq userId }
        .execute()
}

// Bulk update
database.transaction {
    update(Users)
        .set(Users.active, false)
        .where { Users.age lt 18 }
        .execute()
}
```

### Delete Data

```kotlin
// Delete by condition
database.transaction {
    deleteFrom(Users)
        .where { Users.active eq false }
        .execute()
}

// Delete all
database.transaction {
    deleteFrom(Users).execute()
}
```

### Transactions

```kotlin
// Automatic commit on success, rollback on exception
database.transaction {
    val fromAccount = from(Accounts)
        .where { Accounts.id eq fromId }
        .fetchFirst { it[Accounts.balance]!! }!!

    if (fromAccount < amount) {
        throw IllegalStateException("Insufficient funds")
    }

    update(Accounts)
        .set(Accounts.balance, fromAccount - amount)
        .where { Accounts.id eq fromId }
        .execute()

    val toAccount = from(Accounts)
        .where { Accounts.id eq toId }
        .fetchFirst { it[Accounts.balance]!! }!!

    update(Accounts)
        .set(Accounts.balance, toAccount + amount)
        .where { Accounts.id eq toId }
        .execute()
}
```

### Advanced Queries

```kotlin
// LIKE operator
val johns = database.transaction {
    from(Users)
        .where { Users.username like "john%" }
        .fetch { mapUser(it) }
}

// IN operator
val specificUsers = database.transaction {
    from(Users)
        .where { Users.id inList listOf(1L, 2L, 3L) }
        .fetch { mapUser(it) }
}

// NULL checks
val noEmail = database.transaction {
    from(Users)
        .where { Users.email.isNull() }
        .fetch { mapUser(it) }
}

// Complex conditions
val filtered = database.transaction {
    from(Users)
        .where {
            ((Users.age gte 18) and (Users.age lte 65)) or
            (Users.username like "admin%")
        }
        .fetch { mapUser(it) }
}
```

## Reactive Database (R2DBC)

```kotlin
import com.physics91.korma.r2dbc.R2dbcDatabase
import com.physics91.korma.r2dbc.R2dbcConnectionFactory
import kotlinx.coroutines.flow.toList

// Create connection
val options = R2dbcConnectionFactory.options()
    .postgresql("localhost", 5432, "mydb")
    .credentials("user", "password")
    .build()

val factory = R2dbcConnectionFactory.create(
    options,
    R2dbcConnectionFactory.PoolConfig(initialSize = 5, maxSize = 20)
)

val database = R2dbcDatabase(factory, PostgreSqlDialect)

// Suspend functions
suspend fun getUsers(): List<User> {
    return database.select(SelectBuilder(Users)) { row ->
        User(
            id = row.get("id", java.lang.Long::class.java)!!,
            username = row.get("username", String::class.java)!!
        )
    }
}

// Streaming with Flow
suspend fun streamUsers(): Flow<User> {
    return database.selectFlow(SelectBuilder(Users)) { row ->
        User(
            id = row.get("id", java.lang.Long::class.java)!!,
            username = row.get("username", String::class.java)!!
        )
    }
}

// Reactive transactions
suspend fun transfer(fromId: Long, toId: Long, amount: BigDecimal) {
    database.transaction {
        // Operations within coroutine transaction
    }
}
```

## Spring Boot Integration

```kotlin
// application.yml
korma:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    username: user
    password: password
  dialect: postgresql
  show-sql: true

// Usage with auto-configuration
@Service
class UserService(private val kormaTemplate: KormaTemplate) {

    fun findActiveUsers(): List<User> = kormaTemplate.transaction {
        from(Users)
            .where { Users.active eq true }
            .fetch { mapUser(it) }
    }
}
```

## Ktor Integration

```kotlin
fun Application.module() {
    install(Korma) {
        jdbc {
            url = "jdbc:postgresql://localhost:5432/mydb"
            username = "user"
            password = "password"
            dialect = PostgreSqlDialect
        }
    }

    routing {
        get("/users") {
            val users = call.kormaTransaction {
                from(Users).fetch { mapUser(it) }
            }
            call.respond(users)
        }
    }
}
```

## Caching

```kotlin
import com.physics91.korma.cache.caffeine.CaffeineCache

// Configure cache
val cache = CaffeineCache<String, User>(
    maximumSize = 1000,
    expireAfterWrite = Duration.ofMinutes(10)
)

// Use with queries
val user = cache.get("user:$id") {
    database.transaction {
        from(Users)
            .where { Users.id eq id }
            .fetchFirst { mapUser(it) }
    }
}
```

## Code Generation

Generate table definitions from existing database:

```kotlin
// build.gradle.kts
plugins {
    id("com.physics91.korma") version "0.1.0"
}

korma {
    database {
        url = "jdbc:postgresql://localhost:5432/mydb"
        username = "user"
        password = "password"
    }
    codegen {
        packageName = "com.example.db.tables"
        outputDir = "src/main/kotlin"
    }
}

// Run: ./gradlew generateKormaTables
```

## Modules

| Module | Description |
|--------|-------------|
| `korma-core` | DSL, schema definitions, SQL generation |
| `korma-jdbc` | Synchronous JDBC operations |
| `korma-r2dbc` | Reactive database with coroutines |
| `korma-dao` | Entity management and relations |
| `korma-cache` | Caching abstractions |
| `korma-cache-caffeine` | Caffeine cache implementation |
| `korma-cache-redis` | Redis cache implementation |
| `korma-migration` | Schema migrations |
| `korma-codegen` | Code generation from schema |
| `korma-micrometer` | Metrics integration |
| `korma-spring-boot-starter` | Spring Boot auto-configuration |
| `korma-ktor-plugin` | Ktor integration |
| `korma-gradle-plugin` | Gradle tasks |
| `korma-dialect-postgresql` | PostgreSQL dialect |
| `korma-dialect-mysql` | MySQL dialect |
| `korma-dialect-h2` | H2 dialect |
| `korma-dialect-sqlite` | SQLite dialect |

## Requirements

- Kotlin 2.0+
- JDK 17+
- Gradle 8.0+

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
