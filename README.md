# kminiorm

<!-- BADGES -->
<p align="center">
	<a href="https://github.com/korlibs/kminiorm/actions"><img alt="Build Status" src="https://github.com/korlibs/kminiorm/workflows/CI/badge.svg" /></a>
    <a href="https://search.maven.org/artifact/com.soywiz.korlibs.kminiorm/kminiorm"><img alt="Maven Central" src="https://img.shields.io/maven-central/v/com.soywiz.korlibs.kminiorm/kminiorm"></a>
	<a href="https://discord.korge.org/"><img alt="Discord" src="https://img.shields.io/discord/728582275884908604?logo=discord" /></a>
</p>
<!-- /BADGES -->

ORM for Kotlin supporting JDBC and MongoDB

### Full Documentation: <https://docs.korge.org/old/kminiorm/>

## Gradle:

```kotlin
def kminiOrmVersion = "..." // Find latest version on this README

repositories {
    // ...
    mavenCentral()
}
dependencies {
    // Core:
    implementation("com.soywiz.korlibs.kminiorm:kminiorm-jvm:$kminiOrmVersion")
    // JDBC:
    implementation("com.soywiz.korlibs.kminiorm:kminiorm-jdbc-jvm:$kminiOrmVersion")
    implementation("org.xerial:sqlite-jdbc:3.30.1")
    implementation("com.h2database:h2:1.4.200")
    // Mongo:
    implementation("com.soywiz.korlibs.kminiorm:kminiorm-mongo-jvm:$kminiOrmVersion")
}
```

## Sample:

You can run `./sample.main.kts` to get it working.

```kotlin
import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.dialect.*
import com.soywiz.kminiorm.where.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.*
import java.io.*

fun main() = runBlocking {
    data class MyTable(
        @DbPrimary val key: String,
        @DbIndex val value: Long
    ) : DbBaseModel

    val sqliteFile = File("sample.sq3")
    val db = JdbcDb(
        "jdbc:sqlite:${sqliteFile.absoluteFile.toURI()}",
        debugSQL = System.getenv("DEBUG_SQL") == "true",
        dialect = SqliteDialect,
        async = true
    )

    val table = db.table<MyTable>()
    table.insert(
        MyTable("hello", 10L),
        MyTable("world", 20L),
        MyTable("this", 30L),
        MyTable("is", 40L),
        MyTable("a", 50L),
        MyTable("test", 60L),
        onConflict = DbOnConflict.IGNORE
    )

    table.where { it::value ge 20L }.limit(10).collect {
        println(it)
    }
    Unit
}
```

## Defining Tables

You can use normal Kotlin fields

```kotlin
data class MyTable(
    @DbPrimary val key: String,
    @DbIndex val value: Long
) : DbBaseModel
```

### Multi-column indices

```kotlin
data class MyTable(
    @DbUnique("a_b") val a: String,
    @DbUnique("a_b") val b: String
) : DbBaseModel
```

## Creating a Repository

## Migrations

If you change a table adding a new field to it,
you can register a DbMigration that will be executed
when the ALTER TABLE is automatically performed.

```kotlin
data class MyTable(
    val a: String,
    @DbPerformMigration(MyAddColumnMigration::class) val newlyAddedField: String // 
) : DbBaseModel {
    class MyAddColumnMigration : DbMigration<MyTable> {
        override suspend fun migrate(table: DbTable<MyTable>, action: DbMigration.Action, column: ColumnDef<MyTable>?) {
            table.where.collect { item -> 
                // Update item here ...
            }
        }
    }
}
```
