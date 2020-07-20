# kminiorm

[![Actions Status](https://github.com/soywiz/kminiorm/workflows/CI/badge.svg)](https://github.com/soywiz/kminiorm/actions?query=workflow%3ACI)

ORM for Kotlin supporting JDBC and MongoDB

## Repository:

https://bintray.com/korlibs/korlibs/kminiorm

```
repositories {
    jcenter()
    maven { url = uri("https://dl.bintray.com/korlibs/korlibs") }
}
dependencies {
    implementation("com.soywiz.korlibs.kminiorm:kminiorm-jvm:0.9.0")
}
```

## Sample:

You can run `./sample.main.kts` to get it working.

```kotlin
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
