#!/usr/bin/env kotlin

//curl -s "https://get.sdkman.io" | bash
//sdk install kotlin

@file:Repository(url = "https://jcenter.bintray.com/")
@file:DependsOn("com.soywiz.kminiorm:kminiorm-jdbc:0.8.2")
@file:DependsOn("org.xerial:sqlite-jdbc:3.30.1")
@file:DependsOn("org.jetbrains.kotlin:kotlin-reflect:1.3.70")
@file:CompilerOptions("-jvm-target", "1.8")

import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.dialect.SqliteDialect
import com.soywiz.kminiorm.where.where
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import java.io.File

data class MyTable(
    @DbPrimary val key: String,
    @DbIndex val value: Long
) : DbBaseModel

runBlocking {
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
            onConflict = DbOnConflict.REPLACE
    )

    val baseQuery = table
        .where { it::value ge 20L }
        .sorted { it::value to +1 }

    baseQuery.limit(4).collect {
        println(it)
    }

    table.findOne { it::key eq "hello" }!!
        .copy(value = 70L)
        .save(table)

    println("UPDATED HELLO")

    baseQuery.limit(10).collect {
        println(it)
    }

}
