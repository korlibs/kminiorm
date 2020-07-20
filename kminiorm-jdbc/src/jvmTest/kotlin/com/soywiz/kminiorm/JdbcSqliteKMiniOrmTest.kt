package com.soywiz.kminiorm

import com.soywiz.kminiorm.dialect.SqliteDialect
import com.soywiz.kminiorm.where.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class JdbcSqliteKMiniOrmTest : JdbcKMiniOrmTest(JdbcDb("jdbc:sqlite::memory:", "", "", async = false, debugSQL = true, dialect = SqliteDialect)) {
    init {
        println("JdbcKMiniOrmTest:" + (db as JdbcDb).connection)
    }

    @Test
    fun testTableUpgrade2() = suspendTest {
        val tableV1 = db.table<TableV1>()
        val tableV2 = db.table<TableV2>()
        tableV1.deleteAll()
        val row1 = tableV1.insert(TableV1("hello", "world"))
        val row2 = tableV2.findOne { TableV2::name eq "hello" }
        assertEquals("hello", row2?.name)
        assertEquals("", row2?.fieldV2)
        assertEquals(1, tableV2.findAll().count())
    }

    @Test
    @Ignore // @TODO: Ignored
    override fun testBoolean() = super.testBoolean()

    @Test
    fun testSimpleFlow1() = suspendTest {
        val tableV1 = db.table<TableV1>()
        tableV1.deleteAll()
        tableV1.where.findFlow().collect {
        }
    }
}
