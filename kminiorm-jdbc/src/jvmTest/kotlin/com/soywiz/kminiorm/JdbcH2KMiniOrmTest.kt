package com.soywiz.kminiorm

import com.soywiz.kminiorm.dialect.*
import com.soywiz.kminiorm.where.*
import kotlin.test.*

//val H2_DEBUG_SQL = true
val H2_DEBUG_SQL = false
class JdbcH2KMiniOrmTest : JdbcKMiniOrmTest(JdbcDb("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", "", async = false, debugSQL = H2_DEBUG_SQL, dialect = H2Dialect)) {
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
}
