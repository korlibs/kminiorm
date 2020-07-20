package com.soywiz.kminiorm

import com.soywiz.kminiorm.where.*
import kotlin.test.*

abstract class JdbcKMiniOrmTest(db: Db) : KMiniOrmBaseTests(db) {
    @Test
    open fun testAutoIncrement() = suspendTest {
        val table = db.table<TableAutoIncrement>()
        table.deleteAll()
        val result1 = table.insert(TableAutoIncrement("a"))
        val result2 = table.insert(TableAutoIncrement("b"))
        assertEquals(1L, result1.lastInsertId)
        assertEquals(2L, result2.lastInsertId)
        assertEquals(2, table.where.countRows())
        assertEquals("a", table.where { it::id eq 1 }.findOne()!!.tag)
        assertEquals("b", table.where { it::id eq 2 }.findOne()!!.tag)
    }

    data class TableAutoIncrement(
        @DbUnique val tag: String,
        @DbAutoIncrement @DbPrimary val id: Int = -1
    ) : DbTableBaseElement

}