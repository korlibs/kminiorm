package com.soywiz.kminiorm

import java.util.*
import kotlin.test.*

class JdbcDbTest {
    fun db() = JdbcDb("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", "")

    @Test
    fun test() = suspendTest {
        val db = db()
        val demoTable = db.table<Demo>()
        demoTable.insert(Demo(test = "hello"))
        demoTable.insert(Demo(test = "world"))
        //demoTable.insert(Partial(Demo::test to "hello"))
        //query("insert into ${demoTable.quotedTableName} (\"test\") VALUES (?)", "hello")
        println(demoTable.find { Demo::test eq "test" })
        println(demoTable.find { Demo::test eq "hello" })
        println(demoTable.find { Demo::test IN listOf("hello") })
        println(demoTable.find { NOT(Demo::test IN listOf("hello")) })
        println(demoTable.find())
        //demoTable.update(Partial(Demo::test to "hi")) { (Demo::test eq "hello") OR (Demo::test eq "world") }
        demoTable.update(Partial(Demo::test to "hi")) { Demo::test IN listOf("hello", "world") }
        println(demoTable.find())
    }

    @Test
    fun testByteArray() = suspendTest {
        val db = db()
        val demoTable = db.table<Demo2>()
        demoTable.insert(Demo2(bytes = byteArrayOf(1, 2, 3, 4)))
        assertEquals(byteArrayOf(1, 2, 3, 4).toList(), demoTable.find().first().bytes.toList())
    }

    @Test
    fun testList() = suspendTest {
        val db = db()
        val demoTable = db.table<Demo3>()
        demoTable.insert(Demo3(listOf(Demo3.Item("hello"), Demo3.Item("world"))))
        assertEquals(listOf("hello", "world"), demoTable.find().first().items.map { it.name })
    }

    @Test
    fun testItem() = suspendTest {
        val db = db()
        val demoTable = db.table<Demo4>()
        demoTable.insert(Demo4(Demo4.Item("hello")))
        assertEquals("hello", demoTable.find().first().item.name)
    }

    data class Demo4(val item: Item) : DbModel.Base() {
        data class Item(val name: String)
    }

    data class Demo3(val items: List<Item>) : DbModel.Base() {
        data class Item(val name: String)
    }

    data class Demo2(val bytes: ByteArray, val time: Date = Date()) : DbModel.Base()

    data class Demo(
            @DbUnique override val _id: DbKey = DbKey(),
            @DbMaxLength(256)

            //@Name("test2")
            @DbIndex val test: String
    ) : DbModel
}