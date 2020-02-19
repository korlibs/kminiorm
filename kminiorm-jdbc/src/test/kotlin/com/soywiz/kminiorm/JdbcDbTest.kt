package com.soywiz.kminiorm

import java.text.*
import java.util.*
import kotlin.test.*

class JdbcDbTest {
    fun db(debug: Boolean = false) = JdbcDb("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", "", debugSQL = debug)

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

    @Test
    fun testDate1() = suspendTest {
        //val db = db(debug = true)
        val db = db(debug = false)
        val table = db.table<Date1>().apply { deleteAll() }
        table.upsert(Date1(0, Date(1, 2, 3, 4, 5, 6)))
        table.upsert(Date1(0, Date(1, 2, 3, 4, 5, 6)))
        assertEquals(
            "1901-03-03 04:05:06",
            SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(table.findOne()!!.date)
        )
    }

    data class Date1(@DbPrimary val id: Int, val date: Date) : DbModel.Base<Demo4>() {
    }

    data class Demo4(val item: Item) : DbModel.Base<Demo4>() {
        data class Item(val name: String)
    }

    data class Demo3(val items: List<Item>) : DbModel.Base<Demo3>() {
        data class Item(val name: String)
    }

    data class Demo2(val bytes: ByteArray, val time: Date = Date()) : DbModel.Base<Demo2>()

    data class Demo(
            @DbUnique override val _id: DbKey = DbKey(),
            @DbMaxLength(256)

            //@Name("test2")
            @DbIndex val test: String
    ) : DbModel
}