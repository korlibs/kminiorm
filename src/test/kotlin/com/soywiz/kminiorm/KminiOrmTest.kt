package com.soywiz.kminiorm

import kotlinx.coroutines.*
import java.util.*
import kotlin.test.*

class KminiOrmTest {
    @Test
    fun test() {
        runBlocking {
            val db = Db("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", "")
            val demoTable = db.table<Demo>()
            demoTable.insert(Demo(test = "hello"))
            demoTable.insert(Demo(test = "world"))
            //demoTable.insert(Partial(Demo::test to "hello"))
            //query("insert into ${demoTable.quotedTableName} (\"test\") VALUES (?)", "hello")
            println(demoTable.select { Demo::test eq "test" })
            println(demoTable.select { Demo::test eq "hello" })
            println(demoTable.select { Demo::test IN listOf("hello") })
            println(demoTable.select { NOT(Demo::test IN listOf("hello")) })
            println(demoTable.select())
            //demoTable.update(Partial(Demo::test to "hi")) { (Demo::test eq "hello") OR (Demo::test eq "world") }
            demoTable.update(Partial(Demo::test to "hi")) { Demo::test IN listOf("hello", "world") }
            println(demoTable.select())
        }
    }

    @Test
    fun testByteArray() {
        runBlocking {
            val db = Db("jdbc:h2:mem:test2;DB_CLOSE_DELAY=10", "user", "")
            val demoTable = db.table<Demo2>()
            demoTable.insert(Demo2(bytes = byteArrayOf(1, 2, 3, 4)))
            assertEquals(byteArrayOf(1, 2, 3, 4).toList(), demoTable.select().first().bytes.toList())
        }
    }

    @Test
    fun testList() {
        runBlocking {
            val db = Db("jdbc:h2:mem:test2;DB_CLOSE_DELAY=10", "user", "")
            val demoTable = db.table<Demo3>()
            demoTable.insert(Demo3(listOf(Demo3.Item("hello"), Demo3.Item("world"))))
            assertEquals(listOf("hello", "world"), demoTable.select().first().items.map { it.name })
        }
    }

    @Test
    fun testItem() {
        runBlocking {
            val db = Db("jdbc:h2:mem:test2;DB_CLOSE_DELAY=10", "user", "")
            val demoTable = db.table<Demo4>()
            demoTable.insert(Demo4(Demo4.Item("hello")))
            assertEquals("hello", demoTable.select().first().item.name)
        }
    }

    data class Demo4(val item: Item){
        data class Item(val name: String)
    }

    data class Demo3(val items: List<Item>){
        data class Item(val name: String)
    }

    data class Demo2(val bytes: ByteArray, val time: Date = Date())

    data class Demo(
        //val _id: String = UUID.randomUUID().toString(),
        @Unique val _id: UUID = UUID.randomUUID(),
        @MaxLength(256)

        //@Name("test2")
        @Index val test: String
    )


}