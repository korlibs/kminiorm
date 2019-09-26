package com.soywiz.kminiorm

import java.util.*
import kotlin.test.*

class KminiOrmTest {
    @Test
    fun test() {
        val db = Db("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "user", "")
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

    data class Demo(
        //val _id: String = UUID.randomUUID().toString(),
        @Unique val _id: UUID = UUID.randomUUID(),
        @MaxLength(256)

        //@Name("test2")
        @Index val test: String
    )


}