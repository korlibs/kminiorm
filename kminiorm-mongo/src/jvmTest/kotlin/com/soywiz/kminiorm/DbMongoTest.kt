package com.soywiz.kminiorm

import kotlin.test.*

class DbMongoTest {
    val HELLO = "hello"
    val WORLD = "world"

    @Test
    //@Ignore
    fun test() = suspendTest {
        val mongo = DbMongo("mongodb://127.0.0.1:27017/kminiormtest")
        val demos = mongo.table<Demo>()
        val demo = demos.insert(Demo(demo = HELLO)).instance
        val demo2 = demos.findOne { Demo::_id eq demo._id }
        assertEquals(demo, demo2)
        demos.update(Partial(Demo::demo to WORLD)) { Demo::_id eq demo._id }
        assertEquals(WORLD, demos.findOne { Demo::_id eq demo._id }?.demo)
        //println(demos.findAll())
    }

    data class Demo(
            override val _id: DbKey = DbKey(),
            val demo: String = "test"
    ) : DbModel
}