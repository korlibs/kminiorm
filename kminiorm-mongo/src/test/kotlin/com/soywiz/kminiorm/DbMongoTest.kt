package com.soywiz.kminiorm

import io.vertx.core.*
import kotlinx.coroutines.*
import kotlin.test.*
import kotlin.test.Ignore

class DbMongoTest {
    @Test
    //@Ignore
    fun test() {
        runBlocking {
            val vertx = Vertx.vertx()
            val mongo = vertx.createMongo("mongodb://127.0.0.1:27017/kminiormtest")
            val demos = mongo.table<Demo>()
            demos.insert(Demo(demo = "hello"))
            println(demos.findAll())
        }
    }

    data class Demo(
            override val _id: DbKey = DbKey(),
            val demo: String = "test"
    ) : DbModel
}