package com.soywiz.kminiorm

import java.io.*

class JdbcKMiniOrmTest : KMiniOrmBaseTests(JdbcDb("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", "")) {
//class JdbcKMiniOrmTest : KMiniOrmBaseTests(JdbcDb("jdbc:h2:file:" + File("./test").absoluteFile, "user", "")) {
    init {
        println("JdbcKMiniOrmTest:" + (db as JdbcDb).connection)
    }
}
