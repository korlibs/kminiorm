package com.soywiz.kminiorm

class JdbcKMiniOrmTest : KMiniOrmBaseTests(JdbcDb("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", "", async = false)) {
//class JdbcKMiniOrmTest : KMiniOrmBaseTests(JdbcDb("jdbc:h2:file:" + File("./test").absoluteFile, "user", "")) {
    init {
        println("JdbcKMiniOrmTest:" + (db as JdbcDb).connection)
    }
}
