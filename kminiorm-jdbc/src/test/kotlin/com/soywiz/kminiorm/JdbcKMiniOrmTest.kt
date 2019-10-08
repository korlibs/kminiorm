package com.soywiz.kminiorm

class JdbcKMiniOrmTest : KMiniOrmBaseTests(JdbcDb("jdbc:h2:mem:test;DB_CLOSE_DELAY=10", "user", ""))
