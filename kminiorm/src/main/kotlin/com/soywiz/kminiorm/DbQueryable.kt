package com.soywiz.kminiorm

import kotlinx.coroutines.*

interface DbQueryable {
    suspend fun query(sql: String, vararg params: Any?): DbResult
}

fun DbQueryable.queryBlocking(sql: String, vararg params: Any?): DbResult = runBlocking { query(sql, *params) }
