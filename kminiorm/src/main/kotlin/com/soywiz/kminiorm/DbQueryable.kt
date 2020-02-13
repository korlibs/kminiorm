package com.soywiz.kminiorm

import kotlinx.coroutines.*
import org.intellij.lang.annotations.*

interface DbQueryable {
    suspend fun query(@Language("SQL") sql: String, vararg params: Any?): DbResult
}

fun DbQueryable.queryBlocking(@Language("SQL") sql: String, vararg params: Any?): DbResult = runBlocking { query(sql, *params) }
