package com.soywiz.kminiorm

interface DbQueryable {
    suspend fun query(sql: String, vararg params: Any?): DbResult
}
