package com.soywiz.kminiorm

import kotlinx.coroutines.*
import org.intellij.lang.annotations.*
import kotlin.coroutines.*

interface DbQueryable {
    suspend fun query(@Language("SQL") sql: String, vararg params: Any?): DbResult
    suspend fun multiQuery(@Language("SQL") sql: String, paramsList: List<Array<out Any?>>): DbResult {
        if (paramsList.isEmpty()) error("paramsList is empty")
        var lastResult: DbResult? = null
        for (params in paramsList) {
            lastResult = query(sql, *params)
        }
        return lastResult!!
    }
}

fun DbQueryable.queryBlocking(@Language("SQL") sql: String, vararg params: Any?): DbResult = runBlocking { query(sql, *params) }

interface DbQuoteable {
    fun quoteColumnName(str: String): String
    fun quoteTableName(str: String): String
    fun quoteString(str: String): String
    fun quoteLiteral(value: Any?): String
}

fun DbQueryBinOp.toSqlString() = when (this) {
    DbQueryBinOp.AND -> "AND"
    DbQueryBinOp.OR -> "OR"
    DbQueryBinOp.LIKE -> "LIKE"
    DbQueryBinOp.EQ -> "="
    DbQueryBinOp.NE -> "<>"
    DbQueryBinOp.GT -> ">"
    DbQueryBinOp.LT -> "<"
    DbQueryBinOp.GE -> ">="
    DbQueryBinOp.LE -> "<="
}
fun DbQueryUnOp.toSqlString() = when (this) {
    DbQueryUnOp.NOT -> "NOT"
}

fun <T> DbQuery<T>.toString(db: DbQuoteable): String = when (this) {
    is DbQuery.BinOp<*, *> -> "${db.quoteTableName(prop.name)}${op.toSqlString()}${db.quoteLiteral(literal)}"
    is DbQuery.Always<*> -> "1=1"
    is DbQuery.Never<*> -> "1=0"
    is DbQuery.BinOpNode<*> -> "((${left.toString(db)}) ${op.toSqlString()} (${right.toString(db)}))"
    is DbQuery.UnOpNode<*> -> "(${op.toSqlString()} (${right.toString(db)}))"
    is DbQuery.IN<*, *> -> {
        if (literal.isNotEmpty()) {
            "${db.quoteTableName(prop.name)} IN (${literal.joinToString(", ") { db.quoteLiteral(it) }})"
        } else {
            "1=0"
        }
    }
    is DbQuery.Raw<*> -> TODO()
    else -> TODO()
}

interface DbBase : Db, DbQueryable, DbQuoteable {
    val debugSQL: Boolean get() = false
    val dispatcher: CoroutineContext
    val async: Boolean get() = true
    suspend fun <T> transaction(callback: suspend DbBaseTransaction.() -> T): T
}

interface DbBaseTransaction : DbQueryable {
    val db: DbBase
    suspend fun DbBase.commit(): Unit
    suspend fun DbBase.rollback(): Unit
}

