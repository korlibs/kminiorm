package com.soywiz.kminiorm

import kotlinx.coroutines.*
import org.intellij.lang.annotations.*
import java.util.*
import kotlin.coroutines.*

interface DbQueryable {
    suspend fun query(@Language("SQL") sql: String, vararg params: Any?): DbResult
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

open class SqlDialect() : DbQuoteable {
    open val supportPrimaryIndex get() = false

    companion object ANSI : SqlDialect()

    override fun quoteColumnName(str: String) = _quote(str)
    override fun quoteTableName(str: String) = _quote(str)
    override fun quoteString(str: String) = _quote(str, type = '\'')
    override fun quoteLiteral(value: Any?) = when (value) {
        null -> "NULL"
        is Int, is Long, is Float, is Double, is Number -> "$value"
        is DbIntKey -> "${value.key}"
        is String -> quoteString(value)
        is Date -> quoteString(java.sql.Date(value.time).toString())
        else -> quoteString("$value")
    }

    protected fun _quote(str: String, type: Char = '"') = buildString {
        append(type)
        for (char in str) {
            if (char == type) {
                append(type)
                append(type)
            } else {
                append(char)
            }
        }
        append(type)
    }
}

open class MySqlDialect : SqlDialect() {
    override val supportPrimaryIndex get() = true
    companion object : MySqlDialect()
    override fun quoteColumnName(str: String) = _quote(str, '`')
    override fun quoteTableName(str: String) = _quote(str, '`')
}
