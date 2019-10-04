package com.soywiz.kminiorm

import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.*
import com.fasterxml.jackson.module.kotlin.*
import com.soywiz.kminiorm.internal.*
import kotlinx.coroutines.*
import java.sql.*
import kotlin.coroutines.*

//class Db(val connection: String, val user: String, val pass: String, val dispatcher: CoroutineDispatcher = Dispatchers.IO) : DbQueryable {
class Db(val connection: String, val user: String, val pass: String, override val dispatcher: CoroutineContext = Dispatchers.IO) : DbBase {
    override val mapper = KotlinMapper.registerModule(KotlinModule()).registerModule(object : SimpleModule() {
        override fun setupModule(context: SetupContext) {
            addSerializer(Blob::class.java, object : JsonSerializer<Blob>() {
                override fun serialize(value: Blob, gen: JsonGenerator, serializers: SerializerProvider) {
                    gen.writeBinary(value.binaryStream.readBytes())
                }
            })
        }
    })

    @PublishedApi
    internal val connectionPool = Pool {
        withContext(dispatcher) {
            DriverManager.getConnection(connection, user, pass).also { it.autoCommit = false }
        }
    }

    override suspend fun <T> transaction(callback: suspend DbTransaction.() -> T): T {
        return connectionPool.take {
            val tr = DbTransaction(this, it)
            tr.run {
                try {
                    callback(tr).also { this@Db.commit() }
                } catch (e: Throwable) {
                    this@Db.rollback()
                    throw e
                }
            }
        }
    }

    override fun quoteColumnName(str: String) = _quote(str)
    override fun quoteTableName(str: String) = _quote(str)
    override fun quoteString(str: String) = _quote(str, type = '\'')
    override fun quoteLiteral(value: Any?) = when (value) {
        null -> "NULL"
        is Int, is Long, is Float, is Double, is Number -> "$value"
        is String -> quoteString(value)
        else -> quoteString("$value")
    }

    private fun _quote(str: String, type: Char = '"') = buildString {
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

    override suspend fun query(sql: String, vararg params: Any?) = transaction { query(sql, *params) }
}

interface DbQuoteable {
    fun quoteColumnName(str: String): String
    fun quoteTableName(str: String): String
    fun quoteString(str: String): String
    fun quoteLiteral(value: Any?): String
}

interface DbBase : DbQueryable, DbQuoteable {
    val mapper: ObjectMapper
    val dispatcher: CoroutineContext
    suspend fun <T> transaction(callback: suspend DbTransaction.() -> T): T
}

val ResultSet.columnNames get() = (1..metaData.columnCount).map { metaData.getColumnName(it) }

fun ResultSet.toListMap(): List<Map<String, Any?>> {
    val metaData = this.metaData
    val out = arrayListOf<Map<String, Any?>>()
    while (this.next()) {
        val row = LinkedHashMap<String, Any?>()
        for (column in 1..metaData.columnCount) {
            row[metaData.getColumnName(column)] = this.getObject(column)
        }
        out.add(row)
    }
    return out
}

class DbTransaction(val db: DbBase, val connection: Connection) : DbQueryable {
    suspend fun DbBase.commit() {
        withContext(dispatcher) {
            this@DbTransaction.connection.commit()
        }
    }

    suspend fun DbBase.rollback() {
        withContext(dispatcher) {
            this@DbTransaction.connection.rollback()
        }
    }

    override suspend fun query(sql: String, vararg params: Any?): DbResult {
        //println("QUERY: $sql")
        return withContext(db.dispatcher) {
            val statement = connection.prepareStatement(sql)
            for (index in params.indices) {
                val param = params[index]
                if (param is ByteArray) {
                    statement.setBlob(index + 1, param.inputStream())
                } else {
                    statement.setObject(index + 1, param)
                }
            }
            val resultSet = when {
                sql.startsWith("select", ignoreCase = true) || sql.startsWith("show", ignoreCase = true) -> statement.executeQuery()
                else -> null.also { statement.executeUpdate() }
            }
            DbResult(resultSet, statement)
        }
    }
}

class DbResult(
    val resultSet: ResultSet?,
    val statement: Statement,
    val data: List<Map<String, Any?>> = resultSet?.toListMap() ?: listOf(mapOf("updateCount" to statement.updateCount))
) : List<Map<String, Any?>> by data {
    val updateCount get() = statement.updateCount
    override fun toString(): String = data.toString()
}
