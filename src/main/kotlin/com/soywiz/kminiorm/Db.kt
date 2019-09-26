package com.soywiz.kminiorm

import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.*
import com.soywiz.kminiorm.internal.*
import java.sql.*

class Db(val connection: String, val user: String, val pass: String) {
    val mapper = KotlinMapper.registerModule(object : SimpleModule() {
        override fun setupModule(context: SetupContext) {
            addSerializer(Blob::class.java, object : JsonSerializer<Blob>() {
                override fun serialize(value: Blob, gen: JsonGenerator, serializers: SerializerProvider) {
                    gen.writeBinary(value.binaryStream.readBytes())
                }
            })
        }
    })

    private val connectionPool = Pool { DriverManager.getConnection(connection, user, pass).also { it.autoCommit = false } }

    fun <T> transaction(callback: DbTransaction.() -> T): T {
        connectionPool.take {
            val tr = DbTransaction(this, it)
            return tr.run {
                try {
                    callback(tr).also { this@Db.commit() }
                } catch (e: Throwable) {
                    this@Db.rollback()
                    throw e
                }
            }
        }
    }

    fun quoteColumnName(str: String) = _quote(str)
    fun quoteTableName(str: String) = _quote(str)
    fun quoteString(str: String) = _quote(str, type = '\'')
    fun quoteLiteral(value: Any?) = when (value) {
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

    fun query(sql: String, vararg params: Any?) = transaction { query(sql, *params) }
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

class DbTransaction(val db: Db, val connection: Connection) {
    fun Db.commit() {
        this@DbTransaction.connection.commit()
    }

    fun Db.rollback() {
        this@DbTransaction.connection.rollback()
    }

    fun query(sql: String, vararg params: Any?): DbResult {
        //println("QUERY: $sql")
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
        return DbResult(resultSet, statement)
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
