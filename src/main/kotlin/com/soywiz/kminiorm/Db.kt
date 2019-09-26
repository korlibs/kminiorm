package com.soywiz.kminiorm

import java.sql.*

class Db(val connection: String, val user: String, val pass: String) {
    fun <T> transaction(callback: DbTransaction.() -> T): T {
        val tr = DbTransaction(this)
        return tr.run {
            this@Db.begin()
            try {
                callback(tr).also { this@Db.commit() }
            } catch (e: Throwable) {
                this@Db.rollback()
                throw e
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

class DbTransaction(val db: Db) {
    lateinit var connection: Connection
    fun Db.begin() {
        this@DbTransaction.connection = DriverManager.getConnection(db.connection, db.user, db.pass)
        this@DbTransaction.connection.autoCommit = false
    }

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
            statement.setObject(index + 1, params[index])
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
