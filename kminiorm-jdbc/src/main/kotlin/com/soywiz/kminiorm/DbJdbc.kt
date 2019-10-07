package com.soywiz.kminiorm

import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.*
import com.fasterxml.jackson.databind.node.*
import com.fasterxml.jackson.module.kotlin.*
import com.soywiz.kminiorm.internal.*
import kotlinx.coroutines.*
import java.io.*
import java.sql.*
import java.util.*
import kotlin.coroutines.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

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
    is DbQuery.BinOpNode<*> -> "((${left.toString(db)}) ${op.toSqlString()} (${right.toString(db)}))"
    is DbQuery.UnOpNode<*> -> "(${op.toSqlString()} (${right.toString(db)}))"
    is DbQuery.IN<*, *> -> "${db.quoteTableName(prop.name)} IN (${literal.joinToString(", ") { db.quoteLiteral(it) }})"
    is DbQuery.Raw<*> -> TODO()
    else -> kotlin.TODO()
}

interface DbBase : Db, DbQueryable, DbQuoteable {
    val mapper: ObjectMapper
    val dispatcher: CoroutineContext
    suspend fun <T> transaction(callback: suspend DbTransaction.() -> T): T
}

//class Db(val connection: String, val user: String, val pass: String, val dispatcher: CoroutineDispatcher = Dispatchers.IO) : DbQueryable {
class JdbcDb(val connection: String, val user: String, val pass: String, override val dispatcher: CoroutineContext = Dispatchers.IO) : DbBase {
    override suspend fun <T : DbTableElement> table(clazz: KClass<T>): DbTable<T> = DbJdbcTable(this, clazz).initialize()

    override val mapper = KotlinMapper.registerModule(KotlinModule()).also { mapper ->
        mapper.registerModule(object : SimpleModule() {
            override fun setupModule(context: SetupContext) {
                addSerializer(Blob::class.java, object : JsonSerializer<Blob>() {
                    override fun serialize(value: Blob, gen: JsonGenerator, serializers: SerializerProvider) {
                        gen.writeBinary(value.binaryStream.readBytes())
                    }
                })
            }
        })
        mapper.registerDbKeyModule()
    }

    @PublishedApi
    internal val connectionPool = InternalDbPool {
        withContext(dispatcher) {
            DriverManager.getConnection(connection, user, pass).also { it.autoCommit = false }
        }
    }

    override suspend fun <R> transaction(callback: suspend DbTransaction.() -> R): R {
        return connectionPool.take {
            val tr = DbTransaction(this, it)
            tr.run {
                try {
                    callback(tr).also { this@JdbcDb.commit() }
                } catch (e: Throwable) {
                    this@JdbcDb.rollback()
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


class DbTransaction(val db: DbBase, val connection: Connection) : DbQueryable {
    suspend fun DbBase.commit() {
        kotlinx.coroutines.withContext(dispatcher) {
            this@DbTransaction.connection.commit()
        }
    }

    suspend fun DbBase.rollback() {
        kotlinx.coroutines.withContext(dispatcher) {
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
            JdbcDbResult(resultSet, statement)
        }
    }
}

abstract class SqlTable<T : DbTableElement> : DbTable<T>, DbQueryable, ColumnExtra {
    abstract val table: DbJdbcTable<T>
    private val _db get() = table.db
    private val _quotedTableName get() = table.quotedTableName

    override suspend fun showColumns(): Map<String, Map<String, Any?>> {
        return query("SHOW COLUMNS FROM $_quotedTableName;").associateBy { it["COLUMN_NAME"]?.toString() ?: "-" }
    }

    override suspend fun initialize(): DbTable<T> = this.apply {
        query("CREATE TABLE IF NOT EXISTS $_quotedTableName;")
        val oldColumns = showColumns()
        for (column in table.columns) {
            if (column.name in oldColumns) continue // Do not add columns if they already exists

            query(buildString {
                append("ALTER TABLE ")
                append(_quotedTableName)
                append(" ADD ")
                append(column.quotedName)
                append(" ")
                append(column.sqlType)
                if (column.isNullable) {
                    append(" NULLABLE")
                } else {
                    append(" NOT NULL")
                    when {
                        column.jclazz == String::class.java -> append(" DEFAULT (\"\")")
                        column.jclazz.isSubclassOf(Number::class) -> append(" DEFAULT (0)")
                    }
                }
                append(";")
            })
        }

        for (column in table.columns) {
            //println("$column: ${column.quotedName}: ${column.isUnique}, ${column.isIndex}")
            if (column.isUnique || column.isIndex) {
                val unique = column.isUnique
                query(buildString {
                    append("CREATE ")
                    if (unique) append("UNIQUE ")
                    append("INDEX IF NOT EXISTS ${column.quotedName} ON $_quotedTableName (${column.quotedName});")
                })
            }
        }
    }

    override suspend fun insert(instance: T): T {
        insert(_db.mapper.convertValueToMap(instance))
        return instance
    }

    override suspend fun insert(data: Map<String, Any?>): DbResult {
        val entries = table.toColumnMap(data).entries

        return query(buildString {
            append("INSERT INTO ")
            append(_quotedTableName)
            append("(")
            append(entries.joinToString(", ") { it.key.quotedName })
            append(")")
            append(" VALUES ")
            append("(")
            append(entries.joinToString(", ") { "?" })
            append(")")
        }, *entries.map { table.serializeColumn(it.value, it.key) }.toTypedArray())
    }

    override suspend fun find(skip: Long?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Iterable<T> {
        return query(buildString {
            append("SELECT ")
            append("*")
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(DbQueryBuilder.build(query).toString(_db))
            if (limit != null) append(" LIMIT $limit")
            if (skip != null) append(" OFFSET $skip")
            append(";")
        }).map { _db.mapper.convertValue(it.mapValues { (key, value) ->
            table.deserializeColumn(value, table.getColumnByName(key), key)
        }, table.clazz.java) }
    }

    override suspend fun update(set: Partial<T>, increment: Partial<T>?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        if (increment != null) {
            error("DbJdbc.update.increment not implemented yet")
        }
        val entries = table.toColumnMap(set.data).entries
        val keys = entries.map { it.key }
        val values = entries.map { table.serializeColumn(it.value, it.key) }
        return query(buildString {
            append("UPDATE ")
            append(table.quotedTableName)
            append(" SET ")
            append(keys.joinToString(", ") { "${it.quotedName}=?" })
            append(" WHERE ")

            append(DbQueryBuilder.build(query).toString(_db))
            if (limit != null) append(" LIMIT $limit")
            append(";")
        }, *values.toTypedArray()).updateCount
    }
}

interface ColumnExtra {
    val db: DbBase
    val <T : Any> ColumnDef<T>.quotedName get() = db.quoteColumnName(name)
    val <T : Any> ColumnDef<T>.sqlType get() = property.returnType.toSqlType(db, property)
}

class DbJdbcTable<T: DbTableElement>(override val db: DbBase, val clazz: KClass<T>) : SqlTable<T>(), ColumnExtra {
    val ormTableInfo = OrmTableInfo(clazz)
    val tableName = ormTableInfo.tableName
    val quotedTableName = db.quoteTableName(tableName)
    val columns = ormTableInfo.columns
    val columnsByName = ormTableInfo.columnsByName
    fun getColumnByName(name: String) = ormTableInfo.getColumnByName(name)

    override val table: DbJdbcTable<T> get() = this
    override suspend fun query(sql: String, vararg params: Any?): DbResult = db.query(sql, *params)

    override suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R = db.transaction {
        callback(DbTableTransaction(this@DbJdbcTable, this))
    }

    fun toColumnMap(map: Map<String, Any?>): Map<ColumnDef<T>, Any?> {
        @Suppress("UNCHECKED_CAST")
        return map.entries.map { getColumnByName(it.key) to it.value }.filter { it.first != null }.toMap() as Map<ColumnDef<T>, Any?>
    }

    fun serializeColumn(value: Any?, column: ColumnDef<T>?, columnName: String = column?.name ?: "unknown"): Any? {
        return when (value) {
            null -> null
            is ByteArray -> value
            is String -> value
            is Number -> value
            is UUID -> value
            else -> db.mapper.writeValueAsString(value)
        }
        //return value
    }

    fun deserializeColumn(value: Any?, column: ColumnDef<T>?, columnName: String = column?.name ?: "unknown"): Any? {
        return when (value) {
            is InputStream -> value.readBytes()
            is Blob -> value.binaryStream.readBytes()
            is UUID -> value
            //is Number -> (value as? Number) ?: value.toString().toDoubleOrNull()
            else -> when {
                column != null -> when (column.jclazz) {
                    String::class -> value.toString()
                    Number::class -> if (value is Number) value else value.toString().toDouble()
                    UUID::class -> value
                    else -> db.mapper.readValue(value.toString(), column.jclazz.java)
                }
                else -> value
            }
        }
    }
}

class DbTableTransaction<T: DbTableElement>(override val table: DbJdbcTable<T>, val transaction: DbTransaction) : SqlTable<T>() {
    override val db get() = table.db

    override suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R = db.transaction {
        callback(DbTableTransaction(this@DbTableTransaction.table, this))
    }

    override suspend fun query(sql: String, vararg params: Any?): DbResult = transaction.query(sql, *params)
}

fun KType.toSqlType(db: DbBase, annotations: KAnnotatedElement): String {
    return when (this.jvmErasure) {
        Int::class -> "INTEGER"
        ByteArray::class -> "BLOB"
        String::class -> {
            val maxLength = annotations.findAnnotation<DbMaxLength>()
            //if (maxLength != null) "VARCHAR(${maxLength.length})" else "TEXT"
            if (maxLength != null) "VARCHAR(${maxLength.length})" else "VARCHAR"
        }
        else -> "VARCHAR"
    }
}

class JdbcDbResult(
    val resultSet: ResultSet?,
    val statement: Statement,
    override val data: List<Map<String, Any?>> = resultSet?.toListMap() ?: listOf(mapOf("updateCount" to statement.updateCount))
) : DbResult, List<Map<String, Any?>> by data {
    override val updateCount: Long get() = statement.updateCount.toLong()
    override fun toString(): String = data.toString()
}
