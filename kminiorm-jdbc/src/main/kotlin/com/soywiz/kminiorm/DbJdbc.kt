package com.soywiz.kminiorm

import com.soywiz.kminiorm.internal.*
import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.Flow
import org.intellij.lang.annotations.*
import java.io.*
import java.sql.*
import java.text.*
import java.time.*
import java.time.Duration
import java.util.*
import java.util.Date
import kotlin.coroutines.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*
import kotlin.time.*

//val DEBUG_JDBC = true
val DEBUG_JDBC = false

class WrappedConnection(val connectionStr: String, val user: String?, val pass: String?, val timeout: Duration) {
    private var lastUsedTimestamp = 0L
    private var _connection: Connection? = null
    fun clear() = run {
        kotlin.runCatching { _connection?.close() }
        _connection = null
    }
    val connection: Connection get() = run {
        val now = System.currentTimeMillis()
        if (now >= lastUsedTimestamp + timeout.toMillis()) {
            clear()
        }
        if (_connection == null) {
            _connection = DriverManager.getConnection(connectionStr, user, pass).also { it.autoCommit = false }
        }
        lastUsedTimestamp = now
        _connection!!
    }
}

//class Db(val connection: String, val user: String, val pass: String, val dispatcher: CoroutineDispatcher = Dispatchers.IO) : DbQueryable {
class JdbcDb(
    val connection: String,
    val user: String? = null, val pass: String? = null,
    override val dispatcher: CoroutineContext = Dispatchers.IO,
    val typer: Typer = JdbcDbTyper,
    override val debugSQL: Boolean = false,
    val dialect: SqlDialect = SqlDialect.ANSI,
    override val async: Boolean = true,
    // MySQL default timeout is 8 hours
    val connectionTimeout: Duration = Duration.ofHours(1L) // Reuse this connection during 1 hour, then reconnect
) : AbstractDb(), DbBase, DbQuoteable by dialect {
    override fun <T : DbTableBaseElement> constructTable(clazz: KClass<T>): DbTable<T> = DbJdbcTable(this, clazz)

    private fun getConnection(): WrappedConnection = WrappedConnection(connection, user, pass, connectionTimeout)

    @PublishedApi
    internal val connectionPool = InternalDbPool {
        if (async) withContext(dispatcher) { getConnection() } else getConnection()
    }

    override suspend fun <R> transaction(callback: suspend DbBaseTransaction.() -> R): R {
        return connectionPool.take {
            try {
                val tr = DbTransaction(this, it)
                tr.run {
                    try {
                        callback(tr).also { this@JdbcDb.commit() }
                    } catch (e: Throwable) {
                        this@JdbcDb.rollback()
                        throw e
                    }
                }
            } catch (e: SQLNonTransientConnectionException) {
                // Even by reconnecting after timeout, we got an error here. So let's close the connection.
                it.clear()
                // Just in case something is wrong, let's wait 3 seconds before continuing to avoid spamming
                delay(3_000L)
                throw e
            }
        }
    }

    override suspend fun query(@Language("SQL") sql: String, vararg params: Any?) = transaction { query(sql, *params) }
}


class DbTransaction(override val db: DbBase, val wrappedConnection: WrappedConnection) : DbBaseTransaction {
    val connection get() = wrappedConnection.connection

    override suspend fun DbBase.commit(): Unit = if (db.async) {
        withContext(context = dispatcher) { this@DbTransaction.connection.commit() }
    } else {
        this@DbTransaction.connection.commit()
    }

    override suspend fun DbBase.rollback(): Unit = if (db.async) {
        withContext(dispatcher) { this@DbTransaction.connection.rollback() }
    } else {
        this@DbTransaction.connection.rollback()
    }

    private fun _query(sql: String, vararg params: Any?): DbResult {
        val statement = connection.prepareStatement(sql)
        for (index in params.indices) {
            val param = params[index]
            if (param is ByteArray) {
                statement.setBlob(index + 1, param.inputStream())
            } else {
                statement.setObject(index + 1, param)
            }
        }
        val resultSet: ResultSet? = when {
            sql.startsWith("select", ignoreCase = true) || sql.startsWith("show", ignoreCase = true) -> statement.executeQuery()
            else -> null.also { statement.executeUpdate() }
        }
        return JdbcDbResult(resultSet, statement).also { result ->
            if (DEBUG_JDBC) println(" --> $result")
        }
    }

    @UseExperimental(ExperimentalTime::class)
    override suspend fun query(sql: String, vararg params: Any?): DbResult {
        val startTime = MonoClock.markNow()
        try {
            return if (db.async) withContext(db.dispatcher) { _query(sql, *params) } else _query(sql, *params)
        } finally {
            val time = startTime.elapsedNow()
            if (DEBUG_JDBC || db.debugSQL) println("QUERY[$time] : $sql, ${params.toList()}")
        }
    }
}

abstract class SqlTable<T : DbTableBaseElement> : DbTable<T>, DbQueryable, ColumnExtra {
    abstract val table: DbJdbcTable<T>
    override val typer get() = table.db.typer
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

            val result = kotlin.runCatching {
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
                            column.jclazz == String::class -> append(" DEFAULT ('')")
                            column.jclazz.isSubclassOf(Number::class) -> append(" DEFAULT (0)")
                        }
                    }
                    append(";")
                })
            }
            if (result.isFailure) {
                println(result.exceptionOrNull())
            }
        }

        if (table.hasExtrinsicData) {
            val result = kotlin.runCatching {
                query("ALTER TABLE $_quotedTableName ADD $__extrinsic__ VARCHAR NOT NULL DEFAULT '{}'")
            }
            if (result.isFailure) {
                println(result.exceptionOrNull())
            }
        }

        for ((indexName, columns) in table.ormTableInfo.columnIndices) {
            //println("$column: ${column.quotedName}: ${column.isUnique}, ${column.isIndex}")
            val unique = columns.any { it.isUnique }
            val result = kotlin.runCatching {
                query(buildString {
                    append("CREATE ")
                    if (unique) append("UNIQUE ")
                    val packs = columns.map { "${it.quotedName} ${it.indexDirection.sname}" }
                    append("INDEX IF NOT EXISTS ${db.quoteColumnName("${table.tableName}_${indexName}")} ON $_quotedTableName (${packs.joinToString(", ")});")
                })
            }
            if (result.isFailure) {
                println(result.exceptionOrNull())
            }
        }
    }

    override suspend fun insert(instance: T): T {
        insert(JdbcDbTyper.untype(instance) as Map<String, Any?>)
        return instance
    }

    override suspend fun insert(data: Map<String, Any?>): DbResult {
        try {
            val entries = table.toColumnMap(data).entries

            val keys = entries.map { it.key.quotedName }.toMutableList()
            val values = entries.map { table.serializeColumn(it.value, it.key) }.toMutableList()
            if (table.hasExtrinsicData) {
                keys += __extrinsic__
                values += MiniJson.stringify(data.filter { table.getColumnByName(it.key) == null })
            }

            assert(keys.size == values.size)

            return query(buildString {
                append("INSERT INTO ")
                append(_quotedTableName)
                append("(")
                append(keys.joinToString(", "))
                append(")")
                append(" VALUES ")
                append("(")
                append(keys.joinToString(", ") { "?" })
                append(")")
            }, *values.toTypedArray())
        } catch (e: SQLIntegrityConstraintViolationException) {
            throw DuplicateKeyDbException("Conflict", e)
        }
    }

    override suspend fun findFlowPartial(skip: Long?, limit: Long?, fields: List<KProperty1<T, *>>?, sorted: List<Pair<KProperty1<T, *>, Int>>?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Flow<Partial<T>> {
        val rquery = DbQueryBuilder.buildOrNull(query) ?: return listOf<Partial<T>>().asFlow()
        val data = query(buildString {
            append("SELECT ")
            if (fields != null) {
                append(fields.mapNotNull { table.getColumnByProp(it) }.joinToString(", ") { it.quotedName })
            } else {
                append("*")
            }
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(rquery.toString(_db))
            if (sorted != null && sorted.isNotEmpty()) {
                val orders = sorted.map { table.getColumnByProp(it.first)!!.quotedName + when { it.second > 0 -> " ASC"; it.second < 0 -> " DESC"; else -> "" } }
                append(" ORDER BY ${orders.joinToString(", ")}")
            }
            if (limit != null) append(" LIMIT $limit")
            if (skip != null) append(" OFFSET $skip")
            append(";")
        }).map { Partial(it.mapValues { (key, value) ->
            table.deserializeColumn(value, table.getColumnByName(key), key)
        }, table.clazz) }
        // @TODO: Can we optimize this by streaming results?
        return data.asFlow()
    }

    override suspend fun count(query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        val rquery = DbQueryBuilder.buildOrNull(query) ?: return 0L
        val data = query(buildString {
            append("SELECT ")
            append("COUNT(*)")
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(rquery.toString(_db))
            append(";")
        })
        // @TODO: Can we optimize this by streaming results?
        return data.first().values.first().toString().toLong()
    }

    override suspend fun <R> countGrouped(groupedBy: KProperty1<T, R>, query: DbQueryBuilder<T>.() -> DbQuery<T>): Map<R, Long> {
        val rquery = DbQueryBuilder.buildOrNull(query) ?: return mapOf()
        val keyAlias = "__k1"
        val countAlias = "__c1"
        val groupedByCol = table.getColumnByProp(groupedBy) ?: error("Can't find column '$groupedBy'")
        val groupColumnQuotedName = groupedByCol.quotedName
        val data = query(buildString {
            append("SELECT ")
            append(groupColumnQuotedName)
            append(" AS $keyAlias, ")
            append("COUNT(*) AS $countAlias")
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(rquery.toString(_db))
            append(" GROUP BY ")
            append(groupColumnQuotedName)
            append(";")
        })
        // @TODO: Can we optimize this by streaming results?
        return data
            .associate { (table.deserializeColumn(it[keyAlias], groupedByCol, keyAlias) as R) to it[countAlias].toString().toLong() }
    }

    override suspend fun update(set: Partial<T>?, increment: Partial<T>?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        val table = this.table
        val setEntries = (set?.let { table.toColumnMap(it.data).entries } ?: setOf()).toList()
        val incrEntries = (increment?.let { table.toColumnMap(it.data).entries } ?: setOf()).toList()

        // Can't make an UPDATE query that does nothing
        if (setEntries.isEmpty() && incrEntries.isEmpty()) return 0L

        return query(buildString {
            append("UPDATE ")
            append(table.quotedTableName)
            append(" SET ")
            append((setEntries.map { "${it.key.quotedName}=?" } + incrEntries.map { "${it.key.quotedName}=${it.key.quotedName}+?" }).joinToString(", "))
            append(" WHERE ")
            append(DbQueryBuilder.build(query).toString(_db))
            if (limit != null) append(" LIMIT $limit")
            append(";")
        }, *((setEntries + incrEntries).map { table.serializeColumn(it.value, it.key) }).toTypedArray()).updateCount
    }

    override suspend fun delete(limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        return query(buildString {
            append("DELETE FROM ")
            append(table.quotedTableName)
            append(" WHERE ")
            append(DbQueryBuilder.build(query).toString(_db))
            if (limit != null) append(" LIMIT $limit")
            append(";")
        }).updateCount
    }
}

interface ColumnExtra {
    val db: DbBase
    val <T : Any> ColumnDef<T>.quotedName get() = db.quoteColumnName(name)
    val <T : Any> ColumnDef<T>.sqlType get() = property.returnType.toSqlType(db, property)
}

class DbJdbcTable<T : DbTableBaseElement>(override val db: JdbcDb, override val clazz: KClass<T>) : SqlTable<T>(), ColumnExtra {
    val ormTableInfo = OrmTableInfo(clazz)
    val tableName = ormTableInfo.tableName
    val quotedTableName = db.quoteTableName(tableName)
    val hasExtrinsicData = ExtrinsicData::class.isSuperclassOf(clazz)
    val columns = ormTableInfo.columns
    val columnsByName = ormTableInfo.columnsByName
    fun getColumnByName(name: String) = ormTableInfo.getColumnByName(name)
    fun getColumnByProp(prop: KProperty1<T, *>) = ormTableInfo.getColumnByProp(prop)

    override val table: DbJdbcTable<T> get() = this
    override suspend fun query(sql: String, vararg params: Any?): DbResult = db.query(sql, *params)

    override suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R = db.transaction {
        callback(DbTableTransaction(this@DbJdbcTable, this))
    }

    fun toColumnMap(map: Map<String, Any?>): Map<ColumnDef<T>, Any?> {
        @Suppress("UNCHECKED_CAST")
        return map.entries
            .map { getColumnByName(it.key) to it.value }
            .filter { it.first != null }
            .toMap() as Map<ColumnDef<T>, Any?>
    }

    fun serializeColumn(value: Any?, column: ColumnDef<T>?, columnName: String = column?.name ?: "unknown"): Any? {
        return when (value) {
            null -> null
            is ByteArray -> value
            is String -> value
            is Number -> value
            is UUID -> value
            is Date -> java.sql.Date(value.time)
            else -> MiniJson.stringify(JsonTyper.untype(value))
        }
        //return value
    }

    fun deserializeColumn(value: Any?, column: ColumnDef<T>?, columnName: String = column?.name ?: "unknown"): Any? {
        return when (value) {
            is InputStream -> value.readBytes()
            is Blob -> value.binaryStream.readBytes()
            is UUID -> value
            is DbKey -> value
            is DbIntKey -> value
            is Timestamp -> Date(value.time)
            is Date -> value
            is LocalDate -> value
            //is Number -> (value as? Number) ?: value.toString().toDoubleOrNull()
            else -> when {
                column != null -> when (column.jclazz) {
                    String::class -> value.toString()
                    Number::class -> if (value is Number) value else value.toString().toDouble()
                    UUID::class -> value
                    DbRef::class -> value
                    DbKey::class -> value
                    DbIntRef::class -> value
                    DbIntKey::class -> value
                    Date::class -> value
                    LocalDate::class -> value
                    else -> {
                        val clazz = column.jclazz.java
                        if (clazz.isEnum && value is String) {
                            // @TODO: Cache
                            val constants = clazz.enumConstants.associateBy { it.toString().toUpperCase().trim() }
                            return constants[value.toUpperCase().trim()] ?: constants.values.first()
                        } else {
                            JsonTyper.type(MiniJson.parse(value.toString()) ?: Unit, column.jclazz)
                        }
                    }
                }
                else -> value
            }
        }
    }
}

class DbTableTransaction<T: DbTableBaseElement>(override val table: DbJdbcTable<T>, val transaction: DbBaseTransaction) : SqlTable<T>() {
    override val clazz: KClass<T> get() = table.clazz
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
        Date::class -> "TIMESTAMP"
        String::class -> {
            val maxLength = annotations.findAnnotation<DbMaxLength>()
            //if (maxLength != null) "VARCHAR(${maxLength.length})" else "TEXT"
            if (maxLength != null) "VARCHAR(${maxLength.length})" else "VARCHAR"
        }
        DbIntRef::class, DbIntKey::class -> "INTEGER"
        DbRef::class, DbKey::class -> "VARCHAR"
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

private val JsonTyper = Typer()
        .withTyperUntyper<Date>(
                typer = { it, type ->
                    when (it) {
                        is String -> SimpleDateFormat.getDateTimeInstance().parse(it.toString())
                        is Date -> it
                        else -> TODO()
                    }
                },
                untyper = {
                    SimpleDateFormat.getDateTimeInstance().format(it)
                }
        )

private val JdbcDbTyper = Typer()
        .withDbKeyTyperUntyper()
        .withTyperUntyper<UUID>(
                typer = { it, type ->
                    when (it) {
                        is ByteArray -> UUID.nameUUIDFromBytes(it)
                        is String -> UUID.fromString(it)
                        is UUID -> it
                        else -> UUID.randomUUID()
                    }
                },
                untyper = {
                    it.toString()
                }
        )
        .withTyperUntyper<Date>(
                typer = { it, type ->
                    when (it) {
                        is String -> Date(Timestamp.valueOf(it).time)
                        is LocalDate -> Date.from(it.atStartOfDay(ZoneId.systemDefault()).toInstant())
                        is Date -> it
                        else -> Date(0L)
                    }
                },
                untyper = {
                    Timestamp(it.time).toString()
                }
        )
        .withTyperUntyper<Timestamp>(
                typer = { it, type ->
                    when (it) {
                        is String -> Timestamp.valueOf(it)
                        is LocalDate -> Timestamp(Date.from(it.atStartOfDay(ZoneId.systemDefault()).toInstant()).time)
                        is Date -> Timestamp(it.time)
                        is Timestamp -> it
                        else -> Timestamp(0L)
                    }
                },
                untyper = {
                    it.toString()
                }
        )
        /*
        .withTyperUntyper<LocalDate>(
                typer = {
                    when (it) {
                        is String, is Date -> {
                            val date = if (it is String) timestampFormat.parse(it) else it as Date
                            date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
                        }
                        is LocalDate -> it
                        else -> LocalDate.EPOCH
                    }
                },
                untyper = {
                    timestampFormat.format(it)
                }
        )
        */
        .withTyperUntyper<ByteArray>(
                typer = { it, type ->
                    when (it) {
                        is Blob -> it.binaryStream.readBytes()
                        is InputStream -> it.readBytes()
                        is ByteArray -> it
                        else -> byteArrayOf()
                    }
                },
                untyper = {
                    it
                }
        )
