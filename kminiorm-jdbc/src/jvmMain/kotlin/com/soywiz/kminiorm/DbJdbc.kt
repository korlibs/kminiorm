package com.soywiz.kminiorm

import com.soywiz.kminiorm.dialect.SqlDialect
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
import java.util.concurrent.atomic.*
import kotlin.coroutines.*
import kotlin.reflect.*
import kotlin.reflect.full.*

private val _DEBUG_JDBC = System.getenv("DEBUG_JDBC") == "true"
private val _DEBUG_JDBC_RESULTS = System.getenv("DEBUG_JDBC_RESULTS") == "true"
private val PRE_DEBUG_SQL = System.getenv("PRE_DEBUG_SQL") == "true"

class WrappedConnection(val connectionIndex: Int, val connectionStr: String, val user: String?, val pass: String?, val timeout: Duration) {
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

class JdbcDbConnectionContext(val connection: WrappedConnection) : AbstractCoroutineContextElement(JdbcDbConnectionContext.Key) {
    companion object Key : CoroutineContext.Key<JdbcDbConnectionContext>
}

//class Db(val connection: String, val user: String, val pass: String, val dispatcher: CoroutineDispatcher = Dispatchers.IO) : DbQueryable {
class JdbcDb(
        val connection: String,
        val user: String? = null, val pass: String? = null,
        override val dispatcher: CoroutineContext = Dispatchers.IO,
        val typer: Typer = JdbcDbTyper,
        override val debugSQL: Boolean = false,
        dialect: SqlDialect = SqlDialect.ANSI,
        override val async: Boolean = true,
        val maxConnections: Int = 8,
        val ignoreInitErrors: Boolean = false,
    // MySQL default timeout is 8 hours
        val connectionTimeout: Duration = Duration.ofHours(1L) // Reuse this connection during 1 hour, then reconnect
) : AbstractDb(dialect), DbBase, DbQuoteable by dialect {
    val reallyDebugSQL get() = _DEBUG_JDBC || debugSQL
    val reallyDebugSQLResults  get() = _DEBUG_JDBC_RESULTS

    override fun <T : DbTableBaseElement> constructTable(clazz: KClass<T>): DbTable<T> = DbJdbcTable(this, clazz)

    private var connectionIndex = AtomicInteger()
    private fun getConnection(): WrappedConnection = WrappedConnection(connectionIndex.getAndIncrement(), connection, user, pass, connectionTimeout)

    @PublishedApi
    internal val connectionPool = InternalDbPool(maxConnections) {
        if (async) withContext(dispatcher) { getConnection() } else getConnection()
    }

    suspend fun <R> ensureSameConnection(block: suspend (WrappedConnection) -> R): R {
        @Suppress("LiftReturnOrAssignment") // Crashes
        val con = coroutineContext[JdbcDbConnectionContext.Key]
        if (con != null) {
            return block(con.connection)
        } else {
            return connectionPool.take { conn ->
                withContext(JdbcDbConnectionContext(conn)) {
                    block(conn)
                }
            }
        }
    }

    override suspend fun <R> transaction(name: String, callback: suspend DbBaseTransaction.() -> R): R {
        if (reallyDebugSQL) println("TRANSACTION START ($name)")
        val startTime = System.currentTimeMillis()
        return ensureSameConnection {
            try {
                val tr = JdbcTransaction(this, it)
                tr.run {
                    try {
                        callback(tr)
                                .also {
                                    this@JdbcDb.commit()
                                    if (reallyDebugSQL) {
                                        val time = System.currentTimeMillis() - startTime
                                        println("TRANSACTION COMMIT[${time}ms]")
                                    }
                                }
                    } catch (e: Throwable) {
                        this@JdbcDb.rollback()
                        if (reallyDebugSQL) {
                            val time = System.currentTimeMillis() - startTime
                            println("TRANSACTION ROLLBACK[${time}ms]: ${e.message}")
                        }
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

    override suspend fun query(@Language("SQL") sql: String, vararg params: Any?) = transaction("QUERY $sql") { query(sql, *params) }
    override suspend fun multiQuery(@Language("SQL") sql: String, paramsList: List<Array<out Any?>>): DbResult = transaction("MQUERY $sql") { multiQuery(sql, paramsList) }
}


class JdbcTransaction(override val db: JdbcDb, val wrappedConnection: WrappedConnection) : DbBaseTransaction {
    val connection get() = wrappedConnection.connection

    override suspend fun DbBase.commit(): Unit = if (db.async) {
        withContext(context = dispatcher) { this@JdbcTransaction.connection.commit() }
    } else {
        this@JdbcTransaction.connection.commit()
    }

    override suspend fun DbBase.rollback(): Unit = if (db.async) {
        withContext(dispatcher) { this@JdbcTransaction.connection.rollback() }
    } else {
        this@JdbcTransaction.connection.rollback()
    }

    companion object {
        val WRITE_QUERIES = listOf("UPDATE", "INSERT", "CREATE", "DELETE", "UPSERT", "ALTER")
    }

    @Suppress("ComplexRedundantLet")
    internal fun _query(sql: String, paramsList: List<Array<out Any?>>): DbResult {
        if (paramsList.isEmpty()) error("Query doesn't have parameters")
        val isWrite = WRITE_QUERIES.any { sql.startsWith(it, ignoreCase = true) }
        val statement = connection.prepareStatement(sql)
        return try {
            val hasMultiple = paramsList.size > 1
            for (params in paramsList) {
                for (index in params.indices) {
                    val param = params[index]
                    if (param is ByteArray) {
                        //statement.setBlob(index + 1, param.inputStream())
                        statement.setBytes(index + 1, param)
                    } else {
                        statement.setObject(index + 1, param)
                    }
                }
                if (isWrite && hasMultiple) {
                    statement.addBatch()
                } else {
                    break
                }
            }
            val map: List<Map<String, Any?>> = when {
                isWrite -> {
                    val updateCount: Int = if (hasMultiple) statement.executeBatch().sum() else statement.executeUpdate()
                    listOf(mapOf("updateCount" to updateCount.toLong()))
                }
                else -> {
                    val resultSet = statement.executeQuery()
                    try {
                        resultSet.toListMap()
                    } finally {
                        resultSet.close()
                    }
                }
            }
            JdbcDbResult(statement.largeUpdateCountSafe, map).also { result ->
                if (db.reallyDebugSQLResults) println(" --> $result")
            }
        } finally {
            statement.close()
        }
    }

    val Statement.largeUpdateCountSafe: Long get() = kotlin.runCatching { largeUpdateCount }.getOrNull() ?: kotlin.runCatching { updateCount.toLong() }.getOrNull() ?: 0L

    override suspend fun query(sql: String, vararg params: Any?): DbResult {
        return multiQuery(sql, listOf(params))
    }

    override suspend fun multiQuery(sql: String, paramsList: List<Array<out Any?>>): DbResult {
        val startTime = System.currentTimeMillis()
        var results: DbResult? = null
        if (PRE_DEBUG_SQL) println("QUERY[${wrappedConnection.connectionIndex}] : $sql, ${paramsList.map { it.toList() }}")

        try {
            results = if (db.async) withContext(db.dispatcher) { _query(sql, paramsList) } else _query(sql, paramsList)
            return results
        } finally {
            val time = System.currentTimeMillis() - startTime
            if (db.reallyDebugSQL) println("QUERY[${wrappedConnection.connectionIndex}][${time}ms] : $sql, ${paramsList.map { it.toList() }} --> ${results?.size}")
        }
    }
}

abstract class SqlTable<T : DbTableBaseElement> : AbstractDbTable<T>(), DbQueryable, ColumnExtra {
    abstract val table: DbJdbcTable<T>
    override val db: JdbcDb get() = table.db
    override val typer get() = table.db.typer
    private val dialect get() = db.dialect
    private val _quotedTableName get() = table.quotedTableName

    override suspend fun showColumns(): Map<String, IColumnDef> {
        return dialect.showColumns(db, table.tableName).map { it.name to it }.toMap()
    }

    @OptIn(ExperimentalStdlibApi::class)
    override suspend fun initialize(): DbTable<T> = this.apply {
        query(dialect.sqlCreateTable(typer, table.tableName, table.columnsWithExtrinsic))
        try {
            val oldColumns = showColumns()
            //println("oldColumns: $oldColumns")
            for (column in table.columns) {
                if (column.name in oldColumns) continue // Do not add columns if they already exists

                val result = kotlin.runCatching {
                    //transaction { // @TODO: This seems to fail on sqlite atleast
                        query(dialect.sqlAlterTableAddColumn(typer, table.tableName, column))
                        for (migrationAnnotation in column.migrations) {
                            val instance = migrationAnnotation.migration.createInstance() as DbMigration<T>
                            instance.migrate(table, DbMigration.Action.ADD_COLUMN, column)
                        }
                    //}
                }
                if (result.isFailure) {
                    if (db.ignoreInitErrors) {
                        result.exceptionOrNull()?.printStackTrace()
                    } else {
                        throw result.exceptionOrNull()!!
                    }
                    //println(result.exceptionOrNull())
                }
            }
        } catch (e: Throwable) {
            e.printStackTrace()
        }

        if (table.hasExtrinsicData) {
            val result = kotlin.runCatching {
                query(dialect.sqlAlterTableAddColumn(typer, table.tableName, EXTRINSIC_COLUMN))
            }
            if (result.isFailure) {
                //throw result.exceptionOrNull()!!
                println(result.exceptionOrNull())
            }
        }

        for ((indexName, columns) in table.ormTableInfo.columnIndices) {
            //println("$column: ${column.quotedName}: ${column.isUnique}, ${column.isIndex}")
            val unique = columns.any { it.isUnique }
            val primary = columns.any { it.isPrimary }
            val result = kotlin.runCatching {
                query(dialect.sqlAlterCreateIndex(
                        when {
                            primary -> SqlDialect.IndexType.PRIMARY
                            unique -> SqlDialect.IndexType.UNIQUE
                            else -> SqlDialect.IndexType.INDEX
                        },
                        table.tableName,
                        columns,
                        indexName
                ))
            }
            if (result.isFailure) {
                //throw result.exceptionOrNull()!!
                println(result.exceptionOrNull())
            }
        }
    }

    suspend fun _insert(data: List<Map<String, Any?>>, onConflict: DbOnConflict): DbInsertResult<T> {
        try {
            val columns = data.first().keys
            val dataList = columns.map { column -> column to data.map { it[column] } }.toMap()
            val entries = table.toColumnMap(dataList, skipOnInsert = true).entries

            val keys: MutableList<IColumnDef> = entries.map { it.key }.toMutableList()
            if (table.hasExtrinsicData) keys += EXTRINSIC_COLUMN

            val valuesList = arrayListOf<Array<out Any?>>()

            val insertInfo = dialect.sqlInsert(table.tableName, ormTableInfo, keys, onConflict)

            for (n in 0 until data.size) {
                val rdata = data[n]
                val values = entries.map { table.serializeColumn(it.value[n], it.key) }.toMutableList()
                if (table.hasExtrinsicData) {
                    values += MiniJson.stringify(rdata.filter { table.getColumnByName(it.key) == null })
                }
                assert(keys.size == values.size)
                val zvalues = arrayListOf<Any?>()
                repeat(insertInfo.repeatCount) {
                    zvalues.addAll(values)
                }
                valuesList += zvalues.toTypedArray()
            }

            return db.ensureSameConnection {
                val result = multiQuery(insertInfo.sql, valuesList)
                var lastInsertId: Long? = null
                //println("RESULT: $result")
                val insertCount = result.extractNumber()?.toLong() ?: -1L
                if (dialect.supportsLastInsertId) {
                    val rowIdResult = query(dialect.lastInsertId())
                    //println("ROWID_RESULT: $rowIdResult")
                    lastInsertId = rowIdResult.extractNumber()?.toLong()
                }
                DbInsertResult<T>(
                    lastInsertId = lastInsertId,
                    lastKey = null,
                    insertCount = insertCount,
                    result = result,
                    instanceOrNull = null
                )
            }
        } catch (e: Throwable) {
            throw dialect.transformException(e)
        }
    }

    override suspend fun insert(data: Map<String, Any?>): DbInsertResult<T> {
        return _insert(listOf(data), onConflict = DbOnConflict.ERROR)
    }

    override suspend fun insert(instances: List<T>, onConflict: DbOnConflict) {
        if (dialect.supportExtendedInsert) {
            _insert(instances.map { JdbcDbTyper.untype(it) as Map<String, Any?> }, onConflict)
        } else {
            super.insert(instances, onConflict)
        }
    }

    override suspend fun findFlowPartial(skip: Long?, limit: Long?, fields: List<KProperty1<T, *>>?, sorted: List<Pair<KProperty1<T, *>, Int>>?, query: DbQueryBuilder<T>.(T) -> DbQuery<T>): Flow<Partial<T>> {
        val rquery = queryBuilder.buildOrNull(query) ?: return listOf<Partial<T>>().asFlow()
        val params = arrayListOf<Any?>()
        val queryStr = buildString {
            append("SELECT ")
            if (fields != null) {
                append(fields.mapNotNull { table.getColumnByProp(it) }.joinToString(", ") { it.quotedName })
            } else {
                append("*")
            }
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(rquery.toString(db, params))
            if (sorted != null && sorted.isNotEmpty()) {
                val orders = sorted.map { table.getColumnByProp(it.first)!!.quotedName + when { it.second > 0 -> " ASC"; it.second < 0 -> " DESC"; else -> "" } }
                append(" ORDER BY ${orders.joinToString(", ")}")
            }
            if (limit != null || skip != null) {
                append(" LIMIT ${limit ?: Int.MAX_VALUE}")
                if (skip != null) append(" OFFSET $skip")
            }
            append(";")
        }
        // @TODO: Can we optimize this by streaming results?
        return query(queryStr, *params.toTypedArray())
                .map { Partial(it.mapValues { (key, value) -> table.deserializeColumn(value, table.getColumnByName(key), key) }, table.clazz) }
                .asFlow()
    }

    override suspend fun count(query: DbQueryBuilder<T>.(T) -> DbQuery<T>): Long {
        val rquery = queryBuilder.buildOrNull(query) ?: return 0L
        val params = arrayListOf<Any?>()
        val queryStr = buildString {
            append("SELECT ")
            append("COUNT(*)")
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(rquery.toString(db, params))
            append(";")
        }
        val data = query(queryStr, *params.toTypedArray())
        // @TODO: Can we optimize this by streaming results?
        return data.first().values.first().toString().toLong()
    }

    override suspend fun <R> countGrouped(groupedBy: KProperty1<T, R>, query: DbQueryBuilder<T>.(T) -> DbQuery<T>): Map<R, Long> {
        val rquery = queryBuilder.buildOrNull(query) ?: return mapOf()
        val keyAlias = "__k1"
        val countAlias = "__c1"
        val groupedByCol = table.getColumnByProp(groupedBy) ?: error("Can't find column '$groupedBy'")
        val groupColumnQuotedName = groupedByCol.quotedName
        val params = arrayListOf<Any?>()
        val queryStr = buildString {
            append("SELECT ")
            append(groupColumnQuotedName)
            append(" AS $keyAlias, ")
            append("COUNT(*) AS $countAlias")
            append(" FROM ")
            append(_quotedTableName)
            append(" WHERE ")
            append(rquery.toString(db, params))
            append(" GROUP BY ")
            append(groupColumnQuotedName)
            append(";")
        }
        val data = query(queryStr, *params.toTypedArray())
        // @TODO: Can we optimize this by streaming results?
        return data
            .associate { (table.deserializeColumn(it[keyAlias], groupedByCol, keyAlias) as R) to it[countAlias].toString().toLong() }
    }

    override suspend fun update(set: Partial<T>?, increment: Partial<T>?, limit: Long?, query: DbQueryBuilder<T>.(T) -> DbQuery<T>): Long {
        val table = this.table
        val setEntries = (set?.let { table.toColumnMap(it.data, skipOnInsert = true).entries } ?: setOf()).toList()
        val incrEntries = (increment?.let { table.toColumnMap(it.data, skipOnInsert = true).entries } ?: setOf()).toList()

        // Can't make an UPDATE query that does nothing
        if (setEntries.isEmpty() && incrEntries.isEmpty()) return 0L

        val values = (setEntries + incrEntries).map { table.serializeColumn(it.value, it.key) }

        val params = arrayListOf<Any?>()
        val queryStr = buildString {
            append("UPDATE ")
            append(table.quotedTableName)
            append(" SET ")
            append((setEntries.map { "${it.key.quotedName}=?" } + incrEntries.map { "${it.key.quotedName}=${it.key.quotedName}+?" }).joinToString(", "))
            append(" WHERE ")
            append(queryBuilder.build(query).toString(db, params))
            if (limit != null) append(" LIMIT $limit")
            append(";")
        }
        return query(queryStr, *values.toTypedArray(), *params.toTypedArray()).updateCount
    }

    override suspend fun delete(limit: Long?, query: DbQueryBuilder<T>.(T) -> DbQuery<T>): Long {
        val params = arrayListOf<Any?>()
        val queryStr = dialect.sqlDelete(table.tableName, queryBuilder.build(query), params, limit)
        return query(queryStr, *params.toTypedArray()).updateCount
    }
}

interface ColumnExtra {
    val db: DbBase
    val <T : Any> ColumnDef<T>.quotedName get() = db.quoteColumnName(name)
    val <T : Any> ColumnDef<T>.sqlType get() = dialect.toSqlType(property)
}

class DbJdbcTable<T : DbTableBaseElement>(override val db: JdbcDb, override val clazz: KClass<T>) : SqlTable<T>(), ColumnExtra {
    override val ormTableInfo = OrmTableInfo(db.dialect, clazz)
    val tableName = ormTableInfo.tableName
    val quotedTableName = db.quoteTableName(tableName)
    val hasExtrinsicData = ExtrinsicData::class.isSuperclassOf(clazz)
    val columns = ormTableInfo.columns
    val columnsWithExtrinsic: List<IColumnDef> = columns + if (hasExtrinsicData) listOf(EXTRINSIC_COLUMN) else listOf()
    val columnsByName = ormTableInfo.columnsByName
    fun getColumnByName(name: String) = ormTableInfo.getColumnByName(name)
    fun getColumnByProp(prop: KProperty1<T, *>) = ormTableInfo.getColumnByProp(prop)

    override val table: DbJdbcTable<T> get() = this
    override suspend fun query(sql: String, vararg params: Any?): DbResult = db.query(sql, *params)
    override suspend fun multiQuery(sql: String, paramsList: List<Array<out Any?>>): DbResult = db.multiQuery(sql, paramsList)

    override suspend fun <R> transaction(name: String, callback: suspend DbTable<T>.() -> R): R = db.transaction(name) {
        callback(DbTableTransaction(name, this@DbJdbcTable, this))
    }

    override suspend fun <R> ensureSameConnection(callback: suspend DbTable<T>.() -> R): R =
        db.ensureSameConnection { callback() }

    fun <R> toColumnMap(map: Map<String, R>, skipOnInsert: Boolean): Map<ColumnDef<T>, R> {
        @Suppress("UNCHECKED_CAST")
        return map.entries
            .map { getColumnByName(it.key) to it.value }
            .filter { it.first != null }
            .toMap()
            .filterKeys { !skipOnInsert || it?.skipOnInsert != true }
            as Map<ColumnDef<T>, R>
    }

    fun serializeColumn(value: Any?, column: ColumnDef<T>?, columnName: String = column?.name ?: "unknown"): Any? {
        return when (value) {
            null -> null
            is ByteArray -> value
            is String -> value
            is Number -> value
            is UUID -> value
            //is Date -> java.sql.Date(value.time)
            is java.sql.Date -> value
            is java.util.Date -> Timestamp(value.time)
			is DbKey -> value.toHexString()
            else -> MiniJson.stringify(JsonTyper.untype(value))
        }
        //return value
    }

    fun deserializeColumn(value: Any?, column: ColumnDef<T>?, columnName: String = column?.name ?: "unknown"): Any? {
        return when (value) {
            is InputStream -> value.readBytes()
            is Blob -> value.binaryStream.readBytes()
            is ByteArray -> value
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

class DbTableTransaction<T: DbTableBaseElement>(
    val name: String,
    override val table: DbJdbcTable<T>,
    val transaction: DbBaseTransaction
) : SqlTable<T>() {
    override val ormTableInfo: OrmTableInfo<T> get() = table.ormTableInfo
    override val clazz: KClass<T> get() = table.clazz
    override val db get() = table.db

    override suspend fun <R> transaction(name: String, callback: suspend DbTable<T>.() -> R): R = db.transaction(name) {
        callback(DbTableTransaction(name, this@DbTableTransaction.table, this))
    }

    override suspend fun query(sql: String, vararg params: Any?): DbResult = transaction.query(sql, *params)
    override suspend fun multiQuery(@Language("SQL") sql: String, paramsList: List<Array<out Any?>>): DbResult = transaction.multiQuery(sql, paramsList)
}

class JdbcDbResult(
    override val updateCount: Long,
    override val data: List<Map<String, Any?>>
) : DbResult, List<Map<String, Any?>> by data {
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
                        is Timestamp -> Date(it.time)
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
