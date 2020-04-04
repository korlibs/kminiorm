package com.soywiz.kminiorm.where

import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.util.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.*
import kotlin.reflect.*

data class DbTableWhere<T : DbTableBaseElement>(
    @PublishedApi
    internal val table: DbTable<T>,
    private val fields: List<KProperty1<T, *>>? = null,
    private val sorted: List<Pair<KProperty1<T, *>, Int>>? = null,
    private val skip: Long? = null,
    private val limit: Long? = null,
    private val chunkSize: Int? = null,
    private val andClauses: List<DbQuery<T>> = emptyList()
) : Flow<T> {
    @PublishedApi
    internal val dummyInstance get() = table.dummyInstance
    @PublishedApi
    internal val <R> KProperty<R>.prop get() = table.getProperty(this)

    fun setFields(vararg fields: KProperty1<T, *>) = this.copy(fields = fields.toList())
    fun setFields(fields: List<KProperty1<T, *>>) = this.copy(fields = fields.toList())
    fun setFields(fields: (T) -> Iterable<KProperty0<*>>) = this.copy(fields = fields(dummyInstance).map { it.prop })

    fun fields(vararg fields: KProperty1<T, *>) = this.copy(fields = (this.fields ?: emptyList()) + fields.toList())
    fun fields(fields: List<KProperty1<T, *>>) = this.copy(fields = (this.fields ?: emptyList()) + fields.toList())
    fun field(fields: (T) -> KProperty0<*>) = this.copy(fields = (this.fields ?: emptyList()) + fields(dummyInstance).prop)
    fun fields(fields: (T) -> Iterable<KProperty0<*>>) = this.copy(fields = (this.fields ?: emptyList()) + fields(dummyInstance).map { it.prop })

    fun setSorted(vararg sorted: Pair<KProperty1<T, *>, Int>) = this.copy(sorted = sorted.toList())
    fun setSorted(sorted: List<Pair<KProperty1<T, *>, Int>>) = this.copy(sorted = sorted.toList())
    fun setSorted(sorted: (T) -> Pair<KProperty0<*>, Int>) = sorted(dummyInstance).let { this.setSorted(it.first.prop to it.second) }

    fun sorted(vararg sorted: Pair<KProperty1<T, *>, Int>) = this.copy(sorted = (this.sorted ?: emptyList()) + sorted.toList())
    fun sorted(sorted: List<Pair<KProperty1<T, *>, Int>>) = this.copy(sorted = (this.sorted ?: emptyList()) + sorted.toList())
    fun sorted(sorted: (T) -> Pair<KProperty0<*>, Int>) = sorted(dummyInstance).let { this.sorted(it.first.prop to it.second) }

    inline fun skip(count: Number) = this.copy(skip = count.toLong())
    inline fun limit(count: Number) = this.copy(limit = count.toLong())

    inline fun chunkSize(chunkSize: Number) = this.copy(chunkSize = chunkSize.toInt())

    @PublishedApi
    internal val _andClauses get() = andClauses

    inline fun setWhere(query: DbQueryBuilder<T>.(T) -> DbQuery<T>) = this.copy(andClauses = listOf(query(table.queryBuilder, table.dummyInstance)))
    inline fun where(query: DbQueryBuilder<T>.(T) -> DbQuery<T>) = this.copy(andClauses = _andClauses + query(table.queryBuilder, table.dummyInstance))

    @Deprecated("Use where instead")
    inline fun <R> eq(field: KProperty1<T, R>, value: R) = where { field eq value }
    @Deprecated("Use where instead")
    inline fun <R> ne(field: KProperty1<T, R>, value: R) = where { field ne value }
    @Deprecated("Use where instead")
    inline fun <R : Comparable<R>?> gt(field: KProperty1<T, R>, value: R) = where { field gt value }
    @Deprecated("Use where instead")
    inline fun <R : Comparable<R>?> ge(field: KProperty1<T, R>, value: R) = where { field ge value }
    @Deprecated("Use where instead")
    inline fun <R : Comparable<R>?> lt(field: KProperty1<T, R>, value: R) = where { field lt value }
    @Deprecated("Use where instead")
    inline fun <R : Comparable<R>?> le(field: KProperty1<T, R>, value: R) = where { field le value }
    @Deprecated("Use where instead")
    inline fun <R : Comparable<R>> between(field: KProperty1<T, R>, value: ClosedRange<R>) = where { field BETWEEN value }
    @Deprecated("Use where instead")
    inline fun <R : Comparable<R>> IN(field: KProperty1<T, R>, values: List<R>) = where { field IN values }

    private var flowCache: Flow<T>? = null
    private val flowLock = Mutex()

    suspend fun countRows(): Long =
        (table.count(query = FINAL_QUERY) - (skip ?: 0L)).coerceIn(0L, limit ?: Long.MAX_VALUE)

    suspend fun count(): Long = countRows()

    val FINAL_QUERY by lazy {
        val v: DbQueryBuilder<T>.(T) -> DbQuery<T> = { if (andClauses.isEmpty()) everything else AND(andClauses) }
        v
    }

    @PublishedApi
    internal val finalChunkSize get() = chunkSize ?: 16

    suspend fun findFlow(): Flow<T> = flowLock.withLock {
        if (flowCache == null) {
            val chunkSize = finalChunkSize
            flowCache = table.findChunked(
                skip = skip, limit = limit,
                fields = fields, sorted = sorted,
                chunkSize = chunkSize,
                query = FINAL_QUERY
            ).buffer(chunkSize)
        }
        flowCache!!
    }

    suspend fun findOne(): T? = find().firstOrNull()

    suspend fun delete() {
        table.delete(limit = limit, query = FINAL_QUERY)
    }

    suspend fun findFlowChunked(): Flow<List<T>> = findFlow().chunked(finalChunkSize)

    private var listCache: List<T>? = null
    private val listLock = Mutex()
    suspend fun find(): List<T> = listLock.withLock {
        if (listCache == null) {
            listCache = table.find(skip = skip, limit = limit, fields = fields, sorted = sorted, query = FINAL_QUERY)
        }
        listCache!!
    }

    // @TODO: Don't do this, since we cannot use it on big sequences
    //suspend operator fun iterator() = find().toList().iterator()

    @InternalCoroutinesApi
    override suspend fun collect(collector: FlowCollector<T>) = findFlow().collect(collector)
}

val <T : DbTableBaseElement> DbTable<T>.where get() = DbTableWhere(this)
fun <T : DbTableBaseElement> DbTable<T>.where(query: DbQueryBuilder<T>.(T) -> DbQuery<T>) = where.where(query)

// @TODO: Can we do this without converting to list?
//@Deprecated("")
//suspend operator fun <T> Flow<T>.iterator() = this.toList().listIterator()
