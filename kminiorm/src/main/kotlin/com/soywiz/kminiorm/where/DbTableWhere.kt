package com.soywiz.kminiorm.where

import com.soywiz.kminiorm.*
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
    fun fields(vararg fields: KProperty1<T, *>) = this.copy(fields = fields.toList())
    fun fields(fields: List<KProperty1<T, *>>) = this.copy(fields = fields.toList())

    fun sorted(vararg sorted: Pair<KProperty1<T, *>, Int>) = this.copy(sorted = sorted.toList())
    fun sorted(sorted: List<Pair<KProperty1<T, *>, Int>>) = this.copy(sorted = sorted.toList())

    inline fun skip(count: Number) = this.copy(skip = count.toLong())
    inline fun limit(count: Number) = this.copy(limit = count.toLong())

    inline fun chunkSize(chunkSize: Number) = this.copy(chunkSize = chunkSize.toInt())

    @PublishedApi
    internal val _andClauses get() = andClauses

    inline fun where(query: DbQueryBuilder<T>.() -> DbQuery<T>) = this.copy(andClauses = _andClauses + query(table.queryBuilder))
    inline fun <R> eq(field: KProperty1<T, R>, value: R) = where { field eq value }
    inline fun <R> ne(field: KProperty1<T, R>, value: R) = where { field ne value }
    inline fun <R : Comparable<R>?> gt(field: KProperty1<T, R>, value: R) = where { field gt value }
    inline fun <R : Comparable<R>?> ge(field: KProperty1<T, R>, value: R) = where { field ge value }
    inline fun <R : Comparable<R>?> lt(field: KProperty1<T, R>, value: R) = where { field lt value }
    inline fun <R : Comparable<R>?> le(field: KProperty1<T, R>, value: R) = where { field le value }
    inline fun <R : Comparable<R>> between(field: KProperty1<T, R>, value: ClosedRange<R>) = where { field BETWEEN value }
    inline fun <R : Comparable<R>> IN(field: KProperty1<T, R>, values: List<R>) = where { field IN values }

    private var flowCache: Flow<T>? = null
    private val flowLock = Mutex()

    suspend fun findFlow(): Flow<T> = flowLock.withLock {
        if (flowCache == null) {
            val chunkSize = chunkSize ?: 16
            flowCache = table.findChunked(
                skip = skip, limit = limit,
                fields = fields, sorted = sorted,
                chunkSize = chunkSize,
                query = { if (andClauses.isEmpty()) everything else AND(andClauses) }
            ).buffer(chunkSize)
        }
        flowCache!!
    }

    private var listCache: List<T>? = null
    private val listLock = Mutex()
    suspend fun find(): List<T> = listLock.withLock {
        if (listCache == null) {
            listCache = findFlow().toList()
        }
        listCache!!
    }

    // @TODO: Don't do this, since we cannot use it on big sequences
    //suspend operator fun iterator() = find().toList().iterator()

    @InternalCoroutinesApi
    override suspend fun collect(collector: FlowCollector<T>) = findFlow().collect(collector)
}

val <T : DbTableBaseElement> DbTable<T>.where get() = DbTableWhere(this)

// @TODO: Can we do this without converting to list?
//@Deprecated("")
//suspend operator fun <T> Flow<T>.iterator() = this.toList().listIterator()
