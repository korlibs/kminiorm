package com.soywiz.kminiorm.where

import com.soywiz.kminiorm.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.*
import kotlin.reflect.*

data class DbTableWhere<T : DbTableBaseElement>(
    private val table: DbTable<T>,
    private val fields: List<KProperty1<T, *>>? = null,
    private val sorted: List<Pair<KProperty1<T, *>, Int>>? = null,
    private val skip: Long? = null,
    private val limit: Long? = null,
    private val andClauses: List<DbQuery<T>> = emptyList()
) : Flow<T> {
    fun fields(vararg fields: KProperty1<T, *>) = this.copy(fields = fields.toList())
    fun fields(fields: List<KProperty1<T, *>>) = this.copy(fields = fields.toList())

    fun sorted(vararg sorted: Pair<KProperty1<T, *>, Int>) = this.copy(sorted = sorted.toList())
    fun sorted(sorted: List<Pair<KProperty1<T, *>, Int>>) = this.copy(sorted = sorted.toList())

    inline fun skip(count: Number) = this.copy(skip = count.toLong())
    inline fun limit(count: Number) = this.copy(limit = count.toLong())

    @PublishedApi
    internal val _andClauses get() = andClauses

    inline fun where(query: DbQueryBuilder<T>.() -> DbQuery<T>) = this.copy(andClauses = _andClauses + query(DbQueryBuilder.builder()))
    inline fun <R> eq(field: KProperty1<T, R>, value: R) = where { field eq value }
    inline fun <R> ne(field: KProperty1<T, R>, value: R) = where { field ne value }
    inline fun <R : Comparable<R>?> gt(field: KProperty1<T, R>, value: R) = where { field gt value }
    inline fun <R : Comparable<R>?> ge(field: KProperty1<T, R>, value: R) = where { field ge value }
    inline fun <R : Comparable<R>?> lt(field: KProperty1<T, R>, value: R) = where { field lt value }
    inline fun <R : Comparable<R>?> le(field: KProperty1<T, R>, value: R) = where { field le value }
    inline fun <R : Comparable<R>> between(field: KProperty1<T, R>, value: ClosedRange<R>) = where { field BETWEEN value }
    inline fun <R : Comparable<R>> IN(field: KProperty1<T, R>, values: List<R>) = where { field IN values }

    private var findCache: List<T>? = null
    private val lock = Mutex()
    suspend fun find(): List<T> = lock.withLock {
        if (findCache == null) {
            findCache = table.find(skip = skip, limit = limit, fields = fields, sorted = sorted, query = { AND(andClauses) })
        }
        findCache!!
    }

    suspend operator fun iterator() = find().toList().iterator()
    @InternalCoroutinesApi
    override suspend fun collect(collector: FlowCollector<T>) = find().asFlow().collect(collector)
}

val <T : DbTableBaseElement> DbTable<T>.where get() = DbTableWhere(this)

// @TODO: Can we do this without converting to list?
//@Deprecated("")
//suspend operator fun <T> Flow<T>.iterator() = this.toList().listIterator()
