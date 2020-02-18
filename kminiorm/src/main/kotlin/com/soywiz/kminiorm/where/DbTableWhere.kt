package com.soywiz.kminiorm.where

import com.soywiz.kminiorm.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.ArrayList
import kotlin.reflect.*

data class DbTableWhere<T : DbTableBaseElement>(
    private val table: DbTable<T>
) : Flow<T> {
    @PublishedApi internal var fields: List<KProperty1<T, *>>? = null
    @PublishedApi internal var sorted: List<Pair<KProperty1<T, *>, Int>>? = null
    @PublishedApi internal var skip: Long? = null
    @PublishedApi internal var limit: Long? = null
    @PublishedApi internal val andClauses: ArrayList<DbQuery<T>> = arrayListOf()

    inline fun fields(vararg fields: KProperty1<T, *>) = this.apply { this.fields = fields.toList() }
    inline fun fields(fields: List<KProperty1<T, *>>) = this.apply { this.fields = fields }

    inline fun sorted(vararg sorted: Pair<KProperty1<T, *>, Int>) = this.apply { this.sorted = sorted.toList() }
    inline fun sorted(sorted: List<Pair<KProperty1<T, *>, Int>>) = this.apply { this.sorted = sorted }

    inline fun skip(count: Number) = this.apply { this.skip = count.toLong() }
    inline fun limit(count: Number) = this.apply { this.limit = count.toLong() }

    inline fun where(query: DbQueryBuilder<T>.() -> DbQuery<T>) = this.apply { this.andClauses += query(
        DbQueryBuilder.builder()
    )  }
    inline fun <R> eq(field: KProperty1<T, R>, value: R) = where { field eq value }
    inline fun <R> ne(field: KProperty1<T, R>, value: R) = where { field ne value }
    inline fun <R : Comparable<R>?> gt(field: KProperty1<T, R>, value: R) = where { field gt value }
    inline fun <R : Comparable<R>?> ge(field: KProperty1<T, R>, value: R) = where { field ge value }
    inline fun <R : Comparable<R>?> lt(field: KProperty1<T, R>, value: R) = where { field lt value }
    inline fun <R : Comparable<R>?> le(field: KProperty1<T, R>, value: R) = where { field le value }
    inline fun <R : Comparable<R>> between(field: KProperty1<T, R>, value: ClosedRange<R>) = where { field BETWEEN value }
    inline fun <R : Comparable<R>> IN(field: KProperty1<T, R>, values: List<R>) = where { field IN values }

    suspend fun find(): Flow<T> = table.findFlow(skip = skip, limit = limit, fields = fields, sorted = sorted, query = { AND(andClauses) })

    suspend operator fun iterator() = find().toList().iterator()
    @InternalCoroutinesApi
    override suspend fun collect(collector: FlowCollector<T>) = find().collect(collector)
}

val <T : DbTableBaseElement> DbTable<T>.where get() = DbTableWhere(this)

// @TODO: Can we do this without converting to list?
suspend operator fun <T> Flow<T>.iterator() = this.toList().listIterator()
