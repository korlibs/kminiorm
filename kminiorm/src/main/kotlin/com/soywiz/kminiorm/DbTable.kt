package com.soywiz.kminiorm

interface DbModel {
    companion object { }
    @DbPrimary
    val _id: DbKey

    open class Base(override val _id: DbKey = DbKey()) : DbModel
}


typealias DbTableElement = DbModel
//typealias DbTableElement = Any

//interface DbTable<T : Any> {
interface DbTable<T : DbTableElement> {
    suspend fun showColumns(): Map<String, Map<String, Any?>>
    suspend fun initialize(): DbTable<T>
    suspend fun insert(instance: T): T
    suspend fun insert(instance: Partial<T>): DbResult = insert(instance.data)
    suspend fun insert(data: Map<String, Any?>): DbResult
    suspend fun find(skip: Long? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Iterable<T>
    suspend fun findAll(skip: Long? = null, limit: Long? = null): Iterable<T> = find(skip = skip, limit = limit)
    suspend fun findOne(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): T? = find(query = query, limit = 1).firstOrNull()
    suspend fun update(set: Partial<T>, increment: Partial<T>? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long
    suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R
    companion object { }
}
