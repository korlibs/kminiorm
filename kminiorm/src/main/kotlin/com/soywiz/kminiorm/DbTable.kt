package com.soywiz.kminiorm

import kotlin.reflect.*

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
    // C
    suspend fun insert(instance: T): T
    suspend fun insert(instance: Partial<T>): DbResult = insert(instance.data)
    suspend fun insert(data: Map<String, Any?>): DbResult
    // R
    suspend fun find(skip: Long? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Iterable<T>
    suspend fun findAll(skip: Long? = null, limit: Long? = null): Iterable<T> = find(skip = skip, limit = limit)
    suspend fun findOne(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): T? = find(query = query, limit = 1).firstOrNull()
    // U
    suspend fun update(set: Partial<T>? = null, increment: Partial<T>? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long
    // D
    suspend fun delete(limit: Long? = 1L, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Long

    suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R
    companion object { }
}

suspend fun <T : DbTableElement> DbTable<T>.findById(id: DbKey): T? = findOne { DbQuery.BinOp(DbModel::_id as KProperty1<T, DbKey>, id, DbQueryBinOp.EQ) }
