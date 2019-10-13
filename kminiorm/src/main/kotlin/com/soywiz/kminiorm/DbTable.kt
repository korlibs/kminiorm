package com.soywiz.kminiorm

import kotlin.reflect.*

interface DbModel {
    companion object { }
    @DbPrimary
    val _id: DbKey

    open class Base(override val _id: DbKey = DbKey()) : DbModel
    open class BaseWithExtrinsic(override val _id: DbKey = DbKey()) : DbModel, ExtrinsicData by ExtrinsicData.Mixin()
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
    suspend fun find(skip: Long? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): List<T>
    suspend fun findAll(skip: Long? = null, limit: Long? = null): List<T> = find(skip = skip, limit = limit)
    suspend fun findOne(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): T? = find(query = query, limit = 1).firstOrNull()
    // U
    suspend fun update(set: Partial<T>? = null, increment: Partial<T>? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long
    suspend fun upsert(instance: T): T {
        val info = OrmTableInfo(instance::class)
        val props = info.columns
                .filter { (it.isUnique || it.isPrimary) && (it.property.name != DbModel::_id.name) }
                .map { it.property }
                .toTypedArray() as Array<KProperty1<T, *>>
        return upsertWithProps(instance, *props)
    }
    suspend fun upsertWithProps(instance: T, vararg props: KProperty1<T, *>): T {
        if (props.isEmpty()) error("Must specify keys for the upsert")

        val instancePartial = Partial(instance, instance::class)

        try {
            return insert(instance)
        } catch (e: DuplicateKeyDbException) {
            val query = DbQueryBuilder.build<T> {
                var out: DbQuery<T> = nothing
                for (prop in props) {
                    val value = instancePartial[prop]
                    val step = (prop eq value)
                    out = if (out == nothing) step else out AND step
                }
                out
            }
            val partial = Partial(instance, instance::class)
                    .without(*props)
                    .without(DbModel::_id as KProperty1<T, DbKey>) // If it exists, let's keep its _id

            //println("query: $query")
            //println("partial: $partial")

            update(partial, query = { query })

            return findOne { query } ?: instance.also {
                System.err.println("Couldn't find the updated instance, and found an error while inserting:")
                e.printStackTrace()
            }
        }
    }
    // D
    suspend fun delete(limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Long

    suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R
    companion object { }
}

suspend fun <T : DbTableElement> DbTable<T>.findById(id: DbKey): T? = findOne { DbQuery.BinOp(DbModel::_id as KProperty1<T, DbKey>, id, DbQueryBinOp.EQ) }
suspend fun <T : DbTableElement> DbTable<T>.findOrCreate(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }, build: () -> T): T {
    return findOne(query) ?: build().also { insert(it) }
}
