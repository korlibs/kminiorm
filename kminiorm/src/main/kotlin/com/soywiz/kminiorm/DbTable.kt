package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.flow.*
import kotlin.reflect.*

interface DbBaseModel {
    abstract class Abstract : DbBaseModel {
        //override var _db: Db? = null
        //suspend inline fun <reified T : DbTableElement> DbRef<T>.resolved(): T? = resolved(_db!!.uninitializedTable(T::class))
        //suspend inline fun <reified T : DbTableIntElement> DbIntRef<T>.resolved(): T? = resolved(_db!!)
    }
}

interface DbModel : DbBaseModel {
    companion object;
    @DbPrimary
    val _id: DbKey

    //var _db: Db? set(_) = Unit; get() = null

    open class Base<T : DbTableElement>(override val _id: DbRef<T> = DbRef()) : DbBaseModel.Abstract(), DbModel
    open class BaseWithExtrinsic<T : DbTableElement>(override val _id: DbRef<T> = DbRef()) : DbBaseModel.Abstract(), DbModel, ExtrinsicData by ExtrinsicData.Mixin()
}

interface DbIntModel : DbBaseModel {
    companion object;

    @DbPrimary
    val id: DbIntKey

    abstract class Base<T : DbTableIntElement>(
        override val id: DbIntRef<T> = DbIntRef()
        //, override val _id: DbRef<T> = DbRef()
    ) : DbBaseModel.Abstract(), DbIntModel
    abstract class BaseWithExtrinsic<T : DbTableIntElement>(
        override val id: DbIntRef<T> = DbIntRef()
        //,override val _id: DbRef<T> = DbRef()
    ) : DbBaseModel.Abstract(), ExtrinsicData by ExtrinsicData.Mixin(), DbIntModel
}

typealias DbTableBaseElement = DbBaseModel
typealias DbTableElement = DbModel
typealias DbTableIntElement = DbIntModel

suspend fun <T : DbTableElement> Iterable<DbRef<T>>.resolved(table: DbTable<T>): Iterable<T?> = this.map { it.resolved(table) }
suspend fun <T : DbTableElement> DbRef<T>.resolved(table: DbTable<T>): T? = table.findById(this)
suspend inline fun <reified T : DbTableElement> DbRef<T>.resolved(db: Db): T? = resolved(db.uninitializedTable(T::class))

@JvmName("resolvedInt")
suspend fun <T : DbTableIntElement> Iterable<DbIntRef<T>>.resolved(table: DbTable<T>): Iterable<T?> =
    //this.map { it.resolved(table) }
    table.find { DbQuery.IN<T, DbIntRef<*>>(DbTableIntElement::id as KProperty1<T, DbIntRef<*>>, this@resolved.toList()) }
@JvmName("resolvedInt")
suspend fun <T : DbTableIntElement> DbIntRef<T>.resolved(table: DbTable<T>): T? =
    table.findOne { DbQuery.BinOp(DbTableIntElement::id as KProperty1<T, DbIntKey>, this@resolved, DbQueryBinOp.EQ) }

@JvmName("resolvedInt")
suspend inline fun <reified T : DbTableIntElement> DbIntRef<T>.resolved(db: Db): T? = resolved(db.uninitializedTable(T::class))

//interface DbTable<T : Any> {
interface DbTable<T : DbTableBaseElement> {
    val db: Db
    val clazz: KClass<T>
    val typer: Typer

    suspend fun showColumns(): Map<String, Map<String, Any?>>
    suspend fun initialize(): DbTable<T>
    // C
    suspend fun insert(instance: T): T
    suspend fun insert(instance: Partial<T>): DbResult = insert(instance.data)
    suspend fun insert(data: Map<String, Any?>): DbResult
    // R
    suspend fun findFlowPartial(skip: Long? = null, limit: Long? = null, fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Flow<Partial<T>>
    suspend fun findFlow(skip: Long? = null, limit: Long? = null, fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Flow<T> = findFlowPartial(skip, limit, fields, sorted, query).map {
        typer.type(it.data, clazz)
            //.also { row -> row._db = db }
    }
    suspend fun find(skip: Long? = null, limit: Long? = null, fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): List<T> = findFlow(skip, limit, fields, sorted, query).toList()
    suspend fun findAll(skip: Long? = null, limit: Long? = null): List<T> = find(skip = skip, limit = limit)
    suspend fun findOne(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): T? = find(query = query, limit = 1).firstOrNull()
    suspend fun count(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Long = this.find(query = query).size.toLong()
    suspend fun <R> countGrouped(groupedBy: KProperty1<T, R>, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Map<R, Long> =
        this.find(query = query).groupBy { groupedBy.get(it) }.map { it.key to it.value.size.toLong()}.toMap()
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
