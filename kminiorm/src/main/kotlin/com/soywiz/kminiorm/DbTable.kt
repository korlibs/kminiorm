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

fun <T : DbBaseModel> T.bind(db: Db): T = db.bindInstance(this)

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

interface DbStringModel : DbBaseModel {
    companion object;

    @DbPrimary
    val id: DbStringRef<*>

    abstract class Base<T : DbTableStringElement>(
        override val id: DbStringRef<T> = DbStringRef()
        //, override val _id: DbRef<T> = DbRef()
    ) : DbBaseModel.Abstract(), DbStringModel
    abstract class BaseWithExtrinsic<T : DbTableStringElement>(
        override val id: DbStringRef<T> = DbStringRef()
        //,override val _id: DbRef<T> = DbRef()
    ) : DbBaseModel.Abstract(), ExtrinsicData by ExtrinsicData.Mixin(), DbStringModel
}

typealias DbTableBaseElement = DbBaseModel
typealias DbTableElement = DbModel
typealias DbTableIntElement = DbIntModel
typealias DbTableStringElement = DbStringModel

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

abstract class AbstractDbTable<T : DbTableBaseElement> : DbTable<T> {
    override val queryBuilder: DbQueryBuilder<T> by lazy { DbQueryBuilder(this) }
    override val typerForClass: ClassTyper<T> by lazy { typer.typerForClass(clazz) }
}

enum class DbOnConflict { ERROR, IGNORE, REPLACE }

//interface DbTable<T : Any> {
interface DbTable<T : DbTableBaseElement> {
    companion object

    val queryBuilder: DbQueryBuilder<T>
    val db: Db
    val ormTableInfo: OrmTableInfo<T>
    val clazz: KClass<T>
    val typer: Typer
    val typerForClass: ClassTyper<T>

    fun <T> bindInstance(instance: T): T = db.bindInstance(instance)

    suspend fun showColumns(): Map<String, IColumnDef>
    suspend fun initialize(): DbTable<T> = this

    // C
    suspend fun insert(instance: T): T
    suspend fun insert(instance: Partial<T>): DbResult = insert(instance.data)
    suspend fun insert(data: Map<String, Any?>): DbResult {
        insert(typerForClass.type(data))
        return DbResult(mapOf("insert" to 1))
    }

    suspend fun insertIgnore(value: T): Unit = insert(value, onConflict = DbOnConflict.IGNORE)
    suspend fun insert(instances: List<T>, onConflict: DbOnConflict = DbOnConflict.ERROR) {
        transaction {
            when (onConflict) {
                DbOnConflict.ERROR -> for (instance in instances) insert(instance)
                DbOnConflict.IGNORE -> for (instance in instances) runCatching { insert(instance) }
                DbOnConflict.REPLACE -> for (instance in instances) upsertGetNew(instance)
            }
        }
    }

    // R
    suspend fun findFlowPartial(skip: Long? = null, limit: Long? = null, fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Flow<Partial<T>>
    suspend fun findFlow(skip: Long? = null, limit: Long? = null, fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Flow<T> = findFlowPartial(skip, limit, fields, sorted, query)
            .map { bindInstance(typerForClass.type(it.data)) }

    suspend fun find(skip: Long? = null, limit: Long? = null, fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): List<T> = findFlow(skip, limit, fields, sorted, query).toList()
    suspend fun findAll(skip: Long? = null, limit: Long? = null): List<T> = find(skip = skip, limit = limit)
    suspend fun findOne(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): T? = find(query = query, limit = 1).firstOrNull()
    suspend fun findChunked(fields: List<KProperty1<T, *>>? = null, sorted: List<Pair<KProperty1<T, *>, Int>>? = null, chunkSize: Int = 10, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Flow<T> {
        val table = this
        return flow {
            var offset = 0L
            while (true) {
                val results = table.find(skip = offset, limit = chunkSize.toLong(), fields = fields, sorted = sorted, query = query)
                if (results.isEmpty()) break
                offset += results.size
                for (result in results) emit(result)
                //delay(0L)
            }
        }
    }

    suspend fun count(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Long = this.find(query = query).size.toLong()
    suspend fun <R> countGrouped(groupedBy: KProperty1<T, R>, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Map<R, Long> =
            this.find(query = query).groupBy { groupedBy.get(it) }.map { it.key to it.value.size.toLong() }.toMap()

    // U
    suspend fun update(set: Partial<T>? = null, increment: Partial<T>? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long
    suspend fun upsert(instance: T) = insert(instance, onConflict = DbOnConflict.REPLACE)

    suspend fun upsertGetNew(instance: T): T {
        val info = OrmTableInfo(db.dialect, instance::class)
        val props = info.columns
                .filter { it.isPrimaryOrUnique && (it.property.name != DbModel::_id.name) }
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
            val query = queryBuilder.build {
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
    suspend fun delete(limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long
    suspend fun deleteAll(limit: Long? = null) = delete() { everything }

    suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R = callback()
}

suspend fun <T : DbTableElement> DbTable<T>.findById(id: DbKey): T? = findOne { DbQuery.BinOp(DbModel::_id as KProperty1<T, DbKey>, id, DbQueryBinOp.EQ) }
suspend fun <T : DbTableIntElement> DbTable<T>.findByIntId(id: DbIntRef<T>): T? = findOne { DbQuery.BinOp(DbIntModel::id as KProperty1<T, DbIntKey>, id, DbQueryBinOp.EQ) }
suspend fun <T : DbTableStringElement> DbTable<T>.findByIntId(id: DbStringRef<T>): T? = findOne { DbQuery.BinOp(DbStringModel::id as KProperty1<T, DbStringRef<*>>, id, DbQueryBinOp.EQ) }
suspend fun <T : DbTableBaseElement> DbTable<T>.findOrCreate(query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }, build: () -> T): T = findOne(query) ?: build().also { insert(it) }
suspend fun <T : DbTableBaseElement> DbTable<T>.insert(vararg values: T, onConflict: DbOnConflict = DbOnConflict.ERROR) = insert(values.toList(), onConflict)

suspend fun <T : DbTableBaseElement> T.save(table: DbTable<T>) = table.upsert(this)
suspend inline fun <reified T : DbTableBaseElement> T.save(db: Db) = save(db.uninitializedTable<T>())

class UpdateSimpleBuilder<T : DbBaseModel> {
    internal val sets = arrayListOf<Pair<KProperty1<T, *>, Any?>>()
    internal val incrs = arrayListOf<Pair<KProperty1<T, *>, Any?>>()

    fun <R> set(field: KProperty1<T, R>, value: R) {
        sets += field to value
    }
    fun <R : Number> incr(field: KProperty1<T, R>, value: R) {
        incrs += field to value
    }
}

suspend fun <T : DbBaseModel> DbTable<T>.updateSimple(query: DbQueryBuilder<T>.() -> DbQuery<T>, build: UpdateSimpleBuilder<T>.() -> Unit): Long {
    val builder = UpdateSimpleBuilder<T>()
    build(builder)
    return this.update(set = Partial(*builder.sets.toTypedArray(), clazz = clazz), increment = Partial(*builder.incrs.toTypedArray(), clazz = clazz), query = query)
}
