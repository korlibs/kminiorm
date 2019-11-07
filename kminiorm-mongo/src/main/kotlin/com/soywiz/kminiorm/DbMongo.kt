package com.soywiz.kminiorm

import com.mongodb.*
import com.mongodb.async.*
import com.mongodb.async.client.*
import com.mongodb.client.model.*
import com.mongodb.client.result.*
import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.*
import org.bson.*
import org.bson.types.*
import java.util.*
import kotlin.coroutines.*
import kotlin.reflect.*

// https://api.mongodb.com/java/3.0/?com/mongodb/ConnectionString.html
//fun Vertx.createMongo(connectionString: String): DbMongo =
//        DbMongo(MongoClient.createShared(this, JsonObject(mapOf("connection_string" to connectionString))))

class DbMongo private constructor(val mongoClient: MongoClient, val client: MongoDatabase, val typer: Typer) : Db {
    companion object {
        /**
         * Example: DbMongo("mongodb://127.0.0.1:27017/kminiormtest")
         */
        operator fun invoke(connectionString: String, typer: Typer = mongoTyper): DbMongo {
            val connection = ConnectionString(connectionString)
            val client = MongoClients.create(connection)
            return DbMongo(client, client.getDatabase(connection.database ?: error("No database specified")), typer)
        }
    }

    private val cachedTables = LinkedHashMap<KClass<*>, DbTable<*>>()
    override suspend fun <T : DbTableElement> table(clazz: KClass<T>): DbTable<T> = cachedTables.getOrPut(clazz) { DbTableMongo(this, clazz).initialize() } as DbTable<T>
}

class DbTableMongo<T : DbTableElement>(val db: DbMongo, val clazz: KClass<T>) : DbTable<T> {
    val ormTableInfo by lazy { OrmTableInfo(clazz) }
    val collection get() = ormTableInfo.tableName
    val mongo get() = db.client
    val dbCollection by lazy { db.client.getCollection(collection) }

    override suspend fun showColumns(): Map<String, Map<String, Any?>> {
        TODO()
    }

    override suspend fun initialize(): DbTable<T> = this.apply {
        kotlin.runCatching {
            //awaitResult<Void> { db.client.createCollection(collection, it) }
            db.client.createCollection(collection) { result, t -> }
        }

        // Ensure indices
        for ((indexName, columns) in ormTableInfo.columnIndices) {
            val isUnique = columns.any { it.isUnique }
            val map = columns.map { it.name to it.indexDirection.sign }.toMap()
            //println("INDEX: indexName=$indexName, map=$map")
            dbCollection.createIndex(
                Document(map),
                IndexOptions().name(indexName).unique(isUnique).background(true)
            ) { result, t -> }
        }
    }

    override suspend fun insert(instance: T): T {
        insert(db.typer.untype(instance) as Map<String, Any?>)
        return instance
    }

    override suspend fun insert(data: Map<String, Any?>): DbResult {
        try {
            val dataToInsert = data.mapToMongoJson(db.typer)
            awaitMongo<Void> { dbCollection.insertOne(dataToInsert, it) }
            return DbResult(mapOf("insert" to 1))
        } catch (e: MongoWriteException) {
            if (e.error.category == ErrorCategory.DUPLICATE_KEY) {
                throw DuplicateKeyDbException("Conflict", e)
            }
            throw DbException("Database exception", e)
        }
    }

    override suspend fun find(skip: Long?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): List<T> {
        val rquery = DbQueryBuilder.build(query).toMongoMap().toJsonObject(db)
        //println("QUERY: $rquery")
        val result = dbCollection.find(rquery)
            .let { if (skip != null) it.limit(skip.toInt()) else it }
            .let { if (limit != null) it.limit(limit.toInt()) else it }
        val items = arrayListOf<Document>()
        awaitMongo<Void> { result.forEach(Block { items.add(it) }, it) }

        return items.map { db.typer.type<T>(it, clazz) }
    }

    override suspend fun update(set: Partial<T>?, increment: Partial<T>?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        val setData = (set?.data?.mapToMongo(db.typer) ?: mapOf())
        val incData = (increment?.data?.mapToMongo(db.typer) ?: mapOf())
        val updateMap = Document(mutableMapOf<String, Any?>().also { map ->
            if (setData.isNotEmpty()) map["\$set"] = setData
            if (incData.isNotEmpty()) map["\$inc"] = incData
        })

        val result = awaitMongo<UpdateResult> {
            val bson = DbQueryBuilder.build(query).toMongoMap().toJsonObject(db)
            when (limit) {
                null -> dbCollection.updateMany(bson, updateMap, it)
                1L -> dbCollection.updateOne(bson, updateMap, it)
                else -> error("Unsupported $limit != 1L, $limit != null")
            }
        }
        return result.modifiedCount
    }

    override suspend fun delete(limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        val rquery = DbQueryBuilder.build(query).toMongoMap().toJsonObject(db)
        val result = awaitMongo<DeleteResult> { dbCollection.deleteMany(rquery, it) }
        return result.deletedCount
    }

    override suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R {
        println("TODO: MongoDB transactions not implemented yet")
        return callback()
    }
}

fun Map<String, Any?>.mapToMongoJson(mongoTyper: Typer) = Document(mapToMongo(mongoTyper))

fun convertToMongoDb(value: Any?, mongoTyper: Typer): Any? = mongoTyper.untypeNull(value)

fun Map<String, Any?>.mapToMongo(mongoTyper: Typer): Map<String, Any?> {
    return convertToMongoDb(this, mongoTyper) as Map<String, Any>
    return this.entries.associate { (key, value) -> key to convertToMongoDb(value, mongoTyper) }
}

sealed class MongoQueryNode {
    abstract fun toJsonObject(db: DbMongo): Document

    class PropComparison<T>(val op: String, val left: KProperty<T>, val value: T) : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo): Document {
            val value = convertToMongoDb(value, db.typer)

            val v: Any? = when (value) {
                //is DbKey -> Document(mapOf(OID to value.toHexString()))
                //is ObjectId -> Document(mapOf(OID to value.toHexString()))
                is DbKey -> ObjectId(value.toHexString())
                else -> value
            }
            return Document(
                mapOf(left.name to if (op == "\$eq") v else mapOf(op to v))
            )
        }
    }

    class Always : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo) = Document(mapOf())
    }

    class Never : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo) = Document(mapOf("__UNIEXIStan__T" to "impossibl€"))
    }

    class And(val left: MongoQueryNode, val right: MongoQueryNode) : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo) = Document(left.toJsonObject(db) + right.toJsonObject(db))
    }

    class Raw(val json: Document) : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo) = json
    }
}

private val OID = "\$oid"

fun DbQueryBinOp.toMongoOp() = when (this) {
    DbQueryBinOp.AND -> "\$and" // CHECK
    DbQueryBinOp.OR -> "\$or" // CHECK
    DbQueryBinOp.LIKE -> "\$like" // CHECK
    DbQueryBinOp.EQ -> "\$eq"
    DbQueryBinOp.NE -> "\$ne"
    DbQueryBinOp.GT -> "\$gt"
    DbQueryBinOp.LT -> "\$lt"
    DbQueryBinOp.GE -> "\$ge"
    DbQueryBinOp.LE -> "\$le"
}

fun DbQueryUnOp.toMongoOp() = when (this) {
    DbQueryUnOp.NOT -> "\$not"
}

fun <T> DbQuery<T>.toMongoMap(): MongoQueryNode = when (this) {
    is DbQuery.BinOp<*, *> -> MongoQueryNode.PropComparison(this.op.toMongoOp(), this.prop, this.literal)
    is DbQuery.Always<*> -> MongoQueryNode.Always()
    is DbQuery.Never<*> -> MongoQueryNode.Never()
    is DbQuery.BinOpNode<*> -> when (this.op) {
        DbQueryBinOp.AND -> MongoQueryNode.And(left.toMongoMap(), right.toMongoMap())
        else -> TODO("Operator ${this.op}")
    }
    is DbQuery.UnOpNode<*> -> TODO("Unary ${this.op}")
    is DbQuery.IN<*, *> -> TODO("IN")
    is DbQuery.Raw<*> -> MongoQueryNode.Raw(Document(map))
    else -> TODO()
}

private val mongoTyper = DbTyper
    .withKeepType<ObjectId>()
    .withTyperUntyper(
        typer = { it, type ->
            when (it) {
                is ObjectId -> DbKey(it.toHexString())
                else -> DbKey(it.toString())
            }
        },
        untyper = { ObjectId(it.toHexString()) }
    )
    .withTyperUntyper<ByteArray>(
        typer = { it, type ->
            when (it) {
                is ByteArray -> it
                is Binary -> it.data
                is String -> it.fromBase64()
                else -> error("Unknown how to decode $it to $type")
            }
        },
        untyper = { it.toBase64() }
    )
    .withTyperUntyper<Binary>(
        typer = { it, type ->
            when (it) {
                is ByteArray -> Binary(it)
                is Binary -> it
                is String -> Binary(it.fromBase64())
                else -> error("Unknown how to decode $it to $type")
            }
        },
        untyper = { it.data.toBase64() }
    )

private fun ByteArray.toBase64(): String = Base64.getEncoder().encodeToString(this)
private fun String.fromBase64(): ByteArray = Base64.getDecoder().decode(this)

@PublishedApi
internal suspend fun <T> awaitMongo(body: (SingleResultCallback<T>) -> Unit) = suspendCancellableCoroutine { c: CancellableContinuation<T> ->
    body(SingleResultCallback { result, t -> if (t != null) c.resumeWithException(t) else c.resume(result) })
}
