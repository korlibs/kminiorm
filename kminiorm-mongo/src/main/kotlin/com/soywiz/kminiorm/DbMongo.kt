package com.soywiz.kminiorm

import com.mongodb.*
import com.mongodb.async.*
import com.mongodb.async.client.*
import com.mongodb.client.model.*
import com.mongodb.client.result.*
import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import org.bson.*
import org.bson.types.*
import java.util.*
import kotlin.coroutines.*
import kotlin.reflect.*

// https://api.mongodb.com/java/3.0/?com/mongodb/ConnectionString.html
//fun Vertx.createMongo(connectionString: String): DbMongo =
//        DbMongo(MongoClient.createShared(this, JsonObject(mapOf("connection_string" to connectionString))))

//val DEBUG_MONGO = true
val DEBUG_MONGO = false

class DbMongo private constructor(val mongoClient: MongoClient, val client: MongoDatabase, val typer: Typer) : AbstractDb() {
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

    override fun <T : DbTableBaseElement> constructTable(clazz: KClass<T>): DbTable<T> = DbTableMongo(this, clazz)
}

class DbTableMongo<T : DbTableBaseElement>(override val db: DbMongo, override val clazz: KClass<T>) : DbTable<T> {
    override val typer get() = db.typer
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
        coroutineScope {
            val jobs = arrayListOf<Job>()
            for ((indexName, columns) in ormTableInfo.columnIndices) {
                val isUnique = columns.any { it.isUnique }
                val map = columns.map { it.name to it.indexDirection.sign }.toMap()
                //println("INDEX: indexName=$indexName, map=$map")
                jobs += launch {
                    awaitMongo<String> {
                        dbCollection.createIndex(
                            Document(map),
                            IndexOptions().name(indexName).unique(isUnique).background(false),
                            it
                        )
                    }
                }
            }
            jobs.joinAll()
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

    override suspend fun findFlowPartial(skip: Long?, limit: Long?, fields: List<KProperty1<T, *>>?, sorted: List<Pair<KProperty1<T, *>, Int>>?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Flow<Partial<T>> {
        val rquery = DbQueryBuilder.build(query).toMongoMap().toJsonObject(db)
        val wantsId = fields?.any { it.name == "_id" } ?: true
        if (DEBUG_MONGO) println("QUERY: $rquery")
        val result = dbCollection.find(rquery)
                //.batchSize(50).limit(1000)
                .let { if (skip != null) it.skip(skip.toInt()) else it }
                .let { if (limit != null) it.limit(limit.toInt()) else it }
                .let { if (fields != null) it.projection(BsonDocument(
                        fields.map { BsonElement(ormTableInfo.getColumnByProp(it)!!.name, BsonInt32(1)) }
                )) else it }
                .let { if (sorted != null) it.sort(BsonDocument(
                        sorted.map { pair -> BsonElement(
                                ormTableInfo.getColumnByProp(pair.first)!!.name,
                                BsonInt32(pair.second)
                        ) }
                )) else it }

        val channel = Channel<Partial<T>>(Channel.UNLIMITED)
        //var n = 0
        result.forEach(
            {
                try {
                    val partial = Partial(it, clazz)
                    val fpartial = if (wantsId) partial else partial.without(DbTableElement::_id as KProperty1<T, *>)
                    //n++
                    //println("ITEM: $n")
                    channel.offer(fpartial)
                } catch (e: Throwable) {
                    //println("EXCEPTION")
                    channel.close(e)
                    throw e
                }
            },
            { result, t ->
                //println("FINISHED! $result, $t")
                channel.close(t)
            }
        )
        return channel.consumeAsFlow()
        //return flow { try {  while (true) emit(channel.receive())  } catch (e: ClosedReceiveChannelException) {  }  }
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

    class Binop(val left: MongoQueryNode, val op: DbQueryBinOp, val right: MongoQueryNode) : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo) = when (op) {
            DbQueryBinOp.AND -> Document(left.toJsonObject(db) + right.toJsonObject(db))
            else -> Document(mapOf(op.toMongoOp() to listOf(left.toJsonObject(db), right.toJsonObject(db))))
        }
    }

    class In(val left: KProperty<*>, val items: List<Any?>) : MongoQueryNode() {
        override fun toJsonObject(db: DbMongo) = Document(mapOf(
                left.name to mapOf("\$in" to items.map { if (it != null) db.typer.untype(it) else null })
        ))
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
    DbQueryBinOp.GE -> "\$gte"
    DbQueryBinOp.LE -> "\$lte"
}

fun DbQueryUnOp.toMongoOp() = when (this) {
    DbQueryUnOp.NOT -> "\$not"
}

fun <T> DbQuery<T>.toMongoMap(): MongoQueryNode = when (this) {
    is DbQuery.BinOp<*, *> -> MongoQueryNode.PropComparison(this.op.toMongoOp(), this.prop, this.literal)
    is DbQuery.Always<*> -> MongoQueryNode.Always()
    is DbQuery.Never<*> -> MongoQueryNode.Never()
    is DbQuery.BinOpNode<*> -> MongoQueryNode.Binop(left.toMongoMap(), this.op, right.toMongoMap())
    is DbQuery.UnOpNode<*> -> TODO("Unary ${this.op}")
    is DbQuery.IN<*, *> -> MongoQueryNode.In(this.prop, this.literal)
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
