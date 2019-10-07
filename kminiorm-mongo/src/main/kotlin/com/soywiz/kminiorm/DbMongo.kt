package com.soywiz.kminiorm

import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.*
import com.fasterxml.jackson.databind.node.*
import io.vertx.core.*
import io.vertx.core.json.*
import io.vertx.ext.mongo.*
import io.vertx.ext.mongo.impl.codec.json.*
import io.vertx.kotlin.coroutines.*
import org.bson.types.*
import kotlin.reflect.*

// https://api.mongodb.com/java/3.0/?com/mongodb/ConnectionString.html
fun Vertx.createMongo(connectionString: String): DbMongo =
        DbMongo(MongoClient.createShared(this, JsonObject(mapOf("connection_string" to connectionString))))

class DbMongo(val client: MongoClient) : Db {
    override suspend fun <T : DbTableElement> table(clazz: KClass<T>): DbTable<T> = DbTableMongo(this, clazz).initialize()
}

class DbTableMongo<T : DbTableElement>(val db: DbMongo, val clazz: KClass<T>) : DbTable<T> {
    val ormTableInfo by lazy { OrmTableInfo(clazz) }
    val collection get() = ormTableInfo.tableName
    val mongo get() = db.client

    override suspend fun showColumns(): Map<String, Map<String, Any?>> {
        TODO()
    }

    override suspend fun initialize(): DbTable<T> = this.apply {
        kotlin.runCatching {
            //awaitResult<Void> { db.client.createCollection(collection, it) }
            db.client.createCollection(collection) { }
        }

        // Ensure indices
        for (column in ormTableInfo.columns) {
            if (column.isIndex || column.isUnique) {
                /*
                awaitResult<Void> { res ->
                    db.client.createIndexWithOptions(
                            collection,
                            JsonObject(mapOf(column.name to +1)),
                            IndexOptions().unique(column.isUnique).background(true),
                            res
                    )
                }
                 */
                db.client.createIndexWithOptions(
                        collection,
                        JsonObject(mapOf(column.name to +1)),
                        IndexOptions().unique(column.isUnique).background(true)
                ) { }
            }
        }
    }

    override suspend fun insert(instance: T): T {
        insert(objMapperForMongo.convertValue(instance, Map::class.java) as Map<String, Any?>)
        return instance
    }

    override suspend fun insert(data: Map<String, Any?>): DbResult {
        val dataToInsert = data.mapToMongoJson()
        //println("dataToInsert: $dataToInsert")
        awaitResult<String> { db.client.insert(collection, dataToInsert, it) }
        return DbResult(mapOf("insert" to 1))
    }

    override suspend fun find(skip: Long?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Iterable<T> {
        val rquery = DbQueryBuilder.build(query).toMongoMap().toJsonObject()
        //println("QUERY: $rquery")
        val result = awaitResult<List<JsonObject>> { mongo.find(collection, rquery, it) }
        return result.map { objMapperForMongo.convertValue<T>(it, clazz.java) }
    }

    override suspend fun update(set: Partial<T>, increment: Partial<T>?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        if (increment != null) error("DbMongo.update.increment not implemented yet!")
        val result = awaitResult<MongoClientUpdateResult> {
            mongo.updateCollection(
                    collection,
                    DbQueryBuilder.build(query).toMongoMap().toJsonObject(),
                    JsonObject(
                            mapOf(
                                    "\$set" to set.data.mapToMongo()
                            )
                    ),
                    it
            )
        }
        return result.docModified
    }

    override suspend fun <R> transaction(callback: suspend DbTable<T>.() -> R): R {
        println("TODO: MongoDB transactions not implemented yet")
        return callback()
    }
}

fun Map<String, Any?>.mapToMongoJson() = JsonObject(mapToMongo())

fun convertToMongoDb(value: Any?): Any? = when (value) {
    is DbKey -> ObjectId(value.toHexString())
    else -> value
}

fun Map<String, Any?>.mapToMongo(): Map<String, Any?> {
    return this.entries.associate { (key, value) -> key to convertToMongoDb(value) }
}

/*
class Mongo(val client: MongoClient) {
    suspend fun createCollection(name: String) = awaitResult<Void> { client.createCollection(name, it) }
    suspend fun listCollections() = awaitResult<List<String>> { client.getCollections(it) }
    suspend fun find(collection: String, query: JsonObject) = awaitResult<List<JsonObject>> { client.find(collection, query, it) }
    suspend fun findOne(collection: String, query: JsonObject) = awaitResult<JsonObject> {
        //println("QUERY: $query")
        client.findOne(collection, query, null, it)
    }

    fun collection(collection: String) = MongoCollection(collection, this)
    inline fun <reified T : Any> typedCollection(collection: String) = TypedMongoCollection(collection, this, T::class)
}

class MongoCollection(val collection: String, val mongo: Mongo) {
    suspend fun find(query: JsonObject) = mongo.find(collection, query)
}

class TypedMongoCollection<T : Any>(val collection: String, val mongo: Mongo, val clazz: KClass<T>) {
    @Suppress("UNCHECKED_CAST")
    private val queryBuilder = DbQueryBuilder.builder()

    //suspend fun find(query: JsonObject) = mongo.find(collection, query)
    suspend fun find(query: DbQueryBuilder<T>.() -> DbQuery<T>) =
            mongo.find(collection, query(queryBuilder).toJsonObject()).map { objMapperForMongo.convertValue<T>(it, clazz.java) }

    suspend fun findOne(query: MongoQueryBuilder<T>.() -> MongoQueryNode) =
            objMapperForMongo.convertValue<T>(mongo.findOne(collection, query(queryBuilder).toJsonObject()), clazz.java)

    suspend fun insert(item: T): String = awaitResult {
        val untyped = JsonObject(objMapperForMongo.convertValue(item, Map::class.java) as Map<String, Any?>)

        mongo.client.insert(collection, untyped, it)
    }

    suspend fun update(query: MongoQueryBuilder<T>.() -> MongoQueryNode, partial: Partial<T>) {
        val result = awaitResult<MongoClientUpdateResult> {
            mongo.client.updateCollection(
                    collection,
                    query(queryBuilder).toJsonObject(),
                    JsonObject(
                            mapOf(
                                    "\$set" to partial.data
                            )
                    ),
                    it
            )
        }
    }

    fun ensureIndex(vararg props: Pair<KProperty1<T, Any>, Int>, unique: Boolean = false, dropDups: Boolean = false, background: Boolean = true) = this.apply {
        mongo.client.createIndex(collection, JsonObject(props.toMap().mapKeys { it.key.name })) { }
    }
}
 */


sealed class MongoQueryNode {
    abstract fun toJsonObject(): JsonObject

    class PropComparison<T>(val op: String, val left: KProperty<T>, val value: T) : MongoQueryNode() {
        override fun toJsonObject(): JsonObject {
            val value = convertToMongoDb(value)

            val v: Any? = when (value) {
                is ObjectId -> JsonObject(mapOf(JsonObjectCodec.OID_FIELD to value.toHexString()))
                else -> value
            }
            return JsonObject(
                    mapOf(left.name to if (op == "\$eq") v else mapOf(op to v))
            )
        }
    }

    class Always : MongoQueryNode() {
        override fun toJsonObject() = JsonObject(mapOf())
    }

    class And(val left: MongoQueryNode, val right: MongoQueryNode) : MongoQueryNode() {
        override fun toJsonObject() = JsonObject(left.toJsonObject().map + right.toJsonObject().map)
    }

    class Raw(val json: JsonObject) : MongoQueryNode() {
        override fun toJsonObject() = json
    }
}

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
    is DbQuery.BinOpNode<*> -> when (this.op) {
        DbQueryBinOp.AND -> MongoQueryNode.And(left.toMongoMap(), right.toMongoMap())
        else -> TODO("Operator ${this.op}")
    }
    is DbQuery.UnOpNode<*> -> TODO("Unary ${this.op}")
    is DbQuery.IN<*, *> -> TODO("IN")
    is DbQuery.Raw<*> -> MongoQueryNode.Raw(JsonObject(map))
    else -> TODO()
}

private val objMapperForMongo: ObjectMapper = Json.mapper.copy().also { mapper ->
    mapper.registerModule(SimpleModule().let { module ->
        module.addSerializer(ObjectId::class.java, object : JsonSerializer<ObjectId>() {
            override fun serialize(value: ObjectId, gen: JsonGenerator, serializers: SerializerProvider) {
                gen.writeObject(mapOf(JsonObjectCodec.OID_FIELD to value.toHexString()))
            }
        })
        module.addDeserializer(ObjectId::class.java, object : JsonDeserializer<ObjectId>() {
            override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ObjectId {
                val node = p.codec.readTree<TreeNode>(p)
                if (node is TextNode) return ObjectId(node.textValue())
                return ObjectId((node.get(JsonObjectCodec.OID_FIELD) as TextNode).textValue())
            }
        })
    })
    mapper.registerDbKeyModule()
}
