package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.*
import java.sql.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

interface Db {
    suspend fun <T : DbTableElement> table(clazz: KClass<T>, initialize: Boolean = true): DbTable<T>
    fun <T : DbTableElement> uninitializedTable(clazz: KClass<T>): DbTable<T>
    companion object
}

abstract class AbstractDb : Db {
    private val cachedTables = java.util.LinkedHashMap<KClass<*>, DbTable<*>>()
    private val uninitializedCachedTables = java.util.LinkedHashMap<KClass<*>, DbTable<*>>()
    override suspend fun <T : DbTableElement> table(clazz: KClass<T>, initialize: Boolean): DbTable<T> {
        return if (initialize) {
            cachedTables.getOrPut(clazz) { uninitializedTable(clazz).also { if (initialize) it.initialize() } } as DbTable<T>
        } else {
            uninitializedTable(clazz)
        }
    }
    override fun <T : DbTableElement> uninitializedTable(clazz: KClass<T>): DbTable<T> = uninitializedCachedTables.getOrPut(clazz) { constructTable(clazz) } as DbTable<T>
    protected abstract fun <T : DbTableElement> constructTable(clazz: KClass<T>): DbTable<T>
}

val __extrinsicUnquoted__ = "__extrinsic__"
val __extrinsic__ = "\"$__extrinsicUnquoted__\""

suspend inline fun <reified T : DbTableElement> Db.table(): DbTable<T> = table(T::class)
inline fun <reified T : DbTableElement> Db.uninitializedTable(): DbTable<T> = uninitializedTable(T::class)

fun <T : DbTableElement> Db.tableBlocking(clazz: KClass<T>) = runBlocking { table(clazz) }
inline fun <reified T : DbTableElement> Db.tableBlocking() = tableBlocking(T::class)

val ResultSet.columnNames get() = (1..metaData.columnCount).map { metaData.getColumnName(it) }

fun ResultSet.toListMap(): List<Map<String, Any?>> {
    val metaData = this.metaData
    val out = arrayListOf<Map<String, Any?>>()
    while (this.next()) {
        val row = LinkedHashMap<String, Any?>()
        for (column in 1..metaData.columnCount) {
            row[metaData.getColumnLabel(column)] = this.getObject(column)
        }
        out.add(row.fixRow())
    }
    return out
}

private fun Map<String, Any?>.fixRow(): Map<String, Any?> {
    if (this.containsKey(__extrinsicUnquoted__)) {
        val data = this[__extrinsicUnquoted__].toString()
        return (this + MiniJson.parse(data) as Map<String, Any?>) - setOf(__extrinsicUnquoted__)
    } else {
        return this
    }
}

interface DbResult : List<Map<String, Any?>> {
    val data: List<Map<String, Any?>> get() = this
    val updateCount: Long get() = this.first().values.firstOrNull()?.toString()?.toLongOrNull() ?: 0L
}

fun DbResult(data: List<Map<String, Any?>>): DbResult = object : DbResult, List<Map<String, Any?>> by data {}
fun DbResult(vararg data: Map<String, Any?>): DbResult = DbResult(data.toList())

class ColumnDef<T : Any>(val property: KProperty1<T, *>) {
    val jclazz get() = property.returnType.jvmErasure
    val name = property.findAnnotation<DbName>()?.name ?: property.name
    val isNullable get() = property.returnType.isMarkedNullable
    val isPrimary = property.findAnnotation<DbPrimary>() != null
    val isUnique = property.findAnnotation<DbUnique>() != null
    val isNormalIndex = property.findAnnotation<DbIndex>() != null
    //val ignored: Boolean = property.findAnnotation<DbIgnore>() != null

    val isAnyIndex get() = isUnique || isNormalIndex

    val unique = property.findAnnotation<DbUnique>()
    val index = property.findAnnotation<DbIndex>()

    val indexOrder = unique?.order ?: index?.order ?: 0

    val indexName = unique?.name?.takeIf { it.isNotEmpty() } ?: index?.name?.takeIf { it.isNotEmpty() } ?: name

    val indexDirection get() =
        property.findAnnotation<DbPrimary>()?.direction
                ?: property.findAnnotation<DbUnique>()?.direction
                ?: property.findAnnotation<DbIndex>()?.direction
                ?: DbIndexDirection.ASC
}

class OrmTableInfo<T : Any>(val clazz: KClass<T>) {
    val tableName = clazz.findAnnotation<DbName>()?.name ?: clazz.simpleName ?: error("$clazz doesn't have name")
    val columns = clazz.memberProperties.filter { it.findAnnotation<DbIgnore>() == null && !it.name.startsWith("__") }.map { ColumnDef(it) }
    val columnIndices = columns.filter { it.isAnyIndex }.sortedBy { it.indexOrder }.groupBy { it.indexName }
    val columnsByName = columns.associateBy { it.name }
    fun getColumnByName(name: String) = columnsByName[name]
    fun getColumnByProp(prop: KProperty1<T, *>) = getColumnByName(prop.name)
}
