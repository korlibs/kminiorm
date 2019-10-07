package com.soywiz.kminiorm

import kotlinx.coroutines.*
import java.sql.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

interface Db {
    suspend fun <T : DbTableElement> table(clazz: KClass<T>): DbTable<T>
    companion object
}

suspend inline fun <reified T : DbTableElement> Db.table() = table(T::class)
fun <T : DbTableElement> Db.tableBlocking(clazz: KClass<T>) = runBlocking { table(clazz) }
inline fun <reified T : DbTableElement> Db.tableBlocking() = tableBlocking(T::class)

val ResultSet.columnNames get() = (1..metaData.columnCount).map { metaData.getColumnName(it) }

fun ResultSet.toListMap(): List<Map<String, Any?>> {
    val metaData = this.metaData
    val out = arrayListOf<Map<String, Any?>>()
    while (this.next()) {
        val row = LinkedHashMap<String, Any?>()
        for (column in 1..metaData.columnCount) {
            row[metaData.getColumnName(column)] = this.getObject(column)
        }
        out.add(row)
    }
    return out
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
    val isUnique = property.findAnnotation<DbUnique>() != null
    val isIndex = property.findAnnotation<DbIndex>() != null
}

class OrmTableInfo<T : Any>(val clazz: KClass<T>) {
    val tableName = clazz.findAnnotation<DbName>()?.name ?: clazz.simpleName ?: error("$clazz doesn't have name")
    val columns = clazz.memberProperties.filter { it.findAnnotation<DbIgnore>() == null }.map { ColumnDef(it) }
    val columnsByName = columns.associateBy { it.name }
    fun getColumnByName(name: String) = columnsByName[name]
}
