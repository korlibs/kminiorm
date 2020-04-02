package com.soywiz.kminiorm

import com.soywiz.kminiorm.dialect.SqlDialect
import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.*
import java.sql.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

interface Db {
    val dialect: SqlDialect
    suspend fun <T : DbTableBaseElement> table(clazz: KClass<T>, initialize: Boolean = true): DbTable<T>
    fun <T : DbTableBaseElement> uninitializedTable(clazz: KClass<T>): DbTable<T>
    fun registerBinder(binder: (value: Any?) -> Unit)
    fun <T> bindInstance(instance: T): T
    companion object
}

abstract class AbstractDb(override val dialect: SqlDialect) : Db {
    private val cachedTables = java.util.LinkedHashMap<KClass<*>, DbTable<*>>()
    private val uninitializedCachedTables = java.util.LinkedHashMap<KClass<*>, DbTable<*>>()
    override suspend fun <T : DbTableBaseElement> table(clazz: KClass<T>, initialize: Boolean): DbTable<T> {
        return if (initialize) {
            cachedTables.getOrPut(clazz) { uninitializedTable(clazz).also { if (initialize) it.initialize() } } as DbTable<T>
        } else {
            uninitializedTable(clazz)
        }
    }
    private var binders = listOf<(value: Any?) -> Unit>()
    override fun registerBinder(binder: (value: Any?) -> Unit) {
        synchronized(this) { binders += binder }
    }
    override fun <T> bindInstance(instance: T): T {
        for (binder in binders) binder(instance)
        return instance
    }
    override fun <T : DbTableBaseElement> uninitializedTable(clazz: KClass<T>): DbTable<T> = uninitializedCachedTables.getOrPut(clazz) { constructTable(clazz) } as DbTable<T>
    protected abstract fun <T : DbTableBaseElement> constructTable(clazz: KClass<T>): DbTable<T>
}

val __extrinsicUnquoted__ = "__extrinsic__"
val EXTRINSIC_COLUMN = SyntheticColumn<String>(__extrinsicUnquoted__, "{}")

// autoBinding
data class DbBinding<T : Any, R>(val clazz: KClass<T>, val prop: KMutableProperty1<T, R>, val value: R)
inline fun <reified T : Any, R> Db.autoBind(prop: KMutableProperty1<T, R>, value: R) = autoBind(DbBinding(T::class, prop, value))
fun <T : Any, R> Db.autoBind(binding: DbBinding<T, R>): Unit = registerBinder {
    if (binding.clazz.isInstance(it)) (binding.prop as KMutableProperty1<Any?, Any?>).set(it, binding.value)
}

suspend inline fun <reified T : DbTableBaseElement> Db.table(): DbTable<T> = table(T::class)
inline fun <reified T : DbTableBaseElement> Db.uninitializedTable(): DbTable<T> = uninitializedTable(T::class)

fun <T : DbTableBaseElement> Db.tableBlocking(clazz: KClass<T>) = runBlocking { table(clazz) }
inline fun <reified T : DbTableBaseElement> Db.tableBlocking() = tableBlocking(T::class)

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

interface IColumnDef {
    val name: String
    val columnType: KType
    val defaultValue: Any?
    val annotatedElement: KAnnotatedElement?
}

data class SyntheticColumn(
    override val name: String,
    override val columnType: KType,
    override val defaultValue: Any? = Unit,
    override val annotatedElement: KAnnotatedElement? = null
) : IColumnDef

@OptIn(ExperimentalStdlibApi::class)
inline fun <reified T> SyntheticColumn(name: String, defaultValue: Any? = Unit, annotatedElement: KAnnotatedElement? = null) = SyntheticColumn(name, typeOf<T>(), defaultValue, annotatedElement)

class ColumnDef<T : Any>(val ormTableInfo: OrmTableInfo<T>, val dialect: SqlDialect, val property: KProperty1<T, *>) : IColumnDef {
    val jclazz get() = property.returnType.jvmErasure
    override val name = property.findAnnotation<DbName>()?.name ?: property.name
    override val columnType get() = property.returnType
    override val defaultValue: Any? get() = when {
        columnType.jvmErasure == String::class -> ""
        columnType.jvmErasure.isSubclassOf(Number::class) -> 0
        else -> Unit
    }
    override val annotatedElement: KAnnotatedElement? = property
    val isNullable get() = property.returnType.isMarkedNullable
    val isPrimary = property.findAnnotation<DbPrimary>() != null
    val isAutoIncrement = property.findAnnotation<DbAutoIncrement>() != null
    val isUnique = property.findAnnotation<DbUnique>() != null
    val isPrimaryOrUnique = isPrimary || isUnique
    val isNormalIndex = property.findAnnotation<DbIndex>() != null
    //val ignored: Boolean = property.findAnnotation<DbIgnore>() != null

    val skipOnInsert get() = isAutoIncrement

    //val isAnyIndex get() = isUnique || isNormalIndex
    val isAnyIndex get() = isUnique || isNormalIndex || isPrimary

    val unique = property.findAnnotation<DbUnique>()
    val index = property.findAnnotation<DbIndex>()

    val indexOrder = unique?.order ?: index?.order ?: 0

    val indexName = unique?.name?.takeIf { it.isNotEmpty() } ?: index?.name?.takeIf { it.isNotEmpty() } ?: name

    val indexDirection by lazy {
        property.findAnnotation<DbPrimary>()?.direction
                ?: property.findAnnotation<DbUnique>()?.direction
                ?: property.findAnnotation<DbIndex>()?.direction
                ?: DbIndexDirection.ASC
    }

    val migrations by lazy {
        ormTableInfo.migrations + listOfNotNull(property.findAnnotation<DbPerformMigration>())
    }
}

class OrmTableInfo<T : Any>(val dialect: SqlDialect, val clazz: KClass<T>) {
    val tableName = clazz.findAnnotation<DbName>()?.name ?: clazz.simpleName ?: error("$clazz doesn't have name")
    val columns = clazz.memberProperties.filter { it.findAnnotation<DbIgnore>() == null && !it.name.startsWith("__") }.map { ColumnDef(this, dialect, it) }
    val columnIndices = columns.filter { it.isAnyIndex }.sortedBy { it.indexOrder }.groupBy { it.indexName }
    val columnUniqueIndices = columns.filter { it.isPrimaryOrUnique }.sortedBy { it.indexOrder }.groupBy { it.indexName }
    val columnsByName = columns.associateBy { it.name }
    val migrations by lazy {
        listOfNotNull(clazz.findAnnotation<DbPerformMigration>())
    }
    fun getColumnByName(name: String) = columnsByName[name]
    fun getColumnByProp(prop: KProperty1<T, *>) = getColumnByName(prop.name)
}
