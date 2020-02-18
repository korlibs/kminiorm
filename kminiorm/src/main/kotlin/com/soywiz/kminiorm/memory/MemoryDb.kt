package com.soywiz.kminiorm.memory

import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.coroutines.*
import kotlin.reflect.*

class MemoryDb(
    val typer: Typer = DbTyper,
    val dialect: SqlDialect = SqlDialect,
    override val dispatcher: CoroutineContext = Dispatchers.IO
) : AbstractDb(), DbBase, DbQuoteable by dialect {
    override fun <T : DbTableBaseElement> constructTable(clazz: KClass<T>): DbTable<T> = MemoryDbTable(this, clazz, typer)

    override suspend fun <T> transaction(callback: suspend DbBaseTransaction.() -> T): T = callback(MemoryTransaction(this))

    override suspend fun query(sql: String, vararg params: Any?): DbResult = TODO()
}

class MemoryTransaction(override val db: DbBase) : DbBaseTransaction {
    override suspend fun DbBase.commit(): Unit = Unit
    override suspend fun DbBase.rollback(): Unit = Unit
    override suspend fun query(sql: String, vararg params: Any?): DbResult = db.query(sql, *params)
}

class MemoryDbIndex<T : DbTableBaseElement>(val name: String, val columns: List<ColumnDef<T>>) {
    val items = LinkedHashMap<Any?, T>()

    fun keyForInstance(instance: T): Any? {
        return when (columns.size) {
            1 -> columns.first().property.get(instance)
            else -> columns.map { it.property.get(instance) }
        }
    }
    fun contains(instance: T): Boolean {
        return keyForInstance(instance) in items
    }
    fun insert(instance: T) {
        items[keyForInstance(instance)] = instance
    }
}

class MemoryDbTable<T : DbTableBaseElement>(
    override val db: MemoryDb,
    override val clazz: KClass<T>,
    override val typer: Typer
) : DbTable<T> {
    val ormTableInfo = OrmTableInfo(clazz)
    val instances = arrayListOf<T>()
    val uniqueIndices = ormTableInfo.columnUniqueIndices.map { MemoryDbIndex(it.key, it.value) }

    @Synchronized
    override suspend fun showColumns(): Map<String, Map<String, Any?>> {
        return ormTableInfo.columns.associate { it.name to mapOf<String, Any?>() }
    }

    @Synchronized
    override suspend fun insert(instance: T): T {
        for (index in uniqueIndices) {
            if (index.contains(instance)) {
                throw DuplicateKeyDbException("Duplicated $index")
            }
            index.insert(instance)
        }
        instances.add(instance)
        return instance
    }

    @Synchronized
    override suspend fun findFlowPartial(
        skip: Long?,
        limit: Long?,
        fields: List<KProperty1<T, *>>?,
        sorted: List<Pair<KProperty1<T, *>, Int>>?,
        query: DbQueryBuilder<T>.() -> DbQuery<T>
    ): Flow<Partial<T>> = flow {
        val realQuery = query(DbQueryBuilder.builder())
        var skipCount = skip ?: 0L
        val maxLimit = limit ?: Long.MAX_VALUE
        var emitCount = 0L
        val fieldsSet = fields?.map { it.name }?.toSet()
        val sortedInstances = if (sorted != null) {
            instances.sortedWith(object : Comparator<T> {
                override fun compare(l: T, r: T): Int {
                    for ((sort, order) in sorted) {
                        val result = (sort.get(l) as Comparable<Any?>).compareTo(sort.get(r))
                        if (order < 0) return -result
                        if (order > 0) return +result
                    }
                    return 0
                }
            })
        } else {
            instances
        }
        for (instance in sortedInstances) {
            if (!realQuery.matches(instance)) continue

            if (skipCount > 0) {
                skipCount--
                continue
            }
            val partial = Partial(instance, clazz)
            if (fieldsSet == null) {
                emit(partial)
            } else {
                emit(Partial(partial.data.filter { it.key in fieldsSet }, clazz))
            }
            emitCount++
            if (emitCount >= maxLimit) {
                break
            }
        }
    }

    @Synchronized
    override suspend fun update(set: Partial<T>?, increment: Partial<T>?, limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        val realQuery = query(DbQueryBuilder.builder())
        var updatedCount = 0L
        for (instanceIndex in instances.indices) {
            if (!realQuery.matches(instances[instanceIndex])) continue
            if (set != null) {
                instances[instanceIndex] = instances[instanceIndex] + set
            }
            if (increment != null) {
                val partial = Partial(instances[instanceIndex], clazz)
                val map = partial.data.toMutableMap()
                for ((key, inc) in increment.data) {
                    val value = map[key]
                    when (value) {
                        is Int -> map[key] = value + (inc as Number).toInt()
                        is Long -> map[key] = value + (inc as Number).toLong()
                        is Float -> map[key] = value + (inc as Number).toFloat()
                        is Double -> map[key] = value + (inc as Number).toDouble()
                        else -> TODO()
                    }
                }
                instances[instanceIndex] = Partial(map, clazz).complete
            }
            updatedCount++
        }
        return updatedCount
    }

    @Synchronized
    override suspend fun delete(limit: Long?, query: DbQueryBuilder<T>.() -> DbQuery<T>): Long {
        val realQuery = query(DbQueryBuilder.builder())

        var count = 0L
        instances
            .removeIf {
                if (limit == null || count < limit) {
                    realQuery.matches(it)
                        .also { if (it) count++ }
                } else {
                    false
                }
            }
        return count
    }
}
