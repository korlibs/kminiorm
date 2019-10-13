package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

data class Partial<T : Any>(val data: Map<String, Any?>, val clazz: KClass<T>) {
    fun <V> has(key: KProperty1<T, V>): Boolean = data.containsKey(key.name)
    operator fun <V> get(key: KProperty1<T, V>): V? = data[key.name] as? V?
    fun <V> without(key: KProperty1<T, V>) = Partial(data - key.name, clazz)
    fun <V> without(vararg key: KProperty1<T, V>) = Partial(data - key.map { it.name }.toSet(), clazz)
    fun <V> with(key: KProperty1<T, V>, value: V) = Partial(data + mapOf(key.name to value), clazz)
    fun <V> withOnly(vararg key: KProperty1<T, V>): Partial<T> {
        val valid = key.map { it.name }.toSet()
        return Partial(data.filterKeys { it in valid }, clazz)
    }

    val partial by lazy { PartialCombiner(clazz).create(this) }

    companion object {
        inline fun <reified T : Any> combine(item: T, partial: Partial<T>) =
            combine(item, partial, T::class)

        fun <T : Any> combine(item: T, partial: Partial<T>, clazz: KClass<T>): T {
            return PartialCombiner(clazz).combine(item, partial)
        }
    }
}

class PartialCombiner<T : Any>(val clazz: KClass<T>) {
    private val membersByName by lazy { clazz.members.filterIsInstance<KProperty1<T, *>>().associateBy { it.name } }
    val constructor by lazy { clazz.primaryConstructor ?: error("Class $clazz doesn't have a primary constructor") }

    fun createEmpty(): T = DbTyper.createDefault(clazz.starProjectedType) as T

    fun create(partial: Partial<T>): T = constructor.call(*constructor.valueParameters.map {
        val member = membersByName[it.name] ?: error("Unmatched property ${it.name}")
        if (partial.has(member)) partial[member] else DbTyper.createDefault(member.returnType)
    }.toTypedArray())

    fun combine(item: T, partial: Partial<T>): T = constructor.call(*constructor.valueParameters.map {
        val member = membersByName[it.name] ?: error("Unmatched property ${it.name}")
        if (partial.has(member)) partial[member] else member.get(item)
    }.toTypedArray())
}

inline operator fun <reified T : Any> T.plus(partial: Partial<T>): T =
    Partial.combine(this, partial, T::class)

class PartialBuilder<T> {
    val data = LinkedHashMap<String, Any?>()
    fun <V> add(item: KProperty1<T, V>, value: V) = this.apply { data[item.name] = value }
}

inline fun <reified T : Any> Partial(builder: PartialBuilder<T>.() -> Unit): Partial<T> {
    val builder = PartialBuilder<T>()
    builder(builder)
    return Partial(builder.data, T::class)
}

inline fun <reified T : Any> Partial(vararg items: Pair<KProperty1<T, *>, Any>): Partial<T> =
    Partial(items.associate { it.first.name to it.second }, T::class)

fun <T : Any> Partial(value: T, clazz: KClass<out T> = value::class): Partial<T> =
        Partial((clazz as KClass<T>).members.filterIsInstance<KProperty1<T, *>>().associate { it.name to it.get(value) }, (clazz as KClass<T>))

inline fun <reified T : Any> Partial(value: T): Partial<T> = Partial(value, T::class)

fun Typer.withPartialTyper() = withTyperUntyper<Partial<*>>(
    typer = { it, type ->
        if (it is Partial<*>) {
            it
        } else {
            val map: Map<String, Any?> = when (it) {
                is Partial<*> -> it.data
                is Map<*, *> -> it as Map<String, Any?>
                is Iterable<*> -> (it as Iterable<Map.Entry<String, Any?>>).toList().associate { it.key to it.value }
                else -> mapOf()
            }
            PartialUntyped<Any>(this, map as Map<Any?, Any?>, type)
        }
    },
    untyper = { it.data }
)

fun <T : Any> PartialUntyped(typer: Typer, map: Map<Any?, Any?>, type: KType): Partial<T> {
    val clazz = type.arguments.first().type?.jvmErasure ?: Any::class
    val propertiesByName = clazz.memberProperties.associateBy { it.name }
    val result = map.entries.associate {
        val property = propertiesByName[it.key.toString()]
        it.key to if (property != null) typer.type<Any?>(it.value!!, property.returnType) else null
    }
    return Partial(result as Map<String, Any?>, clazz as KClass<T>)
}
