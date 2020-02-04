package com.soywiz.kminiorm.typer

import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.internal.*
import java.math.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

open class Typer private constructor(
    private val keepTypes: Set<KClass<*>> = setOf(),
    private val untypersByClass: Map<KClass<*>, Typer.(Any) -> Any> = mapOf(),
    private val typersByClass: Map<KClass<*>, Typer.(Any, KType) -> Any> = mapOf()
) {
    constructor() : this(keepTypes = setOf())

    private fun copy(
        keepTypes: Set<KClass<*>> = this.keepTypes,
        untypersByClass: Map<KClass<*>, Typer.(Any) -> Any> = this.untypersByClass,
        typersByClass: Map<KClass<*>, Typer.(Any, KType) -> Any> = this.typersByClass
    ) = Typer(keepTypes, untypersByClass, typersByClass)

    fun withKeepType(type: KClass<*>) = copy(keepTypes = keepTypes + type)
    inline fun <reified T> withKeepType() = withKeepType(T::class)


    fun <T : Any> withUntyper(clazz: KClass<T>, handler: Typer.(T) -> Any?) = copy(untypersByClass = untypersByClass + mapOf(clazz to (handler as Typer.(Any) -> Any)))
    fun <T : Any> withTyper(clazz: KClass<T>, handler: Typer.(Any, KType) -> T) = copy(typersByClass = typersByClass + mapOf(clazz to (handler as Typer.(Any, KType) -> Any)))

    inline fun <reified T : Any> withUntyper(noinline handler: Typer.(T) -> Any) = withUntyper(T::class, handler)
    inline fun <reified T : Any> withTyper(noinline handler: Typer.(Any, KType) -> T) = withTyper(T::class, handler)

    inline fun <reified T : Any> withTyperUntyper(noinline typer: Typer.(Any, KType) -> T, noinline untyper: Typer.(T) -> Any) = withTyper(T::class, typer).withUntyper(T::class, untyper)

    fun untype(instance: Any): Any {
        val clazz = instance::class
        if (clazz in keepTypes) return instance
        val untyper = untypersByClass[clazz]
        if (untyper != null) return untyper(instance)

        return when (instance) {
            is Number -> instance
            is Boolean -> instance
            is String -> instance
            is ByteArray -> instance.toBase64()
            is Map<*, *> -> instance.entries.associate { (key, value) -> key?.let { untype(it) } to value?.let { untype(it) } }
            is Iterable<*> -> instance.map { it?.let { untype(it) } }
            else -> {

                when {
                    clazz.java.isEnum -> (instance as Enum<*>).name
                    else -> LinkedHashMap<String, Any?>().also { out ->
                        for (prop in clazz.memberProperties.filter {
                            it.isAccessible = true; true
                        }) out[prop.name] = (prop as KProperty1<Any?, Any?>).get(instance)?.let { untype(it) }
                    }
                }
            }
        }
    }

    fun untypeNull(instance: Any?): Any? = when (instance) {
        null -> null
        else -> untype(instance)
    }

    fun <T> type(instance: Any, targetType: KType): T = _type(instance, targetType) as T

    private fun _toIterable(instance: Any): Iterable<Any?> {
        if (instance is Iterable<*>) return instance as Iterable<Any?>
        TODO()
    }

    private fun _toMap(instance: Any): Map<Any?, Any?> {
        if (instance is Map<*, *>) return instance as Map<Any?, Any?>
        if (instance is Iterable<*>) return (instance as Iterable<Map.Entry<Any?, Any?>>).toList().associate { it.key to it.value }
        return instance::class.memberProperties
            //.filter { it.isAccessible = true; true }
            .filter { it.isAccessible }
            .associate { it.name to (it as KProperty1<Any?, Any?>).get(instance) }
    }

    private fun _type(instance: Any, targetType: KType): Any? {
        val targetClass = targetType.jvmErasure
        val targetClassJava = targetClass.java

        val typer = typersByClass[targetClass]
        if (typer != null) return typer(instance, targetType)

        return when (targetClass) {
            instance::class -> instance
            Boolean::class -> when (instance) {
                is Boolean -> instance
                is Number -> instance.toDouble() != 0.0
                is String -> instance != ""
                else -> true
            }
            String::class -> instance.toString()
            ByteArray::class -> {
                when (instance) {
                    is ByteArray -> instance
                    is String -> instance.fromBase64()
                    else -> _toIterable(instance).map { (it as Number).toByte() }.toTypedArray()
                }
            }
            IntArray::class -> (instance as? IntArray) ?: _toIterable(instance).map { (it as Number).toInt() }.toTypedArray()
            LongArray::class -> (instance as? LongArray) ?: _toIterable(instance).map { (it as Number).toLong() }.toTypedArray()
            FloatArray::class -> (instance as? FloatArray) ?: _toIterable(instance).map { (it as Number).toFloat() }.toTypedArray()
            DoubleArray::class -> (instance as? DoubleArray) ?: _toIterable(instance).map { (it as Number).toDouble() }.toTypedArray()
            Any::class -> instance
            else -> when {
                targetClass.isSubclassOf(Number::class) -> when (targetClass) {
                    Byte::class -> (instance as? Number)?.toByte() ?: instance.toString().toInt().toByte()
                    Short::class -> (instance as? Number)?.toShort() ?: instance.toString().toInt().toShort()
                    Char::class -> (instance as? Number)?.toChar() ?: instance.toString().toInt().toChar()
                    Int::class -> (instance as? Number)?.toInt() ?: instance.toString().toInt()
                    Long::class -> (instance as? Number)?.toLong() ?: instance.toString().toLong()
                    Float::class -> (instance as? Number)?.toFloat() ?: instance.toString().toFloat()
                    Double::class -> (instance as? Number)?.toDouble() ?: instance.toString().toDouble()
                    BigInteger::class -> (instance as? BigInteger) ?: instance.toString().toBigInteger()
                    BigDecimal::class -> (instance as? BigDecimal) ?: instance.toString().toBigDecimal()
                    else -> TODO()
                }
                targetClass.isSubclassOf(Map::class) -> {
                    val paramKey = targetType.arguments.first().type ?: Any::class.starProjectedType
                    val paramValue = targetType.arguments.last().type ?: Any::class.starProjectedType
                    _toMap(instance).entries.associate { (key, value) ->
                        key?.let { _type(it, paramKey) } to value?.let { _type(it, paramValue) }
                    }
                }
                targetClass.isSubclassOf(Iterable::class) -> {
                    val param = targetType.arguments.first().type ?: Any::class.starProjectedType
                    val info = _toIterable(instance).map { it?.let { type<Any>(it, param) } }
                    when {
                        targetClass.isSubclassOf(List::class) -> info.toMutableList()
                        targetClass.isSubclassOf(Set::class) -> info.toMutableSet()
                        else -> error("Don't know how to convert iterable into $targetClass")
                    }
                }
                targetClassJava.isEnum -> {
                    val constants = targetClassJava.enumConstants
                    constants.firstOrNull { (it as Enum<*>).name == instance } ?: (if (targetType.isMarkedNullable) null else (constants.firstOrNull() ?: error("No constants")))
                }
                else -> {
                    val data = _toMap(instance)
                    val constructor = targetClass.primaryConstructor
                            ?: targetClass.constructors.firstOrNull { it.parameters.isNotEmpty() }
                            ?: targetClass.constructors.firstOrNull()
                    ?: error("Can't find constructor for $targetClass")

                    val processedKeys = linkedSetOf<String?>()

                    val params = constructor.parameters.map {
                        val value = data[it.name]
                        val type = it.type
                        processedKeys += it.name
                        value?.let { _type(value, type) } ?: DbTyper.createDefault(type)
                    }
                    val instance = kotlin.runCatching { constructor.call(*params.toTypedArray()) }.getOrNull()
                            ?: error("Can't instantiate object $targetClass")

                    for (prop in targetClass.memberProperties.filterIsInstance<KMutableProperty1<*, *>>()) {
                        processedKeys += prop.name
                        kotlin.runCatching { (prop as KMutableProperty1<Any?, Any?>).set(instance, data[prop.name]) }
                    }

                    if (instance is ExtrinsicData) {
                        for (key in data.keys) {
                            if (key in processedKeys) continue
                            instance[key.toString()] = data[key]
                        }
                    }

                    instance
                }
            }
        }
    }

    fun <T : Any> type(instance: Any, type: KClass<T>): T = type(instance, type.starProjectedType)

    @UseExperimental(ExperimentalStdlibApi::class)
    inline fun <reified T> type(instance: Any): T = type(instance, typeOf<T>())

    fun createDefault(type: KType): Any? {
        if (type.isMarkedNullable) return null
        val clazz = type.jvmErasure
        val jclazz = clazz.java
        return when (clazz) {
            Unit::class -> Unit
            Boolean::class -> false
            Float::class -> 0f
            Double::class -> 0.0
            Byte::class -> 0.toByte()
            Short::class -> 0.toShort()
            Char::class -> 0.toChar()
            Int::class -> 0
            Long::class -> 0L
            String::class -> ""
            List::class, ArrayList::class -> arrayListOf<Any?>()
            Map::class, HashMap::class, MutableMap::class -> mutableMapOf<Any?, Any?>()
            DbRef::class -> DbRef<DbTableElement>(ByteArray(12))
            DbKey::class -> DbKey(ByteArray(12))
            DbIntRef::class -> DbIntRef<DbTableElement>()
            DbIntKey::class -> DbIntKey(0L)
            else -> {
                if (jclazz.isEnum) {
                    jclazz.enumConstants.first()
                } else {
                    val constructor = clazz.primaryConstructor ?: clazz.constructors.firstOrNull { it.isAccessible }
                    ?: error("Class $clazz doesn't have public constructors")
                    constructor.call(*constructor.valueParameters.map { createDefault(it.type) }.toTypedArray())
                }
            }
        }
    }
}
