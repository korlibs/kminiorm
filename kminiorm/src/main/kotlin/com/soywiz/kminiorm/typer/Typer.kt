package com.soywiz.kminiorm.typer

import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.convert.*
import com.soywiz.kminiorm.internal.*
import java.math.*
import java.time.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

open class Typer private constructor(
    internal val keepTypes: Set<KClass<*>> = setOf(),
    internal val untypersByClass: Map<KClass<*>, Typer.(Any) -> Any> = mapOf(),
    internal val typersByClass: Map<KClass<*>, Typer.(Any, KType) -> Any> = mapOf(),
    //val USE_JIT: Boolean = true
    val USE_JIT: Boolean = false
) {
    companion object {
        private val EMPTY_BYTE_ARRAY = byteArrayOf()
    }
    val generateFactory: GenerateFactory by lazy { GenerateFactory() }

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

    inline fun <reified T : Any> withTyperUntyper(noinline typer: Typer.(Any, KType) -> T, noinline untyper: Typer.(T) -> Any = { it }) = withTyper(T::class, typer).withUntyper(T::class, untyper)

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
                        for (prop in clazz.memberPropertiesCached) {
                            prop.isAccessible = true
                            if (prop.findAnnotation<DbIgnore>() == null && !prop.isLateinit) {
                                out[prop.name] = (prop as KProperty1<Any?, Any?>).get(instance)?.let { untype(it) }
                            }
                        }
                    }
                }
            }
        }
    }

    fun untypeNull(instance: Any?): Any? = when (instance) {
        null -> null
        else -> untype(instance)
    }

    private fun _toIterable(instance: Any?): Iterable<Any?> {
        if (instance == null) return listOf()
        if (instance is Iterable<*>) return instance as Iterable<Any?>
        TODO()
    }

    private fun _toMap(instance: Any?): Map<Any?, Any?> {
        if (instance == null) return LinkedHashMap()
        if (instance is Map<*, *>) return instance as Map<Any?, Any?>
        if (instance is Iterable<*>) return (instance as Iterable<Map.Entry<Any?, Any?>>).toList().associate { it.key to it.value }

        val props = instance::class.memberPropertiesAccessibleCached
        val out = LinkedHashMap<Any?, Any?>(props.size)
        for (prop in props) out[prop.name] = (prop as KProperty1<Any?, Any?>).get(instance)
        return out
    }

    @OptIn(ExperimentalStdlibApi::class)
    inline fun <reified T> type(instance: Any?): T = type(instance, typeOf<T>())
    fun <T : Any> type(instance: Any?, type: KClass<T>): T = type(instance, type.starProjectedType)
    fun <T> type(instance: Any?, targetType: KType): T = _type(instance, targetType) as T

    private fun _type(instance: Any?, targetType: KType): Any? = if (USE_JIT) _typeJit(instance, targetType) else _typeReflection(instance, targetType)
    private fun _typeJit(instance: Any?, targetType: KType): Any? = generateFactory.get(targetType).convert(instance)

    private val subClassCacheNumber = HashMap<KClass<*>, Boolean>()
    private val subClassCacheMap = HashMap<KClass<*>, Boolean>()
    private val subClassCacheIterable = HashMap<KClass<*>, Boolean>()
    private val subClassCacheList = HashMap<KClass<*>, Boolean>()
    private val subClassCacheSet = HashMap<KClass<*>, Boolean>()

    private val primaryConstructorCache = HashMap<KClass<*>, KFunction<*>>()
    private val <T : Any> KClass<T>.primaryConstructorCached: KFunction<*>
        get() = primaryConstructorCache.getOrPut(this) {
            this.primaryConstructor
                ?: constructors.firstOrNull { it.isAccessible && it.parameters.isNotEmpty() }
                ?: constructors.firstOrNull { it.isAccessible }
                ?: constructors.firstOrNull()
                ?: error("Class $this doesn't have public constructors")
        }

    private val propertiesCache = HashMap<KClass<*>, List<KProperty1<*, *>>>()
    private val <T : Any> KClass<T>.memberPropertiesCached: List<KProperty1<T, *>>
        get() = propertiesCache.getOrPut(this) { this.memberProperties.toList() } as List<KProperty1<T, *>>

    private val propertiesAccessibleCache = HashMap<KClass<*>, List<KProperty1<*, *>>>()
    private val <T : Any> KClass<T>.memberPropertiesAccessibleCached: List<KProperty1<T, *>>
        get() = propertiesAccessibleCache.getOrPut(this) { this.memberPropertiesCached.filter { it.isAccessible } } as List<KProperty1<T, *>>

    private val mutablePropertiesCache = HashMap<KClass<*>, List<KMutableProperty1<*, *>>>()
    private val <T : Any> KClass<T>.memberMutablePropertiesCached: List<KMutableProperty1<T, *>>
        get() = mutablePropertiesCache.getOrPut(this) { this.memberPropertiesCached.filterIsInstance<KMutableProperty1<*, *>>().toList() } as List<KMutableProperty1<T, *>>

    private fun _typeReflection(instance: Any?, targetType: KType): Any? {
        if (targetType.isMarkedNullable && instance == null) return null
        val targetClass = targetType.jvmErasure
        val targetClassJava = targetClass.java

        val typer = typersByClass[targetClass]
        if (typer != null && instance != null) return typer(instance, targetType)

        val instanceClass = if (instance != null) instance::class else null

        return when (targetClass) {
            instanceClass -> instance
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
                subClassCacheNumber.getOrPut(targetClass) { targetClass.isSubclassOf(Number::class) } -> when (targetClass) {
                    Byte::class -> (instance as? Number)?.toByte() ?: instance.toString().toIntOrNull()?.toByte() ?: 0.toByte()
                    Short::class -> (instance as? Number)?.toShort() ?: instance.toString().toIntOrNull()?.toShort() ?: 0.toShort()
                    Char::class -> (instance as? Number)?.toChar() ?: instance.toString().toIntOrNull()?.toChar() ?: 0.toChar()
                    Int::class -> (instance as? Number)?.toInt() ?: instance.toString().toIntOrNull() ?: 0
                    Long::class -> (instance as? Number)?.toLong() ?: instance.toString().toLongOrNull() ?: 0L
                    Float::class -> (instance as? Number)?.toFloat() ?: instance.toString().toFloatOrNull() ?: 0f
                    Double::class -> (instance as? Number)?.toDouble() ?: instance.toString().toDoubleOrNull() ?: 0.0
                    BigInteger::class -> (instance as? BigInteger) ?: instance.toString().toBigInteger()
                    BigDecimal::class -> (instance as? BigDecimal) ?: instance.toString().toBigDecimal()
                    else -> TODO()
                }
                subClassCacheMap.getOrPut(targetClass) { targetClass.isSubclassOf(Map::class) } -> {
                    val paramKey = targetType.arguments.first().type ?: Any::class.starProjectedType
                    val paramValue = targetType.arguments.last().type ?: Any::class.starProjectedType
                    _toMap(instance).entries.associate { (key, value) ->
                        key?.let { _type(it, paramKey) } to value?.let { _type(it, paramValue) }
                    }
                }
                subClassCacheIterable.getOrPut(targetClass) { targetClass.isSubclassOf(Iterable::class) } -> {
                    val param = targetType.arguments.first().type ?: Any::class.starProjectedType
                    val info = _toIterable(instance).map { it?.let { type<Any>(it, param) } }
                    when {
                        subClassCacheList.getOrPut(targetClass) { targetClass.isSubclassOf(List::class) } -> info.toMutableList()
                        subClassCacheSet.getOrPut(targetClass) { targetClass.isSubclassOf(Set::class) } -> info.toMutableSet()
                        else -> error("Don't know how to convert iterable into $targetClass")
                    }
                }
                targetClassJava.isEnum -> {
                    val constants = targetClassJava.enumConstants
                    constants.firstOrNull { (it as Enum<*>).name == instance } ?: (if (targetType.isMarkedNullable) null else (constants.firstOrNull() ?: error("No constants")))
                }
                else -> {
                    val data = _toMap(instance)
                    val constructor = targetClass.primaryConstructorCached

                    val processedKeys = linkedSetOf<String?>()

                    val params = constructor.parameters.map {
                        val value = data[it.name]
                        val type = it.type
                        processedKeys += it.name
                        value?.let { _type(value, type) } ?: DbTyper.createDefault(type)
                    }
                    val instance = kotlin.runCatching { constructor.call(*params.toTypedArray()) }.getOrNull()
                            ?: error("Can't instantiate object $targetClass")

                    for (prop in targetClass.memberMutablePropertiesCached) {
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

    @OptIn(ExperimentalStdlibApi::class)
    inline fun <reified T> createDefault(): Any? = createDefault(typeOf<T>())

    fun createDefault(type: KType): Any? = if (USE_JIT) createDefaultJit(type) else createDefaultReflection(type)

    fun createDefaultJit(type: KType): Any? {
        if (type.isMarkedNullable) return null
        return generateFactory.get(type.jvmErasure.java).convertDefault()
    }

    fun createDefaultReflection(type: KType): Any? {
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
            ByteArray::class -> EMPTY_BYTE_ARRAY
            Int::class -> 0
            Long::class -> 0L
            String::class -> ""
            List::class, ArrayList::class -> arrayListOf<Any?>()
            Map::class, HashMap::class, MutableMap::class -> mutableMapOf<Any?, Any?>()
            DbRef::class -> DbRef<DbTableElement>(ByteArray(12))
            DbKey::class -> DbKey(ByteArray(12))
            DbIntRef::class -> DbIntRef<DbTableIntElement>()
            DbStringRef::class -> DbStringRef<DbTableStringElement>()
            DbIntKey::class -> DbIntKey(0L)
            Date::class -> Date(0L)
            LocalDate::class -> LocalDate.MIN
            else -> {
                if (jclazz.isEnum) {
                    jclazz.enumConstants.first()
                } else {
                    val constructor = clazz.primaryConstructorCached
                    val defaultParameters = constructor.valueParameters.map { createDefault(it.type) }.toTypedArray()
                    constructor.call(*defaultParameters)
                }
            }
        }
    }

    private val typerForClassCacheJit = HashMap<KClass<*>, ClassTyper<*>>()
    private val typerForClassCacheNoJit = HashMap<KClass<*>, ClassTyper<*>>()
    fun <T : Any> typerForClass(clazz: KClass<T>, jit: Boolean = USE_JIT): ClassTyper<T> = when {
        jit -> typerForClassCacheJit.getOrPut(clazz) { JitClassTyper(generateFactory, clazz) }
        else -> typerForClassCacheNoJit.getOrPut(clazz) {
            val ctyper = typersByClass[clazz]
            if (ctyper != null) {
                ReflectTyperClassTyper(this, clazz, ctyper)
            } else {
                ReflectClassTyper(this, clazz)
            }

        }
    } as ClassTyper<T>
}

interface ClassTyper<T: Any> {
    fun type(value: Any?): T
    fun typeOrNull(value: Any?): T? = if (value == null) null else type(value)
}

class JitClassTyper<T: Any>(val factory: GenerateFactory, val clazz: KClass<T>) : ClassTyper<T> {
    private val gen = factory.get(clazz)
    override fun type(value: Any?): T = gen.convert(value)
}

class ReflectClassTyper<T: Any>(val typer: Typer, val clazz: KClass<T>) : ClassTyper<T> {
    override fun type(value: Any?): T = typer.type<T>(value ?: Unit, clazz) as T
}

class ReflectTyperClassTyper<T: Any>(val typer: Typer, val clazz: KClass<T>, private val ctyper: ((Typer, Any, KType) -> Any)) : ClassTyper<T> {
    private val ktype = clazz.starProjectedType
    override fun type(value: Any?): T = ctyper(typer, value ?: Unit, ktype) as T
}
