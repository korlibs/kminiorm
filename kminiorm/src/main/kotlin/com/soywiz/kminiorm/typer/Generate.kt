package com.soywiz.kminiorm.typer

import javassist.*
import java.lang.reflect.Modifier
import java.util.*
import java.util.concurrent.atomic.*
import kotlin.collections.HashMap
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*
import kotlin.time.*

@Suppress("unused", "SameParameterValue")
abstract class Generate<T>(val clazz: KClass<*>, val factory: GenerateFactory) {
    fun convertDefault(): T = this.convert(null)
    fun convertOrNull(value: Any?): T? = value?.let { this.convert(value) }
    abstract fun convert(data: Any?): T

    fun convertFromMapOrNull(data: Map<String?, Any?>?): T? = data?.let { convertFromMap(data) }
    open fun convertFromMap(data: Map<String?, Any?>): T = this.convert(data)

    fun ensureUnit(value: Any?, allowNull: Boolean) = if (allowNull && value == null) null else Unit

    fun ensureDate(value: Any?, allowNull: Boolean): Date? = when(value) {
        null -> if (allowNull) null else Date()
        is Date -> value
        is Number -> Date(value.toLong())
        else -> Date(Date.parse(value.toString()))
    }

    fun ensureBoolean(value: Any?, allowNull: Boolean): Boolean? = when(value) {
        null -> if (allowNull) null else false
        is Boolean -> value
        is Number -> value.toInt() != 0
        else -> when (value.toString()) {
            "true" -> true
            "1" -> true
            else -> false
        }
    }
    fun ensureByte(value: Any?, allowNull: Boolean): Byte? = if (value is Byte) value else ensureNumber(value, allowNull)?.toByte()
    fun ensureChar(value: Any?, allowNull: Boolean): Char? = if (value is Char) value else ensureNumber(value, allowNull)?.toChar()
    fun ensureShort(value: Any?, allowNull: Boolean): Short? = ensureNumber(value, allowNull)?.toShort()
    fun ensureInt(value: Any?, allowNull: Boolean): Int? = ensureNumber(value, allowNull)?.toInt()
    fun ensureFloat(value: Any?, allowNull: Boolean): Float? = ensureNumber(value, allowNull)?.toFloat()
    fun ensureDouble(value: Any?, allowNull: Boolean): Double? = ensureNumber(value, allowNull)?.toDouble()
    fun ensureLong(value: Any?, allowNull: Boolean): Long? = when (value) {
        null -> if (allowNull) null else 0L
        else -> ensureLongNotNull(value)
    }

    fun ensureNumber(value: Any?, allowNull: Boolean): Number? = when (value) {
        null -> if (allowNull) null else 0.0
        else -> ensureNumberNotNull(value)
    }

    //////////

    fun ensureBooleanNotNull(value: Any?): Boolean = when(value) {
        null -> false
        is Boolean -> value
        is Number -> value.toInt() != 0
        else -> when (value.toString()) {
            "true" -> true
            "1" -> true
            else -> false
        }
    }
    fun ensureByteNotNull(value: Any?): Byte = if (value is Byte) value else ensureNumberNotNull(value).toByte()
    fun ensureCharNotNull(value: Any?): Char = if (value is Char) value else ensureNumberNotNull(value).toChar()
    fun ensureShortNotNull(value: Any?): Short = ensureNumberNotNull(value).toShort()
    fun ensureIntNotNull(value: Any?): Int = ensureNumberNotNull(value).toInt()
    fun ensureFloatNotNull(value: Any?): Float = ensureNumberNotNull(value).toFloat()
    fun ensureDoubleNotNull(value: Any?): Double = ensureNumberNotNull(value).toDouble()
    fun ensureLongNotNull(value: Any?): Long = when (value) {
        null -> 0L
        is Long -> value
        is Number -> value.toLong()
        else -> value.toString().toLongOrNull() ?: 0L
    }
    fun ensureNumberNotNull(value: Any?): Number = when (value) {
        null -> 0.0
        is Number -> value
        else -> value.toString().toDoubleOrNull() ?: 0.0
    }

    //////////

    fun ensureString(value: Any?, allowNull: Boolean): String? = if (value == null && allowNull) null else value.toString()


    fun <K : Any, V: Any> generateMapForClass(clazzK: Class<K>, clazzV: Class<V>, value: Any?, allowNull: Boolean): Map<K, V>? {
        if (value == null) return if (allowNull) null else mutableMapOf()
        val clazzFactoryK = factory.get(clazzK)
        val clazzFactoryV = factory.get(clazzV)
        val map = (value as Map<*, *>)
        return map.map { clazzFactoryK.convert(it.key) to clazzFactoryV.convert(it.value) }.toMap().toMutableMap() as Map<K, V>
    }

    fun <T : Any> generateListForClass(clazz: Class<T>, value: Any?, allowNull: Boolean): List<T>? {
        if (value == null) return if (allowNull) null else arrayListOf()
        val clazzFactory = factory.get(clazz)
        val list = value as List<*>

        return when (clazz) {
            //Date::class.java -> MutableList(list.size) { clazzFactory.ensureDate(list[it], false) } as List<T>
            else -> MutableList(list.size) { clazzFactory.convert(list[it]) } as List<T>
        }
    }

    fun <T : Any> generateForClass(clazz: Class<T>, value: Any?, allowNull: Boolean): T? {
        if (value == null) return if (allowNull) null else factory.get(clazz).convert(value) as T
        return factory.get(clazz).convert(value) as T
    }

    fun ensureObject(value: Any?): Any? = value
    fun ensureObject(value: Boolean): Any = value
    fun ensureObject(value: Byte): Any = value
    fun ensureObject(value: Char): Any = value
    fun ensureObject(value: Short): Any = value
    fun ensureObject(value: Int): Any = value
    fun ensureObject(value: Long): Any = value
    fun ensureObject(value: Float): Any = value
    fun ensureObject(value: Double): Any = value
}

data class Other3(val date: Map<String, Date>)
data class Other2(val date: List<Date>)
data class Other(val a: Int)
data class MyClass(val clazz: MyClass?, val a: String, val b: String, val c: Int? = null, val other: Other)

open class GenerateFactory(
    val debug: Boolean = false
) {
    companion object {
        private var id = AtomicInteger(0)
    }
    private val pool = ClassPool.getDefault()
    private val cache = HashMap<KClass<*>, Generate<*>>()
    private val PARAM: String = "data"
    //private val ENSURE_OBJECT = Generate<*>::ensureObject.name
    private val ENSURE_OBJECT = "ensureObject"

    val convertName = Generate<*>::convert.name
    val convertFromMapName = Generate<*>::convertFromMap.name

    val convertDesc = "public Object ${convertName}(Object $PARAM)"
    val convertFromMapDesc = "public Object ${convertFromMapName}(java.util.Map $PARAM)"

    private val primitiveClasses = setOf(
        Boolean::class, Byte::class, Char::class,
        Short::class, Int::class, Long::class,
        Float::class, Double::class
    )

    inline fun <reified T : Any> get(): Generate<T> = get(T::class)

    fun <T : Any> get(clazz: Class<T>): Generate<T> = this.get(clazz.kotlin)
    fun <T : Any> get(clazz: KClass<T>): Generate<T> = cache.getOrPut(clazz) { GenerateClass(clazz).generateUncached() } as Generate<T>

    fun CtClass.addNewMethod(build: StringBuilder.() -> Unit) {
        val cc = this
        val body = StringBuilder().apply(build).toString()
        if (debug) {
            System.err.println(body)
        }
        cc.addMethod(CtNewMethod.make(body, cc))
    }

    inner class GenerateClass<T : Any>(val clazz: KClass<T>) {
        val clazzFQName = clazz.qualifiedName

        fun generateUncached(): Generate<T> {
            val constructor = clazz.primaryConstructor
                ?: clazz.constructors.filter { Modifier.isPublic(it.javaConstructor?.modifiers ?: 0) }.sortedBy { it.parameters.size }.firstOrNull()
                //?: clazz.constructors.first()
                ?: error("Class $clazz doesn't have public constructors")

            val cc = pool.makeClass("___Generate${id.getAndIncrement()}", pool.getCtClass(Generate::class.java.name))

            when {
                clazz.java.isPrimitive || clazz in primitiveClasses -> {
                    cc.addNewMethod {
                        appendln("$convertDesc {")
                        appendln("  return $ENSURE_OBJECT(" + castAnyToType(PARAM, clazz.starProjectedType) + ");");
                        appendln("}")
                    }
                }
                clazz.java.isEnum -> {
                    cc.addNewMethod {
                        appendln("$convertDesc {")
                        appendln("  if ($PARAM != null) {");
                        appendln("    if ($PARAM instanceof $clazzFQName) return ($clazzFQName)$PARAM;");
                        appendln("    if ($PARAM instanceof String) return $clazzFQName.valueOf((String)$PARAM);");
                        appendln("  }");
                        appendln("  return $clazzFQName.values()[0];");
                        appendln("}")
                    }
                }
                clazz == String::class -> {
                    cc.addNewMethod {
                        appendln("$convertDesc {")
                        appendln("  if ($PARAM == null) return \"\";");
                        appendln("  return $PARAM.toString();");
                        appendln("}")
                    }
                }
                else -> {
                    cc.addNewMethod {
                        appendln("$convertFromMapDesc {")
                        run {
                            appendln("return new $clazzFQName(")
                            appendln(constructor.valueParameters.joinToString(", ") { param ->
                                castAnyToType("($PARAM.get(\"${param.name}\"))", param.type)
                            })
                            appendln(");")
                        }
                        appendln("}")
                    }
                    cc.addNewMethod {
                        appendln("$convertDesc {")
                        run {
                            appendln("if ($PARAM instanceof java.util.Map) return $convertFromMapName((java.util.Map)$PARAM);")
                            appendln("return $convertFromMapName(new java.util.LinkedHashMap());")
                        }
                        //appendln("return $ENSURE_OBJECT(" + castAnyToType(PARAM, clazz.starProjectedType) + ");");
                        appendln("}")
                    }
                }
            }
            return cc.toClass().declaredConstructors.first().newInstance(clazz, this@GenerateFactory) as Generate<T>
        }

        fun castAnyToType(obj: String, type: KType): String {
            val nullable = type.isMarkedNullable
            val clazz = type.jvmErasure.java
            val allowNull = if (nullable) "true" else "false"
            fun gen(func: KFunction<*>) = "${func.name}($obj, $allowNull)"
            fun gen2(func: KFunction<*>, func2: KFunction<*>) = if (nullable) "${func.name}($obj, true)" else "${func2.name}($obj)"
            return when (clazz) {
                Unit::class.javaObjectType -> gen(Generate<*>::ensureUnit)
                Boolean::class.javaPrimitiveType, Boolean::class.javaObjectType -> gen2(Generate<*>::ensureBoolean, Generate<*>::ensureBooleanNotNull)
                Byte::class.javaPrimitiveType, Byte::class.javaObjectType -> gen2(Generate<*>::ensureByte, Generate<*>::ensureByteNotNull)
                Char::class.javaPrimitiveType, Char::class.javaObjectType -> gen2(Generate<*>::ensureChar, Generate<*>::ensureCharNotNull)
                Short::class.javaPrimitiveType, Short::class.javaObjectType -> gen2(Generate<*>::ensureShort, Generate<*>::ensureShortNotNull)
                Int::class.javaPrimitiveType, Int::class.javaObjectType -> gen2(Generate<*>::ensureInt, Generate<*>::ensureIntNotNull)
                Float::class.javaPrimitiveType, Float::class.javaObjectType -> gen2(Generate<*>::ensureFloat, Generate<*>::ensureFloatNotNull)
                Double::class.javaPrimitiveType, Double::class.javaObjectType -> gen2(Generate<*>::ensureDouble, Generate<*>::ensureDoubleNotNull)
                Long::class.javaPrimitiveType, Long::class.javaObjectType -> gen2(Generate<*>::ensureLong, Generate<*>::ensureLongNotNull)
                String::class.javaObjectType -> gen(Generate<*>::ensureString)
                Date::class.javaObjectType -> gen(Generate<*>::ensureDate)
                else -> {
                    when {
                        Map::class.java.isAssignableFrom(clazz) -> {
                            val elementClassK = type.arguments.first().type!!.jvmErasure.java
                            val elementClassV = type.arguments.drop(1).first().type!!.jvmErasure.java
                            "(${clazz.name})generateMapForClass(${elementClassK.name}.class, ${elementClassV.name}.class, $obj, $allowNull)"
                        }
                        List::class.java.isAssignableFrom(clazz) -> {
                            val elementClass = type.arguments.first().type!!.jvmErasure.java
                            "(${clazz.name})generateListForClass(${elementClass.name}.class, $obj, $allowNull)"
                        }
                        else -> {
                            "(${clazz.name})generateForClass(${clazz.name}.class, $obj, $allowNull)"
                        }
                    }
                }
            }
        }

        fun defaultLiteralString(type: KType): String {
            if (type.isMarkedNullable) return "null"
            return defaultLiteralString(type.jvmErasure.java)
        }

        fun defaultLiteralString(clazz: Class<*>): String {
            return when (clazz) {
                Boolean::class.javaPrimitiveType -> "false"
                Int::class.javaPrimitiveType -> "0"
                Float::class.javaPrimitiveType -> "0f"
                Double::class.javaPrimitiveType -> "0.0"
                String::class.javaObjectType -> "\"\""
                Date::class.javaObjectType -> "new java.util.Date()"
                else -> {
                    when {
                        clazz.isAssignableFrom(Map::class.java) -> "new java.util.HashMap()"
                        clazz.isAssignableFrom(List::class.java) -> "new java.util.List()"
                        else -> TODO("$clazz")
                    }
                }
            }
        }
    }
}

enum class MyEnum { A, B, C }

@UseExperimental(ExperimentalTime::class)
fun main() {

    val factory = GenerateFactory(debug = true)
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world")))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to "test")))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to "1000")))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to 1000)))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to 1000, "clazz" to mapOf("a" to "hello"))))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to 1000, "clazz" to mapOf("a" to "hello"), "other" to mapOf("a" to 10))))
    println(factory.get<MyClass>().convertFromMap(mapOf()))
    println(factory.get<Other2>().convert(mapOf("date" to listOf(2000000000000L, 999999999999L))))
    println(factory.get<Other3>().convert(mapOf("date" to mapOf("" to 2000000000000L))))
    /*
    */
    //MyEnum.values().first()
    /*
    println(factory.get<Boolean>().generateDefault())
    println(factory.get<Int>().generateDefault())
     */
    //println(factory.get<MyEnum>().generateDefault())
    /*
    println(factory.get<Boolean>().convert(true))
    println(factory.get<Boolean>().convert(1))
    println(factory.get<Int>().convert("hello"))
    println(factory.get<Int>().convert("100"))
    println(factory.get<MyEnum>().generateDefault())
    println(factory.get<MyEnum>().convert(null))
    println(factory.get<MyEnum>().convert(MyEnum.B))
    println(factory.get<MyEnum>().convert("C"))
    println(factory.get<String>().convert(10))
    println(factory.get<String>().convert(null))
    println(factory.get<String>().convert(true))
    println(factory.get<String>().convert(false))
    println(factory.get<String>().convert("hello"))
    println(factory.get<String>().convert(mapOf("a" to "b")))
    */
    /*
    val mapper = ObjectMapper()
    mapper.registerModule(KotlinModule())
    mapper.registerModule(AfterburnerModule())
    mapper.convertValue(mapOf("date" to listOf(Date(), Date())), Other3::class.java)
     */
}
