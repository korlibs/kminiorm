package com.soywiz.kminiorm.convert

import com.soywiz.kminiorm.*
import javassist.*
import java.lang.reflect.Modifier
import java.time.*
import java.util.*
import java.util.concurrent.atomic.*
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*
import kotlin.time.*

@Suppress("unused", "SameParameterValue")
abstract class Generate<T>(val clazz: KClass<*>, val factory: GenerateFactory) {
    init {
        init()
    }

    protected open fun init() {
    }

    fun convertDefault(): T = this.convert(null)
    fun convertOrNull(value: Any?): T? = value?.let { this.convert(value) }
    abstract fun convert(data: Any?): T

    fun convertFromMapOrNull(data: Map<String?, Any?>?): T? = data?.let { convertFromMap(data) }
    open fun convertFromMap(data: Map<String?, Any?>): T = this.convert(data)

    fun ensureUnit(value: Any?, allowNull: Boolean) = if (allowNull && value == null) null else Unit

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

    fun ensureBooleanArray(value: Any?, allowNull: Boolean): BooleanArray? {
        if (value == null && allowNull) return null
        if (value is BooleanArray) return value
        if (value is Iterable<*>) return value.map { ensureBooleanNotNull(it) }.toBooleanArray()
        return BooleanArray(0)
    }

    fun ensureByteArray(value: Any?, allowNull: Boolean): ByteArray? {
        if (value == null && allowNull) return null
        if (value is ByteArray) return value
        if (value is Iterable<*>) return value.map { ensureByteNotNull(it) }.toByteArray()
        return ByteArray(0)
    }

    fun ensureShortArray(value: Any?, allowNull: Boolean): ShortArray? {
        if (value == null && allowNull) return null
        if (value is ShortArray) return value
        if (value is Iterable<*>) return value.map { ensureShortNotNull(it) }.toShortArray()
        return ShortArray(0)
    }

    fun ensureCharArray(value: Any?, allowNull: Boolean): CharArray? {
        if (value == null && allowNull) return null
        if (value is CharArray) return value
        if (value is Iterable<*>) return value.map { ensureCharNotNull(it) }.toCharArray()
        return CharArray(0)
    }

    fun ensureIntArray(value: Any?, allowNull: Boolean): IntArray? {
        if (value == null && allowNull) return null
        if (value is IntArray) return value
        if (value is Iterable<*>) return value.map { ensureIntNotNull(it) }.toIntArray()
        return IntArray(0)
    }

    fun ensureLongArray(value: Any?, allowNull: Boolean): LongArray? {
        if (value == null && allowNull) return null
        if (value is LongArray) return value
        if (value is Iterable<*>) return value.map { ensureLongNotNull(it) }.toLongArray()
        return LongArray(0)
    }

    fun ensureFloatArray(value: Any?, allowNull: Boolean): FloatArray? {
        if (value == null && allowNull) return null
        if (value is FloatArray) return value
        if (value is Iterable<*>) return value.map { ensureFloatNotNull(it) }.toFloatArray()
        return FloatArray(0)
    }

    fun ensureDoubleArray(value: Any?, allowNull: Boolean): DoubleArray? {
        if (value == null && allowNull) return null
        if (value is DoubleArray) return value
        if (value is Iterable<*>) return value.map { ensureDoubleNotNull(it) }.toDoubleArray()
        return DoubleArray(0)
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
        is Boolean -> if (value) 1 else 0
        else -> value.toString().toLongOrNull() ?: 0L
    }
    fun ensureNumberNotNull(value: Any?): Number = when (value) {
        null -> 0.0
        is Number -> value
        is Boolean -> if (value) 1.0 else 0.0
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

        return MutableList(list.size) { clazzFactory.convert(list[it]) } as List<T>
    }

    fun <T : Any> generateListForGenerator(clazzFactory: Generate<T>, value: Any?, allowNull: Boolean): List<T>? {
        if (value == null) return if (allowNull) null else arrayListOf()
        val list = value as Iterable<*>
        return list.map { clazzFactory.convert(it) }.toMutableList()
    }

    fun <T : Any> generateArrayForGenerator(clazzFactory: Generate<T>, value: Any?, allowNull: Boolean): Array<T>? {
        return generateListForGenerator(clazzFactory, value, allowNull)?.toTypedArray<Any?>() as? Array<T>?
    }

    fun <T : Any> generateForClass(clazz: Class<T>, value: Any?, allowNull: Boolean): T? {
        if (value == null) return if (allowNull) null else factory.get(clazz).convert(value) as T
        return factory.get(clazz).convert(value) as T
    }

    fun <T : Any> getClassGenerator(clazz: KClass<T>): Generate<T> = factory.get(clazz)

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
    val debug: Boolean = false,
    val customConverters: ((clazz: KClass<*>) -> ((value: Any?) -> Any)?) = { null }
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

    inline fun <reified T : Any> createGenerate(noinline block: (data: Any?) -> T): Generate<T> = createGenerate(T::class, block)
    fun <T : Any> createGenerate(clazz: KClass<T>, block: (data: Any?) -> T): Generate<T> = object : Generate<T>(clazz, this@GenerateFactory) {
        override fun convert(data: Any?): T = block(data) as T
    }

    fun <T : Any> get(clazz: KType): Generate<T> = this.get(clazz.jvmErasure as KClass<T>)
    fun <T : Any> get(clazz: Class<T>): Generate<T> = this.get(clazz.kotlin)
    fun <T : Any> get(clazz: KClass<T>): Generate<T> = cache.getOrPut(clazz) {
        val converter = customConverters(clazz)
        when {
            converter != null -> {
                if (debug) {
                    System.err.println("Custom converter for class $clazz")
                }
                createGenerate(clazz) { converter(it) as T }
            }
            clazz == Date::class -> {
                createGenerate(clazz as KClass<Date>) {
                    when (it) {
                        is Number -> Date(it.toLong())
                        is String -> Date(it)
                        is Date -> it
                        else -> Date(0L)
                    }
                }
            }
            clazz == LocalDate::class -> {
                createGenerate(clazz as KClass<LocalDate>) {
                    when (it) {
                        is Number -> Date(it.toLong()).toInstant().atZone(ZoneId.of("UTC")).toLocalDate()
                        is String -> LocalDate.parse(it)
                        is LocalDate -> it
                        else -> LocalDate.ofEpochDay(0L)
                    }
                }
            }
            else -> {
                GenerateClass(clazz).generateUncached()
            }
        }
    } as Generate<T>

    fun CtClass.addNewField(build: StringBuilder.() -> Unit) {
        val cc = this
        val body = StringBuilder().apply(build).toString()
        if (debug) {
            System.err.println(body)
        }
        cc.addField(CtField.make(body, cc))
    }

    fun CtClass.addNewMethod(build: StringBuilder.() -> Unit) {
        val cc = this
        val body = StringBuilder().apply(build).toString()
        if (debug) {
            System.err.println(body)
        }
        cc.addMethod(CtNewMethod.make(body, cc))
    }

    inline fun <reified T : Any> createDefault(): T = get(T::class.java).convertDefault()

    inner class GenerateClass<T : Any>(val clazz: KClass<T>) {
        val cc = pool.makeClass("___Generate${id.getAndIncrement()}", pool.getCtClass(Generate::class.java.name))
        val clazzFQName = clazz.java.name
        val constructor by lazy {
            clazz.primaryConstructor
                ?: clazz.constructors.filter { Modifier.isPublic(it.javaConstructor?.modifiers ?: 0) }.sortedBy { it.parameters.size }.firstOrNull()
                //?: clazz.constructors.first()
                ?: error("Class $clazz doesn't have public constructors")
        }
        var lastMethodId = 0
        var lastFieldId = 0
        val generatorFieldNames = LinkedHashMap<KClass<*>, String>()
        fun getGeneratorFieldName(type: KClass<*>) = generatorFieldNames.getOrPut(type) {
            val fieldName = "__f${lastFieldId++}"
            cc.addNewField { appendln("public ${Generate::class.java.name} $fieldName; // $type") }
            fieldName
        }

        fun generateUncached(): Generate<T> {

            if (debug) {
                System.err.println("######### Building generator for $clazzFQName...")
                System.err.println("#########")
            }

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
                clazz.isSubclassOf(List::class) || clazz.isSubclassOf(Map::class) -> {
                    cc.addNewMethod {
                        appendln("$convertDesc {")
                        appendln("  return ${castAnyToType(PARAM, clazz.starProjectedType)};");
                        appendln("}")
                    }
                }
                else -> {
                    cc.addNewMethod {
                        appendln("$convertFromMapDesc {")
                        run {
                            appendln("  return new $clazzFQName(")
                            appendln("" + constructor.valueParameters.joinToString(",\n") { param ->
                                "    " + castAnyToType("($PARAM.get(\"${param.name}\"))", param.type)
                            })
                            appendln("  );")
                        }
                        appendln("}")
                    }
                    cc.addNewMethod {
                        appendln("$convertDesc {")
                        run {
                            appendln("  if ($PARAM instanceof java.util.Map) return $convertFromMapName((java.util.Map)$PARAM);")
                            appendln("  return $convertFromMapName(new java.util.LinkedHashMap());")
                        }
                        appendln("}")
                    }
                }
            }

            cc.addNewMethod {
                appendln("protected void init() {")
                for ((type, fieldName) in generatorFieldNames) {
                    if (type == clazz) {
                        appendln("  $fieldName = this;")
                    } else {
                        appendln("  $fieldName = this.getFactory().get(${type.java.name}.class);")
                    }
                }
                appendln("}")
            }
            return cc.toClass().declaredConstructors.first().newInstance(clazz, this@GenerateFactory) as Generate<T>
        }

        fun Class<*>.toJavaName(): String = when {
            this.isArray -> this.componentType.toJavaName() + "[]"
            else -> this.canonicalName ?: this.name
        }

        fun castAnyToType(obj: String, type: KType): String {
            val nullable = type.isMarkedNullable
            val clazz = type.jvmErasure.java

            val clazzName = clazz.toJavaName()
            val OrNull = if (nullable) "OrNull" else ""
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
                BooleanArray::class.javaObjectType -> "(boolean[])${Generate<*>::ensureBooleanArray.name}($obj, $allowNull)"
                ByteArray::class.javaObjectType -> "(byte[])${Generate<*>::ensureByteArray.name}($obj, $allowNull)"
                ShortArray::class.javaObjectType -> "(short[])${Generate<*>::ensureShortArray.name}($obj, $allowNull)"
                CharArray::class.javaObjectType -> "(char[])${Generate<*>::ensureCharArray.name}($obj, $allowNull)"
                IntArray::class.javaObjectType -> "(int[])${Generate<*>::ensureIntArray.name}($obj, $allowNull)"
                LongArray::class.javaObjectType -> "(long[])${Generate<*>::ensureLongArray.name}($obj, $allowNull)"
                FloatArray::class.javaObjectType -> "(float[])${Generate<*>::ensureFloatArray.name}($obj, $allowNull)"
                DoubleArray::class.javaObjectType -> "(double[])${Generate<*>::ensureDoubleArray.name}($obj, $allowNull)"
                else -> {
                    when {
                        Map::class.java.isAssignableFrom(clazz) -> {
                            val elementClassK = type.arguments.getOrNull(0)?.type?.jvmErasure?.java ?: Any::class.java
                            val elementClassV = type.arguments.getOrNull(1)?.type?.jvmErasure?.java ?: Any::class.java
                            "($clazzName)generateMapForClass(${elementClassK.name}.class, ${elementClassV.name}.class, $obj, $allowNull)"
                        }
                        List::class.java.isAssignableFrom(clazz) -> {
                            val elementType = type.arguments.getOrNull(0)?.type ?: Any::class.starProjectedType
                            val elementClass = elementType.jvmErasure
                            "($clazzName)generateListForGenerator(${getGeneratorFieldName(elementClass)}, $obj, $allowNull)"
                        }
                        clazz.isArray -> {
                            val elementClass = clazz.componentType.kotlin
                            "($clazzName)generateArrayForGenerator(${getGeneratorFieldName(elementClass)}, $obj, $allowNull)"
                        }
                        else -> {
                            "($clazzName)${getGeneratorFieldName(clazz.kotlin)}.convert$OrNull($obj)"
                        }
                    }
                }
            }
        }
    }
}

enum class MyEnum { A, B, C }

@UseExperimental(ExperimentalTime::class)
fun main() {

    val factory = GenerateFactory(
        debug = true,
        customConverters = { clazz ->
            when (clazz) {
                Date::class -> {
                    {
                        when (it) {
                            is Number -> Date(it.toLong())
                            is String -> Date(it)
                            is Date -> it
                            else -> Date(0L)
                        }
                    }
                }
                LocalDate::class -> {
                    {
                        when (it) {
                            is Number -> Date(it.toLong()).toInstant().atZone(ZoneId.of("UTC")).toLocalDate()
                            is String -> LocalDate.parse(it)
                            is LocalDate -> it
                            else -> LocalDate.ofEpochDay(0L)
                        }
                    }
                }
                else -> {
                    null
                }
            }
        }
    )

    data class Demo(val date: Date, val demo: Demo?, val items: List<Int>, val list: IntArray, val map: Map<String, Long>)
    //data class Demo(val date: Date, val demo: Demo?, val items: List<Int>, val list: IntArray)

    println(factory.createDefault<Byte>())
    println(factory.createDefault<Boolean>())
    println(factory.createDefault<Float>())
    println(factory.createDefault<Double>())
    println(factory.createDefault<Byte>())
    println(factory.createDefault<Short>())
    println(factory.createDefault<Char>())
    println(factory.createDefault<Int>())
    println(factory.createDefault<Long>())
    println(factory.createDefault<String>())
    println(factory.createDefault<List<Int>>())
    println(factory.createDefault<Map<Int, String>>())
    println(factory.createDefault<DbRef<*>>())
    println(factory.createDefault<DbKey>())
    println(factory.createDefault<DbIntRef<*>>())
    println(factory.createDefault<DbStringRef<*>>())
    println(factory.createDefault<DbIntKey>())
    println(factory.createDefault<Date>())
    println(factory.createDefault<LocalDate>())
    //println(factory.get<Date>().convert(10000000000000L))
    println(factory.get<Demo>().convert(mapOf(
        "date" to 10000000000000L,
        "map" to mapOf("hello" to "1000"),
        "items" to listOf("1", 2, "3", true, false),
        "list" to listOf("1", 2, "3", true, false),
        "demo" to mapOf("date" to 1000))
    ))
    println(factory.get<String>().convertDefault())
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world")))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to "test")))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to "1000")))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to 1000)))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to 1000, "clazz" to mapOf("a" to "hello"))))
    println(factory.get<MyClass>().convert(mapOf("a" to "hello", "b" to "world", "c" to 1000, "clazz" to mapOf("a" to "hello"), "other" to mapOf("a" to 10))))
    println(factory.get<MyClass>().convertFromMap(mapOf()))
    println(factory.get<Other2>().convert(mapOf("date" to listOf(2000000000000L, 999999999999L))))
    println(factory.get<Other3>().convert(mapOf("date" to mapOf("" to 2000000000000L))))
    //MyEnum.values().first()
    println(factory.get<Boolean>().convertDefault())
    println(factory.get<Int>().convertDefault())
    //println(factory.get<MyEnum>().createDefault())
    println(factory.get<Boolean>().convert(true))
    println(factory.get<Boolean>().convert(1))
    println(factory.get<Int>().convert("hello"))
    println(factory.get<Int>().convert("100"))
    println(factory.get<MyEnum>().convertDefault())
    println(factory.get<MyEnum>().convert(null))
    println(factory.get<MyEnum>().convert(MyEnum.B))
    println(factory.get<MyEnum>().convert("C"))
    println(factory.get<String>().convert(10))
    println(factory.get<String>().convert(null))
    println(factory.get<String>().convert(true))
    println(factory.get<String>().convert(false))
    println(factory.get<String>().convert("hello"))
    println(factory.get<String>().convert(mapOf("a" to "b")))
    /*
    val mapper = ObjectMapper()
    mapper.registerModule(KotlinModule())
    mapper.registerModule(AfterburnerModule())
    mapper.convertValue(mapOf("date" to listOf(Date(), Date())), Other3::class.java)
     */
}
