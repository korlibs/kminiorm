package com.soywiz.kminiorm

import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.module.kotlin.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

val KotlinMapper by lazy { ObjectMapper().registerModule(KotlinModule()) }

fun ObjectMapper.convertValueToMap(value: Any) = convertValue(value, Map::class.java) as Map<String, Any?>
inline fun <reified T> ObjectMapper.toType(data: Any) = convertValue(data, T::class.java)

fun ObjectMapper.createDefault(type: KType): Any? {
    val clazz = type.jvmErasure
    return when (clazz) {
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
        else -> {
            val constructor = clazz.constructors.firstOrNull() ?: error("Class $clazz doesn't have constructors")
            constructor.call(*constructor.valueParameters.map { createDefault(it.type) }.toTypedArray())
        }
    }
}