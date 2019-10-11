package com.soywiz.kminiorm

interface ExtrinsicData {
    @DbIgnore
    val __extrinsicData: MutableMap<String, Any?>
    operator fun get(key: String): Any? = __extrinsicData[key]
    operator fun set(key: String, value: Any?) = __extrinsicData.set(key, value)
    operator fun contains(key: String) = key in __extrinsicData

    open class Mixin(@DbIgnore override val __extrinsicData: MutableMap<String, Any?> = mutableMapOf()) : ExtrinsicData {
    }
}

