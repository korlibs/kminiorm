package com.soywiz.kminiorm

interface ExtrinsicData {
    operator fun get(key: String): Any?
    operator fun set(key: String, value: Any?)

    open class Mixin(val data: MutableMap<String, Any?> = mutableMapOf()) : ExtrinsicData {
        override fun get(key: String): Any? = data[key]
        override fun set(key: String, value: Any?) = run { data[key] = value }
    }
}

