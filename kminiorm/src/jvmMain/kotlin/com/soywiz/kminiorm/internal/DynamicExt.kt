package com.soywiz.kminiorm.internal

internal object DynamicExt {
    inline operator fun <T> invoke(block: DynamicExt.() -> T): T = block()

    val Any?.bool get(): Boolean = when (this) {
        null -> false
        is Boolean -> this
        is Number -> this.toInt() != 0
        is String -> this.toString().toLowerCase() == "true"
        else -> true
    }
    fun compare(a: Any?, b: Any?): Int {
        if (a == null && b == null) return 0
        if (a == null) return +1
        if (b == null) return -1
        if (a is Int && b is Int) return a.compareTo(b)
        if (a is Long && b is Long) return a.compareTo(b)
        if (a is Double && b is Double) return a.compareTo(b)
        if (a is String && b is String) return a.compareTo(b)
        if (a is Comparable<*>) return (a as Comparable<Any>).compareTo(b as Any)
        return -1
    }
}
