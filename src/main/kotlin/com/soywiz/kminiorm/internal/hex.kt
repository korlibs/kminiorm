package com.soywiz.kminiorm.internal

internal fun ByteArray.hex(): String = buildString(size * 2) {
    val digits = "0123456789ABCDEF"
    for (element in this@hex) {
        append(digits[(element.toInt() ushr 4) and 0xF])
        append(digits[(element.toInt() ushr 0) and 0xF])
    }
}
