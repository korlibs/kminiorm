package com.soywiz.kminiorm.internal

import java.util.*

internal fun ByteArray.toBase64(): String = Base64.getEncoder().encodeToString(this)
internal fun String.fromBase64(): ByteArray = Base64.getDecoder().decode(this)
