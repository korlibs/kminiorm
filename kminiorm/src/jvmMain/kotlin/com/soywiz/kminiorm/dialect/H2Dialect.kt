package com.soywiz.kminiorm.dialect

import kotlin.reflect.*

open class H2Dialect : SqlDialect() {
    companion object : H2Dialect()

    override val supportsLastInsertId: Boolean = true
    override fun lastInsertId(): String = "SELECT IDENTITY();"

    override fun autoincrement(type: KType, annotations: KAnnotatedElement): String = "IDENTITY NOT NULL PRIMARY KEY"
}