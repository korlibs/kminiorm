package com.soywiz.kminiorm.dialect

open class MySqlDialect : SqlDialect() {
    companion object : MySqlDialect()
    override val supportPrimaryIndex get() = true
    override fun quoteColumnName(str: String) = _quote(str, '`')
    override fun quoteTableName(str: String) = _quote(str, '`')

    override val supportsLastInsertId: Boolean = true
    override fun lastInsertId(): String = "SELECT LAST_INSERT_ID();"
}