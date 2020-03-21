package com.soywiz.kminiorm.dialect

import com.soywiz.kminiorm.DbOnConflict
import com.soywiz.kminiorm.DbQueryable
import com.soywiz.kminiorm.IColumnDef
import com.soywiz.kminiorm.SyntheticColumn

open class SqliteDialect : SqlDialect() {
    companion object : SqliteDialect()

    //override fun quoteColumnName(str: String) = "[$str]"
    //override fun quoteTableName(str: String) = "[$str]"
    //override fun quoteString(str: String) = _quote(str, type = '\'')

    override val supportExtendedInsert = true

    /*
    override fun sqlInsertReplace(tableInfo: OrmTableInfo<*>, keys: List<IColumnDef>): SqlInsertInfo {
        var repeatCount = 0
        return SqlInsertInfo(buildString {
            for (uniqueColumns in tableInfo.columnUniqueIndices.values) {
                val columns = uniqueColumns.joinToString(", ") { quoteColumnName(it.name) }
                append(" ON CONFLICT($columns) DO UPDATE SET ")
                append(keys.joinToString(", ") { "${quoteColumnName(it.name)}=?" })
                repeatCount++
            }
        }, repeatCount)
    }
     */

    override fun sqlInsertInto(onConflict: DbOnConflict): String = when (onConflict) {
        DbOnConflict.IGNORE -> "INSERT OR IGNORE INTO "
        DbOnConflict.REPLACE -> "INSERT OR REPLACE INTO "
        else -> super.sqlInsertInto(onConflict)
    }
    override suspend fun showColumns(db: DbQueryable, table: String): List<IColumnDef> {
        return db.query("PRAGMA table_info(${quoteTableName(table)});")
                .map { SyntheticColumn<String>(it["name"]?.toString() ?: "-") }
    }
}