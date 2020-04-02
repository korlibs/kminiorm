package com.soywiz.kminiorm.dialect

import com.soywiz.kminiorm.*
import com.soywiz.kminiorm.typer.Typer
import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.util.*
import kotlin.reflect.KAnnotatedElement
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.jvm.jvmErasure

open class SqlDialect() : DbQuoteable {
    val dialect = this
    open val supportPrimaryIndex get() = false

    companion object ANSI : SqlDialect()

    enum class IndexType { PRIMARY, UNIQUE, INDEX, OTHER }

    override fun quoteColumnName(str: String) = _quote(str)
    override fun quoteTableName(str: String) = _quote(str)
    override fun quoteString(str: String) = _quote(str, type = '\'')
    override fun quoteLiteral(value: Any?) = when (value) {
        null -> "NULL"
        is Boolean -> "$value"
        is Int, is Long, is Float, is Double, is Number -> "$value"
        is DbIntKey -> "${value.key}"
        is String -> quoteString(value)
        is Date -> quoteString(java.sql.Date(value.time).toString())
        else -> quoteString("$value")
    }

    open fun toSqlType(property: KProperty1<*, *>): String = toSqlType(property.returnType, property)

    open fun toSqlType(type: KType, annotations: KAnnotatedElement? = null): String {
        return when (type.jvmErasure) {
            Int::class -> "INTEGER"
            Long::class -> "BIGINT"
            //Boolean::class -> "TINYINT"
            Boolean::class -> "BOOLEAN"
            ByteArray::class -> "BLOB"
            Date::class -> "TIMESTAMP"
            String::class -> {
                val maxLength = annotations?.findAnnotation<DbMaxLength>()
                //if (maxLength != null) "VARCHAR(${maxLength.length})" else "TEXT"
                if (maxLength != null) "VARCHAR(${maxLength.length})" else "VARCHAR"
            }
            DbIntRef::class, DbIntKey::class -> "INTEGER"
            DbRef::class, DbKey::class -> "VARCHAR"
            DbStringRef::class, DbStringRef::class -> "VARCHAR"
            DbAnyRef::class, DbAnyRef::class -> "VARCHAR"
            else -> "VARCHAR"
        }
    }

    open fun sqlCreateColumnDef(typer: Typer, colName: String, type: KType, defaultValue: Any? = Unit, annotations: KAnnotatedElement? = null): String {
        return buildString {
            append(quoteColumnName(colName))
            append(" ")
            append(toSqlType(type, annotations))
            when {
                annotations?.findAnnotation<DbAutoIncrement>() != null -> {
                    append(autoincrement())
                }
                type.isMarkedNullable -> {
                    append(" NULL")
                }
                else -> {
                    append(" NOT NULL")
                    append(" DEFAULT (")
                    append(quoteLiteral(if (defaultValue != Unit) defaultValue else typer.createDefault(type)))
                    append(")")
                }
            }
        }
    }

    open fun autoincrement(): String {
        return " PRIMARY KEY AUTOINCREMENT"
    }

    open fun sqlCreateColumnDef(typer: Typer, column: IColumnDef): String {
        return sqlCreateColumnDef(typer, column.name, column.columnType, column.defaultValue, column.annotatedElement)
        /*
        return buildString {
            append(quoteColumnName(column.name))
            append(" ")
            append(toSqlType(column.property))
            if (column.isNullable) {
                append(" NULL")
            } else {
                append(" NOT NULL")
                when {
                    column.jclazz == String::class -> append(" DEFAULT ('')")
                    column.jclazz.isSubclassOf(Number::class) -> append(" DEFAULT (0)")
                }
            }
        }
         */
    }

    open fun sqlCreateTable(typer: Typer, table: String, columns: List<IColumnDef>): String {
        return buildString {
            append("CREATE TABLE IF NOT EXISTS ")
            append(quoteTableName(table))
            append(" (")
            append(columns.joinToString(", ") { sqlCreateColumnDef(typer, it) })
            append(");")
        }
    }

    open fun sqlAlterTableAddColumn(typer: Typer, table: String, column: IColumnDef): String {
        return buildString {
            append("ALTER TABLE ")
            append(quoteTableName(table))
            append(" ADD ")
            append(sqlCreateColumnDef(typer, column))
            append(";")
        }
    }

    open fun sqlDelete(table: String, query: DbQuery<*>, params: ArrayList<Any?>, limit: Long? = null): String {
        return buildString {
            append("DELETE FROM ")
            append(quoteTableName(table))
            append(" WHERE ")
            append(query.toString(dialect, params))
            if (limit != null) append(" LIMIT $limit")
            append(";")
        }
    }

    open suspend fun showColumns(db: DbQueryable, table: String): List<IColumnDef> {
        return db.query("SHOW COLUMNS FROM ${quoteTableName(table)};")
                .map {
                    SyntheticColumn<String>(it["FIELD"]?.toString()
                            ?: it["COLUMN_NAME"]?.toString()
                            ?: "-")
                }
    }

    open protected fun _quote(str: String, type: Char = '"') = buildString {
        append(type)
        for (char in str) {
            if (char == type) {
                append(type)
                append(type)
            } else {
                append(char)
            }
        }
        append(type)
    }

    open fun sqlAlterCreateIndex(index: IndexType, tableName: String, columns: List<ColumnDef<*>>, indexName: String): String {
        return buildString {
            append("CREATE ")
            append(when (index) {
                IndexType.PRIMARY -> if (dialect.supportPrimaryIndex) "PRIMARY INDEX" else "UNIQUE INDEX"
                IndexType.UNIQUE -> "UNIQUE INDEX"
                else -> "INDEX"
            })
            val packs = columns.map { "${quoteColumnName(it.name)} ${it.indexDirection.sname}" }
            append(" IF NOT EXISTS ${quoteColumnName("${tableName}_${indexName}")} ON ${quoteTableName(tableName)} (${packs.joinToString(", ")});")
        }
    }

    open fun sqlInsertInto(onConflict: DbOnConflict): String = when (onConflict) {
        DbOnConflict.ERROR -> "INSERT INTO "
        DbOnConflict.IGNORE -> "INSERT IGNORE INTO "
        DbOnConflict.REPLACE -> "INSERT INTO "
    }

    data class SqlInsertInfo(val sql: String, val repeatCount: Int)
    open fun sqlInsert(tableName: String, tableInfo: OrmTableInfo<*>, keys: List<IColumnDef>, onConflict: DbOnConflict): SqlInsertInfo {
        var repeatCount = 1
        return SqlInsertInfo(buildString {
            append(sqlInsertInto(onConflict))
            append(quoteTableName(tableName))
            append(" (")
            append(keys.joinToString(", ") { quoteColumnName(it.name) })
            append(") VALUES (")
            append(keys.joinToString(", ") { "?" })
            append(")")
            if (onConflict == DbOnConflict.REPLACE) {
                val res = sqlInsertReplace(tableInfo, keys)
                append(res.sql)
                repeatCount += res.repeatCount
            }
        }, repeatCount)
    }

    open fun sqlInsertReplace(tableInfo: OrmTableInfo<*>, keys: List<IColumnDef>): SqlInsertInfo {
        return SqlInsertInfo("", 0)
    }

    open val supportExtendedInsert = false

    open fun transformException(e: Throwable): Throwable = when (e) {
        is SQLIntegrityConstraintViolationException -> DuplicateKeyDbException("Conflict", e)
        else -> {
            val message = e.message ?: ""
            when {
                e is SQLException && (message.contains("constraint violation") || message.contains("constraint failed") || message.contains("key violation")) -> {
                    DuplicateKeyDbException("Conflict", e)
                }
                else -> {
                    e
                }
            }
        }
    }
}