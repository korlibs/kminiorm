package com.soywiz.kminiorm

import java.io.*
import java.sql.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

fun <T : Any> Db.table(clazz: KClass<T>) = DbTable(this, clazz).also { it.initialize() }
inline fun <reified T : Any> Db.table() = table(T::class)

class DbTable<T: Any>(val db: Db, val clazz: KClass<T>) {
    val tableName = clazz.findAnnotation<Name>()?.name ?: clazz.simpleName ?: error("$clazz doesn't have name")
    val quotedTableName = db.quoteTableName(tableName)
    val columns = clazz.memberProperties.map { ColumnDef(db, it) }

    class ColumnDef<T : Any> internal constructor(val db: Db, val property: KProperty1<T, *>) {
        val jclazz get() = property.returnType.jvmErasure
        val name = property.findAnnotation<Name>()?.name ?: property.name
        val quotedName = db.quoteColumnName(name)
        val sqlType by lazy { property.returnType.toSqlType(db, property) }
        val isNullable get() = property.returnType.isMarkedNullable
        val isUnique = property.findAnnotation<Unique>() != null
        val isIndex = property.findAnnotation<Index>() != null
    }

    fun showColumns(): Map<String, Map<String, Any?>> {
        return db.query("SHOW COLUMNS FROM $quotedTableName;").associateBy { it["COLUMN_NAME"]?.toString() ?: "-" }
    }

    fun initialize() = this.apply {
        db.query("CREATE TABLE IF NOT EXISTS $quotedTableName;")
        val oldColumns = showColumns()
        for (column in columns) {
            if (column.name in oldColumns) continue // Do not add columns if they already exists

            db.query(buildString {
                append("ALTER TABLE ")
                append(quotedTableName)
                append(" ADD ")
                append(column.quotedName)
                append(" ")
                append(column.sqlType)
                if (column.isNullable) {
                    append(" NULLABLE")
                } else {
                    append(" NOT NULL")
                    when {
                        column.jclazz == String::class.java -> append(" DEFAULT (\"\")")
                        column.jclazz.isSubclassOf(Number::class) -> append(" DEFAULT (0)")
                    }
                }
                append(";")
            })
        }

        for (column in columns) {
            //println("$column: ${column.quotedName}: ${column.isUnique}, ${column.isIndex}")
            if (column.isUnique || column.isIndex) {
                val unique = column.isUnique
                db.query(buildString {
                    append("CREATE ")
                    if (unique) append("UNIQUE ")
                    append("INDEX IF NOT EXISTS ${column.quotedName} ON $quotedTableName (${column.quotedName});")
                })
            }
        }
    }

    fun insert(instance: T): T {
        insert(db.mapper.convertValueToMap(instance))
        return instance
    }

    fun insert(instance: Partial<T>) {
        insert(instance.data.fix())
    }

    fun insert(data: Map<String, Any?>): DbResult {
        val entries = data.entries.toList()
        return db.query(buildString {
            append("INSERT INTO ")
            append(quotedTableName)
            append("(")
            append(entries.joinToString(", ") { db.quoteColumnName(it.key) })
            append(")")
            append(" VALUES ")
            append("(")
            append(entries.joinToString(", ") { "?" })
            append(")")
        }, *entries.map { it.value }.toTypedArray())
    }

    fun select(skip: Long? = null, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T> = { everything }): Iterable<T> {
        return db.query(buildString {
            append("SELECT ")
            append("*")
            append(" FROM ")
            append(quotedTableName)
            append(" WHERE ")
            append(query(DbQueryBuilder as DbQueryBuilder<T>).toString(db))
            if (limit != null) append(" LIMIT $limit")
            if (skip != null) append(" OFFSET $skip")
            append(";")
        }).map { db.mapper.convertValue(it.mapValues { (key, value) ->
            //println("it: $value, ${value?.javaClass}: ${value is InputStream}")
            when (value) {
                is InputStream -> value.readBytes()
                is Blob -> value.binaryStream.readBytes()
                else -> value
            }
        }, clazz.java) }
    }

    fun update(value: Partial<T>, limit: Long? = null, query: DbQueryBuilder<T>.() -> DbQuery<T>): Int {
        val entries = value.data.fix().entries
        val keys = entries.map { it.key }
        val values = entries.map { it.value }
        return db.query(buildString {
            append("UPDATE ")
            append(quotedTableName)
            append(" SET ")
            append(keys.joinToString(", ") { db.quoteColumnName(it) + "=?" })
            append(" WHERE ")
            append(query(DbQueryBuilder as DbQueryBuilder<T>).toString(db))
            if (limit != null) append(" LIMIT $limit")
            append(";")
        }, *values.toTypedArray()).updateCount
    }

    private fun Map<String, Any?>.fix(): Map<String, Any?> {
        // @TODO: Use @Name annotation
        return this
    }
}


fun KType.toSqlType(db: Db, annotations: KAnnotatedElement): String {
    return when (this.jvmErasure) {
        Int::class -> "INTEGER"
        ByteArray::class -> "BLOB"
        String::class -> {
            val maxLength = annotations.findAnnotation<MaxLength>()
            //if (maxLength != null) "VARCHAR(${maxLength.length})" else "TEXT"
            if (maxLength != null) "VARCHAR(${maxLength.length})" else "VARCHAR"
        }
        else -> "VARCHAR"
    }
}
