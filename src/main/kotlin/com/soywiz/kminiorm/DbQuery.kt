package com.soywiz.kminiorm

import kotlin.internal.Exact
import kotlin.reflect.*

abstract class DbQuery<T> {
    abstract fun toString(db: Db): String
    class BinOp<T, R>(val prop: KProperty1<T, R>, val literal: R, val op: String) : DbQuery<T>() {
        override fun toString(db: Db) = "${db.quoteTableName(prop.name)}$op${db.quoteLiteral(literal)}"
    }
    class Always<T> : DbQuery<T>() {
        override fun toString(db: Db) = "1=1"
    }
    class BinOpNode<T>(val left: DbQuery<T>, val op: String, val right: DbQuery<T>) : DbQuery<T>() {
        override fun toString(db: Db) = "((${left.toString(db)}) $op (${right.toString(db)}))"
    }
    class UnOpNode<T>(val op: String, val right: DbQuery<T>) : DbQuery<T>() {
        override fun toString(db: Db) = "($op (${right.toString(db)}))"
    }
    class IN<T, R>(val prop: KProperty1<T, R>, val literal: List<R>) : DbQuery<T>() {
        override fun toString(db: Db) = "${db.quoteTableName(prop.name)} IN (${literal.joinToString(", ") { db.quoteLiteral(it) }})"
    }
}

open class DbQueryBuilder<T> {
    companion object : DbQueryBuilder<Any>()
    infix fun DbQuery<T>.AND(that: DbQuery<T>) = DbQuery.BinOpNode(this, "AND", that)
    infix fun DbQuery<T>.OR(that: DbQuery<T>) = DbQuery.BinOpNode(this, "OR", that)
    fun NOT(q: DbQuery<T>) = DbQuery.UnOpNode("NOT", q)
    infix fun <R> KProperty1<@Exact T, @Exact R>.IN(literal: List<R>) = DbQuery.IN(this, literal)
    infix fun <R> KProperty1<@Exact T, @Exact R>.LIKE(literal: R) = DbQuery.BinOp(this, literal, "LIKE")
    infix fun <R> KProperty1<@Exact T, @Exact R>.eq(literal: R) = DbQuery.BinOp(this, literal, "=")
    infix fun <R> KProperty1<@Exact T, @Exact R>.ne(literal: R) = DbQuery.BinOp(this, literal, "<>")
    infix fun <R : Comparable<R>> KProperty1<@Exact T, @Exact R>.gt(literal: R) = DbQuery.BinOp(this, literal, ">")
    infix fun <R : Comparable<R>> KProperty1<@Exact T, @Exact R>.lt(literal: R) = DbQuery.BinOp(this, literal, "<")
    infix fun <R : Comparable<R>> KProperty1<@Exact T, @Exact R>.ge(literal: R) = DbQuery.BinOp(this, literal, ">=")
    infix fun <R : Comparable<R>> KProperty1<@Exact T, @Exact R>.le(literal: R) = DbQuery.BinOp(this, literal, "<=")
    val everything get() = DbQuery.Always<T>()
}
