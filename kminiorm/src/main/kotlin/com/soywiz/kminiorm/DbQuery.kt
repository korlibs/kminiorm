package com.soywiz.kminiorm

import com.soywiz.kminiorm.internal.DynamicExt
import kotlin.internal.*
import kotlin.reflect.*

enum class DbQueryBinOp {
    AND, OR, LIKE, EQ, NE, GT, LT, GE, LE
}
enum class DbQueryUnOp {
    NOT
}
abstract class DbQuery<T> {
    class Always<T> : DbQuery<T>() {
        override fun toString() = "Always"
        override fun hashCode(): Int = 0
        override fun equals(other: Any?): Boolean = other is Always<*>
    }
    class Never<T> : DbQuery<T>() {
        override fun toString() = "Never"
        override fun hashCode(): Int = 0
        override fun equals(other: Any?): Boolean = other is Never<*>
    }
    data class BinOp<T, R>(val prop: KProperty1<T, R>, val literal: R, val op: DbQueryBinOp) : DbQuery<T>()
    data class BinOpNode<T>(val left: DbQuery<T>, val op: DbQueryBinOp, val right: DbQuery<T>) : DbQuery<T>()
    data class UnOpNode<T>(val op: DbQueryUnOp, val right: DbQuery<T>) : DbQuery<T>()
    data class IN<T, R>(val prop: KProperty1<T, R>, val literal: List<R>) : DbQuery<T>() {
        val literalSet by lazy { literal.toSet() }
    }
    data class Raw<T>(val map: Map<String, Any?>) : DbQuery<T>()
}

open class DbQueryBuilder<T> {
    val NEVER by lazy { DbQuery.Never<T>() }
    companion object : DbQueryBuilder<Any>() {
        fun <T> builder() = DbQueryBuilder as DbQueryBuilder<T>
        fun <T> build(query: DbQueryBuilder<T>.() -> DbQuery<T>) = query(builder())
        fun <T> buildOrNull(query: DbQueryBuilder<T>.() -> DbQuery<T>) = build(query).takeIf { (it !is DbQuery.Never<*>) }
    }

    fun id(id: DbKey) = DbQuery.BinOp(DbModel::_id as KProperty1<T, DbKey>, id, DbQueryBinOp.EQ)
    fun raw(map: Map<String, Any?>) = DbQuery.Raw<T>(map)
    infix fun AND(clauses: Iterable<DbQuery<T>>) = clauses.reduce { l, r -> l AND r }
    infix fun OR(clauses: Iterable<DbQuery<T>>) = clauses.reduce { l, r -> l OR r }
    infix fun DbQuery<T>.AND(that: DbQuery<T>) = if (this is DbQuery.Never<*> || that is DbQuery.Never<*>) NEVER else DbQuery.BinOpNode(this, DbQueryBinOp.AND, that)
    infix fun DbQuery<T>.OR(that: DbQuery<T>) = DbQuery.BinOpNode(this, DbQueryBinOp.OR, that)
    fun NOT(q: DbQuery<T>) = DbQuery.UnOpNode(DbQueryUnOp.NOT, q)
    infix fun <R : Any?> KProperty1<@Exact T, @Exact R>.IN(literals: List<R>) = if (literals.isNotEmpty()) DbQuery.IN(this, literals) else NEVER
    infix fun <R : Any?> KProperty1<@Exact T, @Exact R>.LIKE(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.LIKE)
    infix fun <R : Any?> KProperty1<@Exact T, @Exact R>.eq(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.EQ)
    infix fun <T : Any?> KProperty1<@Exact T, DbKey>.eq(literal: DbModel) = DbQuery.BinOp(this, literal._id, DbQueryBinOp.EQ)
    infix fun <R : Any?> KProperty1<@Exact T, @Exact R>.ne(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.NE)
    infix fun <R : Comparable<R>?> KProperty1<@Exact T, @Exact R>.gt(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.GT)
    infix fun <R : Comparable<R>?> KProperty1<@Exact T, @Exact R>.lt(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.LT)
    infix fun <R : Comparable<R>?> KProperty1<@Exact T, @Exact R>.ge(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.GE)
    infix fun <R : Comparable<R>?> KProperty1<@Exact T, @Exact R>.le(literal: R) = DbQuery.BinOp(this, literal, DbQueryBinOp.LE)
    infix fun <R : Comparable<R>?> KProperty1<@Exact T, @Exact R>.BETWEEN(literal: Pair<R, R>) = (this ge literal.first) AND (this lt literal.second)
    infix fun <R : Comparable<R>> KProperty1<@Exact T, @Exact R?>.BETWEEN(literal: ClosedRange<R>) = ((this as KProperty1<@Exact T, @Exact R>) ge (literal.start)) AND (this le literal.endInclusive)
    val everything get() = DbQuery.Always<T>()
    val nothing get() = DbQuery.Never<T>()
}
