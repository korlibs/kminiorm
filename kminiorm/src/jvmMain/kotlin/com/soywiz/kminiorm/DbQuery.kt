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

open class DbQueryBuilder<T : DbBaseModel>(val table: DbTable<T>) {
    companion object {
        private val ALWAYS = DbQuery.Always<DbBaseModel>()
        private val NEVER = DbQuery.Never<DbBaseModel>()
    }
    private val NEVER = DbQueryBuilder.NEVER as DbQuery.Never<T>
    val everything = DbQueryBuilder.ALWAYS as DbQuery.Always<T>
    val nothing = DbQueryBuilder.NEVER as DbQuery.Never<T>
    @PublishedApi
    internal val <R : Any?> KProperty0<@Exact R>.prop get() = table.getProperty(this)

    fun build(query: DbQueryBuilder<T>.(T) -> DbQuery<T>): DbQuery<T> = query(this, table.dummyInstance)
    fun buildOrNull(query: DbQueryBuilder<T>.(T) -> DbQuery<T>) = build(query).takeIf { (it !is DbQuery.Never<*>) }

    fun id(id: DbKey) = DbQuery.BinOp(DbModel::_id as KProperty1<T, DbKey>, id, DbQueryBinOp.EQ)
    fun raw(map: Map<String, Any?>) = DbQuery.Raw<T>(map)
    infix fun AND(clauses: Iterable<DbQuery<T>>) = clauses.reduce { l, r -> l AND r }
    infix fun OR(clauses: Iterable<DbQuery<T>>) = clauses.reduce { l, r -> l OR r }
    infix fun DbQuery<T>.AND(that: DbQuery<T>) = if (this is DbQuery.Never<*> || that is DbQuery.Never<*>) NEVER else DbQuery.BinOpNode(this, DbQueryBinOp.AND, that)
    infix fun DbQuery<T>.OR(that: DbQuery<T>) = DbQuery.BinOpNode(this, DbQueryBinOp.OR, that)
    fun NOT(q: DbQuery<T>) = DbQuery.UnOpNode(DbQueryUnOp.NOT, q)

    infix fun <R : Any?> KProperty1<@Exact T, @Exact R>.IN(literals: Iterable<R>) = run {
        val literalsList = literals.toList()
        if (literalsList.isNotEmpty()) DbQuery.IN(this, literalsList) else NEVER
    }
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

    infix fun <R : Any?> KProperty0<@Exact R>.IN(literals: Iterable<R>) = prop IN literals
    infix fun <R : Any?> KProperty0<@Exact R>.LIKE(literal: R) = prop LIKE literal
    infix fun <R : Any?> KProperty0<@Exact R>.eq(literal: R) = prop eq literal
    infix fun <T : Any?> KProperty0<DbKey>.eq(literal: DbModel) = prop eq literal
    infix fun <R : Any?> KProperty0<@Exact R>.ne(literal: R) = prop ne literal
    infix fun <R : Comparable<R>?> KProperty0<@Exact R>.gt(literal: R) = prop gt literal
    infix fun <R : Comparable<R>?> KProperty0<@Exact R>.lt(literal: R) = prop lt literal
    infix fun <R : Comparable<R>?> KProperty0<@Exact R>.ge(literal: R) = prop ge literal
    infix fun <R : Comparable<R>?> KProperty0<@Exact R>.le(literal: R) = prop le literal
    infix fun <R : Comparable<R>?> KProperty0<@Exact R>.BETWEEN(literal: Pair<R, R>) = prop BETWEEN literal
    infix fun <R : Comparable<R>> KProperty0<@Exact R?>.BETWEEN(literal: ClosedRange<R>) = prop BETWEEN literal

    fun auto(instance: T): DbQuery<T> {
        val uniqueColumns = table.ormTableInfo.columnUniqueIndices.values.flatten()
        return AND(uniqueColumns.map { column -> column.property eq column.property.get(instance) })
    }
}
