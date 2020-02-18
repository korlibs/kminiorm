package com.soywiz.kminiorm

import com.soywiz.kminiorm.internal.*

fun <T> DbQuery<T>.matches(it: T): Boolean = DynamicExt { this@matches.evaluates(it).bool }

fun <T> DbQuery<T>.evaluates(it: T): Any? = DynamicExt {
    val query = this@evaluates
    return when (query) {
        is DbQuery.Always -> true
        is DbQuery.Never -> false
        is DbQuery.IN<T, *> -> query.prop.get(it) in query.literalSet
        is DbQuery.UnOpNode -> !query.right.evaluates(it).bool
        is DbQuery.BinOp<T, *> -> {
            val l = query.prop.get(it)
            val r = query.literal
            when (query.op) {
                DbQueryBinOp.AND -> l.bool && r.bool
                DbQueryBinOp.OR -> l.bool || r.bool
                DbQueryBinOp.LIKE -> TODO()
                DbQueryBinOp.EQ -> l == r
                DbQueryBinOp.NE -> l != r
                DbQueryBinOp.GT -> compare(l, r) > 0
                DbQueryBinOp.LT -> compare(l, r) < 0
                DbQueryBinOp.GE -> compare(l, r) >= 0
                DbQueryBinOp.LE -> compare(l, r) <= 0
            }
        }
        is DbQuery.BinOpNode -> {
            val l = query.left.evaluates(it).bool
            val r = query.right.evaluates(it).bool
            when (query.op) {
                DbQueryBinOp.AND -> l && r
                DbQueryBinOp.OR -> l || r
                DbQueryBinOp.LIKE -> TODO()
                DbQueryBinOp.EQ -> l == r
                DbQueryBinOp.NE -> l != r
                DbQueryBinOp.GT -> l > r
                DbQueryBinOp.LT -> l < r
                DbQueryBinOp.GE -> l >= r
                DbQueryBinOp.LE -> l <= r
            }
        }
        else -> TODO()
    }
}
