package com.soywiz.kminiorm.typer

import com.soywiz.kminiorm.*
import kotlin.test.*

class TyperTest {
    @Test
    fun test() {
        val typer = Typer().withKeepType<DbKey>()
        val untyped = typer.untype(Demo())
        println(untyped)
        println(typer.type<List<String>>(setOf("hello", "world")))
        println(typer.type<List<Demo>>(listOf(untyped, untyped)))
        println(typer.type<Demo>(untyped))
    }

    @Test
    fun test2() {
        val typer = Typer().withKeepType<DbKey>()
        val untyped = typer.untype(Demo2(listOf(Demo2.Item("a", Demo2.SubItem("A")), Demo2.Item("b", Demo2.SubItem("B"))), Demo2.Item("c", Demo2.SubItem("C"))))
        println(typer.type<Demo2>(untyped))
        println(typer.type<Map<String, Int>>(mapOf("a" to "10", "b" to "20")))
    }

    @Test
    fun testDbKey() {
        val typer = Typer().withDbKeyTyperUntyper()
        val result = typer.untype(DbKey())
        println("result: $result")
    }

    data class Demo(val id: DbKey = DbKey())

    data class Demo2(val items: List<Item>, val extra: Item) {
        data class Item(val name: String, val sub: SubItem)
        data class SubItem(val name: String)
    }
}