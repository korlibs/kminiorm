package com.soywiz.kminiorm.typer

import com.soywiz.kminiorm.*
import kotlin.reflect.full.*
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

    @Test
    fun testCreateDefault() {
        assertEquals(false, Typer().createDefault(Boolean::class.starProjectedType))
    }

    @Test
    fun testBoolean() {
        val untype = Typer().untype(BooleanTable(value = true))
        val json = MiniJson.stringify(untype)
        assertEquals(mapOf("value" to true), untype)
        assertEquals("{\"value\":true}", json)
    }

    @Test
    fun testDbKeyListTable() {
        val k1 = DbKey("5da318a9a396515aaa9d3600")
        val k2 = DbKey("5da318a9a396515aaa9d3601")
        val result = PartialUntyped<DbKeyListTable>(DbTyper, mapOf("list" to listOf(k1.toHexString(), k2.toHexString())), DbKeyListTable::class)
        assertEquals(listOf(k1, k2), result.complete.list)
    }

    data class DbKeyListTable(val list: List<DbKey>)

    data class BooleanTable(val value: Boolean)

    data class Demo(val id: DbKey = DbKey())

    data class Demo2(val items: List<Item>, val extra: Item) {
        data class Item(val name: String, val sub: SubItem)
        data class SubItem(val name: String)
    }
}