package com.soywiz.kminiorm.typer

import com.soywiz.kminiorm.*
import kotlin.reflect.*
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

    @Test
    fun testByteArray() {
        assertEquals("AQIDBA==", Typer().untype(byteArrayOf(1, 2, 3, 4)))
        assertEquals(byteArrayOf(1, 2, 3, 4).toList(), Typer().type<ByteArray>("AQIDBA==").toList())
    }

    enum class DemoEnum { HELLO, WORLD }

    @Test
    fun testEnum() {
        assertEquals("HELLO", Typer().untype(DemoEnum.HELLO))
        assertEquals("WORLD", Typer().untype(DemoEnum.WORLD))
        assertEquals(DemoEnum.HELLO, Typer().type("HELLO"))
        assertEquals(DemoEnum.WORLD, Typer().type("WORLD"))
        assertEquals(null, Typer().type<DemoEnum?>("WORLD2"))
    }

    class VertxJsonObject(val map: Map<String, Any?>) : Iterable<Map.Entry<String, Any?>> {
        constructor(vararg pairs: Pair<String, Any?>) : this(pairs.toMap())
        override fun iterator(): Iterator<Map.Entry<String, Any?>> = map.iterator()
    }

    @UseExperimental(ExperimentalStdlibApi::class)
    @Test
    fun testMixed() {
        val json = MiniJson.parse("{\"list\": [{\"a\": 1, \"b\": 2}]}")!!
        val json2 = mapOf("list" to listOf(mapOf("a" to 1, "b" to 2)))
        val json3 = mapOf("list" to listOf(VertxJsonObject("a" to 1, "b" to 2)))
        val partial1 = DbTyper.type<Partial<Mixed>>(json)
        val partial2 = DbTyper.type<Partial<Mixed>>(json2)
        val partial3 = DbTyper.type<Partial<Mixed>>(json3)

        println(json)
        println(json2)
        println(partial1)
        println(partial2)
        println(partial3)

        assertEquals(Mixed(listOf(Mixed.Compound(1, 2))), partial1.complete)
        assertEquals(Mixed(listOf(Mixed.Compound(1, 2))), partial2.complete)
        assertEquals(Mixed(listOf(Mixed.Compound(1, 2))), partial3.complete)
    }

    data class Mixed(val list: List<Compound>) {
        data class Compound(val a: Int, val b: Int)
    }

    data class DbKeyListTable(val list: List<DbKey>)

    data class BooleanTable(val value: Boolean)

    data class Demo(val id: DbKey = DbKey())

    data class Demo2(val items: List<Item>, val extra: Item) {
        data class Item(val name: String, val sub: SubItem)
        data class SubItem(val name: String)
    }
}