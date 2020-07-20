package com.soywiz.kminiorm.typer

import kotlin.test.*

class MiniJsonTest {
    fun check(instance: Any?) {
        assertEquals(instance, MiniJson.parse(MiniJson.stringify(instance)))
    }

    @Test
    fun test() {
        check(null)
        check(false)
        check(true)
        check(0)
        check(-100)
        check(1e10)
        check(-1e10)
        //check(Double.NaN)
        //check(Double.NEGATIVE_INFINITY)
        //check(Double.POSITIVE_INFINITY)
        check("")
        check("a")
        check("hello")
        check("a\"b\n\r\t'\u000c\u70f5")
        assertEquals("\"'hello'\"", MiniJson.stringify("'hello'"))
        //check(arrayOf("a", "b"))
        check(listOf("a", "b", 1, 2, mapOf("a" to 10, "b" to 100, "c" to mapOf("a" to null, "b" to false))))
    }
}