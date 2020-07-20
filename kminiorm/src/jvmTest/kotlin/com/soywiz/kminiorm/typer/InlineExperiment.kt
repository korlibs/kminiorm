package com.soywiz.kminiorm.typer

import kotlinx.coroutines.*
import java.lang.reflect.*
import kotlin.coroutines.*
import kotlin.test.*

class InlineExperimentTest {
    @Test
    fun test() {
        runBlocking {
            val method = InlineExperimentTest::class.java.declaredMethods.first { it.name.startsWith("demo-") }
            method.callSuspendSupportingInline(this@InlineExperimentTest, InlineDemo(1.0), 10, "hello")
            Unit
            /*
            println(InlineExperimentTest::class.memberFunctions.map { it.name })
            val demo = InlineExperimentTest::class.memberFunctions.first { it.name == "demo" }
            //demo.callSuspend(InlineDemo(0.0), "hello")
            //demo.callSuspend(this@InlineExperimentTest, 10, InlineDemo(0.0), "hello")

            println(demo.)
            //demo.callSuspend()
            demo.callSuspendSupportingInline(this@InlineExperimentTest, "hello")
             */
        }
    }

    @DemoAnnotation
    suspend fun demo(idemo: InlineDemo, test: Number, a: String) {
        println("$idemo, $test")
        //delay(100L)
    }
}

annotation class DemoAnnotation
inline class InlineDemo(val value: Double)

suspend fun Method.callSuspendSupportingInline(instance: Any?, vararg args: Any?): Any? = suspendCoroutine { c->
    val jmethod = this
    val jargs = jmethod.parameterTypes.withIndex().map { (index, type) ->
        if (type == Continuation::class.java) {
            c
        } else {
            val arg = args[index]
            val argJType = arg?.let { it::class.java } ?: Void.TYPE
            when {
                argJType == Void.TYPE || type.isAssignableFrom(argJType) -> arg
                else -> {
                    val unboxMethod = argJType.methods.firstOrNull { it.name == "unbox-impl" }
                    unboxMethod?.invoke(arg)
                }
            }
        }
    }
    try {
        val result = jmethod.invoke(instance, *jargs.toTypedArray())
        val result2 = if (result != null) {
            val boxMethod = result::class.java.methods.firstOrNull { it.name == "box-impl" }
            if (boxMethod != null) boxMethod.invoke(result) else result
        } else {
            result
        }
        if (result2 != kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED) {
            c.resume(result2)
        }
    } catch (e: Throwable) {
        c.resumeWithException(e)
    }
}