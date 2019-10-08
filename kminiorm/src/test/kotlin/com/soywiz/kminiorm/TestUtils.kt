package com.soywiz.kminiorm

import kotlinx.coroutines.*

inline fun suspendTest(noinline block: suspend () -> Unit): Unit = runBlocking { block() }
