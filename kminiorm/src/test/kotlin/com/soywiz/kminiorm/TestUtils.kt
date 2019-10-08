package com.soywiz.kminiorm

import kotlinx.coroutines.*

fun suspendTest(block: suspend () -> Unit): Unit = runBlocking { block() }
