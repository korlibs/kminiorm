package com.soywiz.kminiorm.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

fun <T> Flow<T>.chunked(chunkSize: Int): Flow<List<T>> {
    val original = this
    return flow {
        val buffer = arrayListOf<T>()
        suspend fun flush() {
            emit(buffer.toList().also { buffer.clear() })
        }
        original.collect {
            buffer.add(it)
            if (buffer.size >= chunkSize) {
                flush()
            }
        }
        flush()
    }
}
