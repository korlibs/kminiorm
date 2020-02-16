package com.soywiz.kminiorm.internal

import kotlinx.coroutines.sync.*

class InternalDbPool<T>(val maxItems: Int = Int.MAX_VALUE, @PublishedApi internal val generate: suspend () -> T) {
    @PublishedApi
    internal val semaphore = Semaphore(maxItems)
    @PublishedApi
    internal val items = arrayListOf<T>()

    suspend inline fun <R> take(callback: suspend (T) -> R): R {
        semaphore.acquire()
        val item = synchronized(this) { if (items.isEmpty()) null else items.removeAt(items.size - 1) } ?: generate()
        try {
            return callback(item)
        } finally {
            synchronized(this) { items.add(item) }
            semaphore.release()
        }
    }
}