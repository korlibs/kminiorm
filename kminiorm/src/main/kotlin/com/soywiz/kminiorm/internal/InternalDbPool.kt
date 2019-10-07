package com.soywiz.kminiorm.internal

class InternalDbPool<T>(@PublishedApi internal val generate: suspend () -> T) {
    @PublishedApi
    internal val items = arrayListOf<T>()

    suspend inline fun <R> take(callback: suspend (T) -> R): R {
        val item = synchronized(this) { if (items.isEmpty()) null else items.removeAt(items.size - 1) } ?: generate()
        try {
            return callback(item)
        } finally {
            synchronized(this) { items.add(item) }
        }
    }
}