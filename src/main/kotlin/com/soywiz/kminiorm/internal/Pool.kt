package com.soywiz.kminiorm.internal

internal class Pool<T>(@PublishedApi internal val generate: () -> T) {
    @PublishedApi
    internal val items = arrayListOf<T>()

    inline fun <R> take(callback: (T) -> R): R {
        val item = synchronized(this) { if (items.isEmpty()) generate() else items.removeAt(items.size - 1) }
        try {
            return callback(item)
        } finally {
            synchronized(this) { items.add(item) }
        }
    }
}