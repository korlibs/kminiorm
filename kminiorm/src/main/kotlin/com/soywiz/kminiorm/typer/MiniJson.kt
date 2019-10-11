package com.soywiz.kminiorm.typer

import kotlin.math.*

object MiniJson {
    fun stringify(instance: Any?): String = buildString { stringify(instance) }
    private fun StringBuilder.stringifyString(instance: String) {
        append('"')
        for (c in instance) {
            when (c) {
                '\'' -> append("\\'")
                '\b' -> append("\\b")
                '\u000c' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                '"' -> append("\\\"")
                '\\' -> append("\\\\")
                else -> append(c)
            }
        }
        append('"')
    }
    private fun StringBuilder.stringifyArray(instance: Iterable<Any?>) {
        append('[')
        var first = true
        for (item in instance) {
            if (first) {
                first = false
            } else {
                append(',')
            }
            stringify(item)
        }
        append(']')
    }
    private fun StringBuilder.stringify(instance: Any?) {
        when (instance) {
            Unit -> append("null")
            null -> append("null")
            false -> append("false")
            true -> append("true")
            is Number -> {
                val double = instance.toDouble()
                when {
                    double.isNaN() -> append(0.0)
                    double.isInfinite() -> append(Double.MAX_VALUE.withSign(double.sign))
                    else -> append(instance)
                }
            }
            is String -> stringifyString(instance)
            is CharArray -> stringifyString(String(instance))
            is Map<*, *> -> {
                append('{')
                var first = true
                for ((key, value) in instance) {
                    if (first) {
                        first = false
                    } else {
                        append(',')
                    }
                    stringify(key)
                    append(':')
                    stringify(value)
                }
                append('}')
            }
            is Iterable<*> -> stringifyArray(instance)
            is Array<*> -> stringifyArray(instance.toList())
            is BooleanArray -> stringifyArray(instance.toList())
            is ByteArray -> stringifyArray(instance.toList())
            is ShortArray -> stringifyArray(instance.toList())
            is IntArray -> stringifyArray(instance.toList())
            is LongArray -> stringifyArray(instance.toList())
            is FloatArray -> stringifyArray(instance.toList())
            is DoubleArray -> stringifyArray(instance.toList())
            else -> TODO("Don't know how to stringify ${instance::class} :: $instance")
        }
    }

    fun parse(str: String): Any? = MiniStrReader(str).parse()

    private fun MiniStrReader.parse(): Any? {
        skipSpaces()
        if (eof) return null
        return when (peek()) {
            'n' -> null.also { expect("null") }
            't' -> true.also { expect("true") }
            'f' -> false.also { expect("false") }
            '+', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.' -> {
                val value = readWhile { it in '0'..'9' || it == '+' || it == '-' || it == 'e' || it == 'E' || it == '.' }.toDouble()
                if (value.toInt().toDouble() == value) value.toInt() else value
            }
            '"' -> {
                val out = StringBuilder()
                expect('"')
                while (peek() != '"') {
                    val c = read()
                    if (c == '\\') {
                        val c2 = read()
                        when (c2) {
                            'b' -> out.append('\b')
                            't' -> out.append('\t')
                            'r' -> out.append('\r')
                            'n' -> out.append('\n')
                            '\'' -> out.append('\'')
                            '\"' -> out.append('\"')
                            '\\' -> out.append('\\')
                            '/' -> out.append('/')
                            'u' -> {
                                val hex = read(4)
                                out.append(hex.toInt(16).toChar())
                            }
                            else -> TODO("Unexpected escape character '$c2'")
                        }
                    } else {
                        out.append(c)
                    }
                }
                expect('"')
                out.toString()
            }
            '[' -> {
                arrayListOf<Any?>().also { out ->
                    expect('[')
                    skipSpaces()
                    while (peek() != ']') {
                        out.add(parse())
                        skipSpaces()
                        if (peek() == ']') break
                        expect(',')
                        skipSpaces()
                    }
                    expect(']')
                }
            }
            '{' -> {
                mutableMapOf<Any?, Any?>().also { out ->
                    expect('{')
                    skipSpaces()
                    while (peek() != '}') {
                        val key = parse()
                        skipSpaces()
                        expect(':')
                        skipSpaces()
                        val value = parse()
                        skipSpaces()
                        out[key] = value
                        if (peek() == '}') break
                        expect(',')
                        skipSpaces()
                    }
                    expect('}')
                }
            }
            else -> TODO("Unexpected json '${peek()}' at $pos in '$str'")
        }
    }

    internal class MiniStrReader(val str: String, var pos: Int = 0) {
        val size get() = str.length
        val available get() = size - pos
        val hasMore get() = available > 0
        val eof get() = available <= 0
        fun peek(): Char = if (hasMore) str[pos] else '\u0000'
        fun peek(count: Int): String = str.substring(pos, min(size, pos + count))
        fun read(): Char = peek().also { skip(1) }
        fun read(count: Int): String = peek(count).also { skip(it.length) }
        fun skip(count: Int = 1) = this.apply { pos += count }
        fun expect(str: String): String {
            if (available < str.length) error("Expected '$str' but eof at $pos in '${this.str}'")
            for (n in str.indices) {
                if (this.str[pos + n] != str[n]) error("Expected '$str' but found '${peek(str.length)}' at $pos in '${this.str}'")
            }
            skip(str.length)
            return str
        }
        fun expect(char: Char): Char {
            if (peek() != char) error("Expected '$char' but found '${peek()}' at $pos in '${this.str}'")
            skip(1)
            return char
        }
        inline fun readRange(body: () -> Unit): String {
            val start = pos
            body()
            return this.str.substring(start, pos)
        }
        inline fun readWhile(check: (Char) -> Boolean): String = readRange { skipWhile(check) }
        inline fun skipWhile(check: (Char) -> Boolean): Int {
            val start = pos
            while (hasMore && check(peek())) skip(1)
            return pos - start
        }
        fun skipSpaces() = skipWhile { it == ' ' || it == '\t' || it == '\r' || it == '\n' }
    }
}