package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import java.io.Serializable
import java.nio.*
import java.security.*
import java.util.*
import java.util.concurrent.atomic.*

// From MongoDB DbKey
class DbRef<T : DbTableElement> : DbKey {
    @JvmOverloads
    constructor(date: Date = Date()) : super(date)
    constructor(hexString: String) : super(hexString)
    constructor(bytes: ByteArray) : super(bytes)
    internal constructor(timestamp: Int, randomValue1: Int, randomValue2: Short, counter: Int, checkCounter: Boolean) : super(timestamp, randomValue1, randomValue2, counter, checkCounter)
}

class DbIntRef<T : DbTableElement>(key: Long = 0L) : DbIntKey(key) {
    constructor(key: Int) : this(key.toLong())
}

interface DbBaseKey

open class DbIntKey(val key: Long = 0L) : Comparable<DbIntKey>, Serializable, DbBaseKey {
    fun <T : DbTableElement> asRef() = DbIntRef<T>(key)

    override fun compareTo(other: DbIntKey): Int = this.key.compareTo(other.key)
    override fun toString(): String = "$key"
}

open class DbKey : Comparable<DbKey>, Serializable, DbBaseKey {
    fun <T : DbTableElement> asRef() = DbRef<T>(timestamp, machineIdentifier, processIdentifier, counter, false)

    internal constructor(timestamp: Int, randomValue1: Int, randomValue2: Short, counter: Int, checkCounter: Boolean) {
        require(randomValue1 and -0x1000000 == 0) { "The machine identifier must be between 0 and 16777215 (it must fit in three bytes)." }
        require(!(checkCounter && counter and -0x1000000 != 0)) { "The counter must be between 0 and 16777215 (it must fit in three bytes)." }
        this.timestamp = timestamp
        this.counter = counter and LOW_ORDER_THREE_BYTES
        this.machineIdentifier = randomValue1
        this.processIdentifier = randomValue2
    }
    val timestamp: Int
    @get:Deprecated("")
    val counter: Int
    @get:Deprecated("")
    val machineIdentifier: Int
    @get:Deprecated("")
    val processIdentifier: Short

    val date: Date get() = Date((timestamp and 0xFFFFFFFF.toInt()) * 1000L)
    @Deprecated("Use #getDate instead")
    val time: Long get() = (timestamp and 0xFFFFFFFF.toInt()) * 1000L

    @JvmOverloads
    constructor(date: Date = Date()) : this(dateToTimestampSeconds(date), NEXT_COUNTER.getAndIncrement() and LOW_ORDER_THREE_BYTES, false)
    constructor(date: Date, counter: Int) : this(dateToTimestampSeconds(date), counter, true)
    constructor(timestamp: Int, counter: Int) : this(timestamp, counter, true)
    private constructor(timestamp: Int, counter: Int, checkCounter: Boolean) : this(timestamp, generatedMachineIdentifier, RANDOM_VALUE2, counter, checkCounter) {}
    constructor(hexString: String) : this(parseHexString(hexString.takeIf { it.isNotEmpty() } ?: "000000000000000000000000"))
    constructor(bytes: ByteArray) : this(ByteBuffer.wrap(bytes)) {
        assert(bytes.size == 12) { "bytes has length of 12" }
    }
    constructor(buffer: ByteBuffer) {
        assert(buffer.remaining() >= OBJECT_ID_LENGTH) { "buffer.remaining() >=12" }

        // Note: Cannot use ByteBuffer.getInt because it depends on tbe buffer's byte order
        // and DbKey's are always in big-endian order.
        timestamp = makeInt(buffer.get(), buffer.get(), buffer.get(), buffer.get())
        machineIdentifier = makeInt(0.toByte(), buffer.get(), buffer.get(), buffer.get())
        processIdentifier = makeShort(buffer.get(), buffer.get())
        counter = makeInt(0.toByte(), buffer.get(), buffer.get(), buffer.get())
    }

    fun toByteArray(): ByteArray {
        val buffer = ByteBuffer.allocate(OBJECT_ID_LENGTH)
        putToByteBuffer(buffer)
        return buffer.array()  // using .allocate ensures there is a backing array that can be returned
    }

    fun putToByteBuffer(buffer: ByteBuffer) {
        assert(buffer.remaining() >= OBJECT_ID_LENGTH) { "buffer.remaining() >=12" }

        buffer.put(int3(timestamp))
        buffer.put(int2(timestamp))
        buffer.put(int1(timestamp))
        buffer.put(int0(timestamp))
        buffer.put(int2(machineIdentifier))
        buffer.put(int1(machineIdentifier))
        buffer.put(int0(machineIdentifier))
        buffer.put(short1(processIdentifier))
        buffer.put(short0(processIdentifier))
        buffer.put(int2(counter))
        buffer.put(int1(counter))
        buffer.put(int0(counter))
    }

    fun toHexString(): String {
        val chars = CharArray(OBJECT_ID_LENGTH * 2)
        var i = 0
        for (b in toByteArray()) {
            chars[i++] = HEX_CHARS[(b.toInt() shr 4) and 0xF]
            chars[i++] = HEX_CHARS[b.toInt() and 0xF]
        }
        return String(chars)
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val objectId = o as DbKey?
        if (counter != objectId!!.counter) return false
        if (timestamp != objectId.timestamp) return false
        if (machineIdentifier != objectId.machineIdentifier) return false
        return if (processIdentifier != objectId.processIdentifier) false else true
    }

    override fun hashCode(): Int {
        var result = timestamp
        result = 31 * result + counter
        result = 31 * result + machineIdentifier
        result = 31 * result + processIdentifier
        return result
    }

    override fun compareTo(other: DbKey): Int {
        val byteArray = toByteArray()
        val otherByteArray = other.toByteArray()
        for (i in 0 until OBJECT_ID_LENGTH) {
            if (byteArray[i] != otherByteArray[i]) {
                return if ((byteArray[i].toInt() and 0xff) < (otherByteArray[i].toInt() and 0xff)) -1 else 1
            }
        }
        return 0
    }

    override fun toString(): String = toHexString()

    companion object {
        private const val serialVersionUID = 3670079982654483072L
        private val OBJECT_ID_LENGTH = 12
        private val LOW_ORDER_THREE_BYTES = 0x00ffffff
        private val secureRandom = SecureRandom()
        @get:Deprecated("")
        val generatedMachineIdentifier: Int = runCatching { secureRandom.nextInt(0x01000000) }.getOrElse { 0 }
        private val RANDOM_VALUE2: Short = runCatching { secureRandom.nextInt(0x00008000).toShort() }.getOrElse { 0 }
        private val NEXT_COUNTER = AtomicInteger(SecureRandom().nextInt())
        private val HEX_CHARS = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')
        fun get(): DbKey = DbKey()

        fun isValid(hexString: String?): Boolean {
            requireNotNull(hexString)
            val len = hexString.length
            if (len != 24) return false
            return hexString.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }
        }

        private fun parseHexString(s: String): ByteArray {
            require(isValid(s)) {
                "invalid hexadecimal representation of an DbKey: [$s]"
            }
            val b = ByteArray(OBJECT_ID_LENGTH)
            for (i in b.indices) b[i] = Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16).toByte()
            return b
        }

        private fun dateToTimestampSeconds(time: Date): Int = (time.time / 1000).toInt()

        // Big-Endian helpers, in this class because all other BSON numbers are little-endian

        private fun makeInt(b3: Byte, b2: Byte, b1: Byte, b0: Byte): Int = b3.toInt() shl 24 or
                ((b2.toInt() and 0xff) shl 16) or
                ((b1.toInt() and 0xff) shl 8) or
                ((b0.toInt() and 0xff))

        private fun makeShort(b1: Byte, b0: Byte): Short = ((b1.toInt() and 0xff) shl 8 or (b0.toInt() and 0xff)).toShort()
        private fun int3(x: Int): Byte = (x shr 24).toByte()
        private fun int2(x: Int): Byte = (x shr 16).toByte()
        private fun int1(x: Int): Byte = (x shr 8).toByte()
        private fun int0(x: Int): Byte = x.toByte()
        private fun short1(x: Short): Byte = (x.toInt() shr 8).toByte()
        private fun short0(x: Short): Byte = x.toByte()
    }

}
/**
 * Create a new object id.
 */

/*
fun ObjectMapper.registerDbKeyModule(serializeAsString: Boolean = true) {
    val mapper = this
    val OID = "\$oid"
    mapper.registerModule(SimpleModule().let { module ->
        module.addSerializer(DbKey::class.java, object : JsonSerializer<DbKey>() {
            override fun serialize(value: DbKey, gen: JsonGenerator, serializers: SerializerProvider) {
                if (serializeAsString) {
                    gen.writeString(value.toHexString())
                } else {
                    gen.writeObject(mapOf(OID to value.toHexString()))
                }
            }
        })
        module.addDeserializer(DbKey::class.java, object : JsonDeserializer<DbKey>() {
            override fun deserialize(p: JsonParser, ctxt: DeserializationContext): DbKey {
                val node = p.codec.readTree<TreeNode>(p)
                if (node is TextNode) return DbKey(node.textValue())
                return DbKey((node.get(OID) as TextNode).textValue())
            }
        })
    })
}
*/

fun Typer.withDbKeyTyperUntyper(): Typer = this
    .withTyperUntyper<DbRef<DbTableElement>>(
        typer = { it, type ->
            when (it) {
                is DbRef<*> -> it as DbRef<DbTableElement>
                is DbKey -> it.asRef()
                is String -> DbRef(it)
                else -> DbRef()
            }
        },
        untyper = {
            it.toHexString()
        }
    )
    .withTyperUntyper<DbKey>(
        typer = { it, type ->
            when (it) {
                is DbKey -> it
                is String -> DbKey(it)
                else -> DbKey()
            }
        },
        untyper = {
            it.toHexString()
        }
    )
    .withTyperUntyper<DbIntRef<DbTableElement>>(
        typer = { it, type ->
            when (it) {
                is DbIntRef<*> -> it as DbIntRef<DbTableElement>
                is DbIntKey -> it.asRef()
                is String -> DbIntRef(it.toLong())
                is Long -> DbIntRef(it.toLong())
                is Number -> DbIntRef(it.toLong())
                else -> DbIntRef()
            }
        },
        untyper = {
            it.key
        }
    )
    .withTyperUntyper<DbIntKey>(
        typer = { it, type ->
            when (it) {
                is DbIntKey -> it
                is String -> DbIntKey(it.toLong())
                is Long -> DbIntKey(it.toLong())
                is Number -> DbIntKey(it.toLong())
                else -> DbIntKey()
            }
        },
        untyper = {
            it.key
        }
    )
