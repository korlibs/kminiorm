package com.soywiz.kminiorm

import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.*
import com.fasterxml.jackson.databind.node.*
import java.io.Serializable
import java.nio.*
import java.security.*
import java.util.*
import java.util.concurrent.atomic.*

class DbKey : Comparable<DbKey>, Serializable {
    private constructor(timestamp: Int, randomValue1: Int, randomValue2: Short, counter: Int, checkCounter: Boolean) {
        require(randomValue1 and -0x1000000 == 0) { "The machine identifier must be between 0 and 16777215 (it must fit in three bytes)." }
        require(!(checkCounter && counter and -0x1000000 != 0)) { "The counter must be between 0 and 16777215 (it must fit in three bytes)." }
        this.timestamp = timestamp
        this.counter = counter and LOW_ORDER_THREE_BYTES
        this.machineIdentifier = randomValue1
        this.processIdentifier = randomValue2
    }

    /**
     * Gets the timestamp (number of seconds since the Unix epoch).
     *
     * @return the timestamp
     */
    val timestamp: Int
    /**
     * Gets the counter.
     *
     * @return the counter
     */
    @get:Deprecated("")
    val counter: Int
    /**
     * Gets the machine identifier.
     *
     * @return the machine identifier
     */
    @get:Deprecated("")
    val machineIdentifier: Int
    /**
     * Gets the process identifier.
     *
     * @return the process identifier
     */
    @get:Deprecated("")
    val processIdentifier: Short

    /**
     * Gets the timestamp as a `Date` instance.
     *
     * @return the Date
     */
    val date: Date
        get() = Date((timestamp and 0xFFFFFFFF.toInt()) * 1000L)

    /**
     * Gets the time of this instance, in milliseconds.
     *
     * @return the time component of this ID in milliseconds
     */
    val time: Long
        @Deprecated("Use #getDate instead")
        get() = (timestamp and 0xFFFFFFFF.toInt()) * 1000L

    /**
     * Constructs a new instance using the given date.
     *
     * @param date the date
     */
    @JvmOverloads
    constructor(date: Date = Date()) : this(dateToTimestampSeconds(date), NEXT_COUNTER.getAndIncrement() and LOW_ORDER_THREE_BYTES, false) {
    }

    /**
     * Constructs a new instances using the given date and counter.
     *
     * @param date    the date
     * @param counter the counter
     * @throws IllegalArgumentException if the high order byte of counter is not zero
     */
    constructor(date: Date, counter: Int) : this(dateToTimestampSeconds(date), counter, true) {}

    /**
     * Constructs a new instances using the given date, machine identifier, process identifier, and counter.
     *
     * @param date              the date
     * @param machineIdentifier the machine identifier
     * @param processIdentifier the process identifier
     * @param counter           the counter
     * @throws IllegalArgumentException if the high order byte of machineIdentifier or counter is not zero
     */
    @Deprecated("Use {@link #ObjectId(Date, int)} instead")
    constructor(date: Date, machineIdentifier: Int, processIdentifier: Short, counter: Int) : this(dateToTimestampSeconds(date), machineIdentifier, processIdentifier, counter) {
    }

    /**
     * Creates an ObjectId using the given time, machine identifier, process identifier, and counter.
     *
     * @param timestamp         the time in seconds
     * @param machineIdentifier the machine identifier
     * @param processIdentifier the process identifier
     * @param counter           the counter
     * @throws IllegalArgumentException if the high order byte of machineIdentifier or counter is not zero
     */
    @Deprecated("Use {@link #ObjectId(int, int)} instead")
    constructor(timestamp: Int, machineIdentifier: Int, processIdentifier: Short, counter: Int) : this(timestamp, machineIdentifier, processIdentifier, counter, true) {
    }

    /**
     * Creates an ObjectId using the given time, machine identifier, process identifier, and counter.
     *
     * @param timestamp         the time in seconds
     * @param counter           the counter
     * @throws IllegalArgumentException if the high order byte of counter is not zero
     */
    constructor(timestamp: Int, counter: Int) : this(timestamp, counter, true) {}

    private constructor(timestamp: Int, counter: Int, checkCounter: Boolean) : this(timestamp, generatedMachineIdentifier, RANDOM_VALUE2, counter, checkCounter) {}

    /**
     * Constructs a new instance from a 24-byte hexadecimal string representation.
     *
     * @param hexString the string to convert
     * @throws IllegalArgumentException if the string is not a valid hex string representation of an ObjectId
     */
    constructor(hexString: String) : this(parseHexString(hexString)) {}

    /**
     * Constructs a new instance from the given byte array
     *
     * @param bytes the byte array
     * @throws IllegalArgumentException if array is null or not of length 12
     */
    constructor(bytes: ByteArray) : this(ByteBuffer.wrap(bytes)) {
        assert(bytes.size == 12) { "bytes has length of 12" }
    }

    /**
     * Creates an ObjectId
     *
     * @param timestamp                   time in seconds
     * @param machineAndProcessIdentifier machine and process identifier
     * @param counter                     incremental value
     */
    internal constructor(timestamp: Int, machineAndProcessIdentifier: Int, counter: Int) : this(legacyToBytes(timestamp, machineAndProcessIdentifier, counter)) {}

    /**
     * Constructs a new instance from the given ByteBuffer
     *
     * @param buffer the ByteBuffer
     * @throws IllegalArgumentException if the buffer is null or does not have at least 12 bytes remaining
     * @since 3.4
     */
    constructor(buffer: ByteBuffer) {
        assert(buffer.remaining() >= OBJECT_ID_LENGTH) { "buffer.remaining() >=12" }

        // Note: Cannot use ByteBuffer.getInt because it depends on tbe buffer's byte order
        // and ObjectId's are always in big-endian order.
        timestamp = makeInt(buffer.get(), buffer.get(), buffer.get(), buffer.get())
        machineIdentifier = makeInt(0.toByte(), buffer.get(), buffer.get(), buffer.get())
        processIdentifier = makeShort(buffer.get(), buffer.get())
        counter = makeInt(0.toByte(), buffer.get(), buffer.get(), buffer.get())
    }

    /**
     * Convert to a byte array.  Note that the numbers are stored in big-endian order.
     *
     * @return the byte array
     */
    fun toByteArray(): ByteArray {
        val buffer = ByteBuffer.allocate(OBJECT_ID_LENGTH)
        putToByteBuffer(buffer)
        return buffer.array()  // using .allocate ensures there is a backing array that can be returned
    }

    /**
     * Convert to bytes and put those bytes to the provided ByteBuffer.
     * Note that the numbers are stored in big-endian order.
     *
     * @param buffer the ByteBuffer
     * @throws IllegalArgumentException if the buffer is null or does not have at least 12 bytes remaining
     * @since 3.4
     */
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

    /**
     * Converts this instance into a 24-byte hexadecimal string representation.
     *
     * @return a string representation of the ObjectId in hexadecimal format
     */
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

        // Use primitives to represent the 5-byte random value.
        /**
         * Gets the generated machine identifier.
         *
         * @return an int representing the machine identifier
         */
        @get:Deprecated("")
        val generatedMachineIdentifier: Int
        private val RANDOM_VALUE2: Short

        private val NEXT_COUNTER = AtomicInteger(SecureRandom().nextInt())

        private val HEX_CHARS = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

        /**
         * Gets a new object id.
         *
         * @return the new id
         */
        fun get(): DbKey {
            return DbKey()
        }

        /**
         * Checks if a string could be an `ObjectId`.
         *
         * @param hexString a potential ObjectId as a String.
         * @return whether the string could be an object id
         * @throws IllegalArgumentException if hexString is null
         */
        fun isValid(hexString: String?): Boolean {
            requireNotNull(hexString)

            val len = hexString.length
            if (len != 24) {
                return false
            }

            return hexString.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }
        }

        private fun legacyToBytes(timestamp: Int, machineAndProcessIdentifier: Int, counter: Int): ByteArray {
            val bytes = ByteArray(OBJECT_ID_LENGTH)
            bytes[0] = int3(timestamp)
            bytes[1] = int2(timestamp)
            bytes[2] = int1(timestamp)
            bytes[3] = int0(timestamp)
            bytes[4] = int3(machineAndProcessIdentifier)
            bytes[5] = int2(machineAndProcessIdentifier)
            bytes[6] = int1(machineAndProcessIdentifier)
            bytes[7] = int0(machineAndProcessIdentifier)
            bytes[8] = int3(counter)
            bytes[9] = int2(counter)
            bytes[10] = int1(counter)
            bytes[11] = int0(counter)
            return bytes
        }

        init {
            try {
                val secureRandom = SecureRandom()
                generatedMachineIdentifier = secureRandom.nextInt(0x01000000)
                RANDOM_VALUE2 = secureRandom.nextInt(0x00008000).toShort()
            } catch (e: Exception) {
                throw RuntimeException(e)
            }

        }

        private fun parseHexString(s: String): ByteArray {
            require(isValid(s)) { "invalid hexadecimal representation of an ObjectId: [$s]" }

            val b = ByteArray(OBJECT_ID_LENGTH)
            for (i in b.indices) {
                b[i] = Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16).toByte()
            }
            return b
        }

        private fun dateToTimestampSeconds(time: Date): Int {
            return (time.time / 1000).toInt()
        }

        // Big-Endian helpers, in this class because all other BSON numbers are little-endian

        private fun makeInt(b3: Byte, b2: Byte, b1: Byte, b0: Byte): Int {
            // CHECKSTYLE:OFF
            return b3.toInt() shl 24 or
                    ((b2.toInt() and 0xff) shl 16) or
                    ((b1.toInt() and 0xff) shl 8) or
                    ((b0.toInt() and 0xff))
            // CHECKSTYLE:ON
        }

        private fun makeShort(b1: Byte, b0: Byte): Short {
            // CHECKSTYLE:OFF
            return ((b1.toInt() and 0xff) shl 8 or (b0.toInt() and 0xff)).toShort()
            // CHECKSTYLE:ON
        }

        private fun int3(x: Int): Byte {
            return (x shr 24).toByte()
        }

        private fun int2(x: Int): Byte {
            return (x shr 16).toByte()
        }

        private fun int1(x: Int): Byte {
            return (x shr 8).toByte()
        }

        private fun int0(x: Int): Byte {
            return x.toByte()
        }

        private fun short1(x: Short): Byte {
            return (x.toInt() shr 8).toByte()
        }

        private fun short0(x: Short): Byte {
            return x.toByte()
        }
    }

}
/**
 * Create a new object id.
 */

fun ObjectMapper.registerDbKeyModule() {
    val mapper = this
    val OID = "\$oid"
    mapper.registerModule(SimpleModule().let { module ->
        module.addSerializer(DbKey::class.java, object : JsonSerializer<DbKey>() {
            override fun serialize(value: DbKey, gen: JsonGenerator, serializers: SerializerProvider) {
                //gen.writeObject(mapOf(OID to value.toHexString()))
                gen.writeString(value.toHexString())
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
