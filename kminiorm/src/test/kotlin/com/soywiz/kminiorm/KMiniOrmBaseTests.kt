package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import kotlinx.coroutines.flow.*
import java.util.*
import kotlin.test.*

abstract class KMiniOrmBaseTests(val db: Db) {
    @Test
    fun testBasicTable() = suspendTest {
        val table = db.table<MyTestTable>()
        table.delete { everything }
        val item0 = table.insert(MyTestTable(
                _id = DbKey("5d9c47664b3f792e8d8ceb33"),
                uuid = UUID.fromString("5190ac73-e244-48fb-a535-36a3b3e653db"),
                date = Date(1570522248800L),
                long = 1234567890123L,
                int = 12345678,
                string = "My String",
                listString = listOf("hello", "world"),
                mapString = mapOf("hello" to "world")
        ))
        val items = table.findAll().toList()
        assertEquals(1, items.size)
        val item1 = items.first()
        //println("item0: $item0")
        //println("item1: $item1")
        assertEquals(item0._id, item1._id)
        assertEquals(item0.uuid, item1.uuid)
        assertEquals(item0.date, item1.date)
        assertEquals(item0.long, item1.long)
        assertEquals(item0.int, item1.int)
        assertEquals(item0.string, item1.string)
        assertEquals(item0.listString, item1.listString)
        assertEquals(item0.mapString, item1.mapString)
        assertEquals(item0, item1)

        assertEquals(1L, table.update(set = Partial(MyTestTable::int to 3, MyTestTable::string to "hello")) { id(item0._id) })
        assertEquals(3, table.findById(item0._id)?.int)
        assertEquals("hello", table.findById(item0._id)?.string)

        table.delete { everything }
        assertEquals(0, table.findAll().count())
    }

    @Test
    fun testBasicCounter() = suspendTest {
        val table = db.table<MyCounterTable>()
        table.delete { everything }
        val item = table.insert(MyCounterTable())
        assertEquals(
                1L,
                table.update(
                    increment = Partial(
                            MyCounterTable::counter1 to +10,
                            MyCounterTable::counter2 to -10
                    )
                ) { MyCounterTable::_id eq item._id }
        )
        val item2 = table.findById(item._id)
        assertEquals(1010, item2?.counter1)
        assertEquals(990, item2?.counter2)
    }

    @Test
    fun testUniqueConflictOnInsert() = suspendTest {
        val table = db.table<UniqueModel>()
        val a = UniqueModel("test")
        val b = UniqueModel("test")
        table.delete { everything }
        table.insert(a)
        assertFailsWith<DuplicateKeyDbException> {
            table.insert(b)
        }
    }

    @Test
    fun testUpsert() = suspendTest {
        val table = db.table<UpsertTestModel>()
        val a = UpsertTestModel("test", "a", _id = DbKey("000000000000000000000001"))
        val b = UpsertTestModel("test", "b", _id = DbKey("000000000000000000000002"))
        table.delete { everything }
        table.insert(a)
        val c = table.upsert(b)
        //println(table.find { everything }.joinToString("\n"))
        assertEquals(1, table.find { everything }.count())
        assertEquals("UpsertTestModel(name=test, value=a, _id=000000000000000000000001)", a.toString())
        assertEquals("UpsertTestModel(name=test, value=b, _id=000000000000000000000002)", b.toString())
        assertEquals("UpsertTestModel(name=test, value=b, _id=000000000000000000000001)", c.toString())
    }

    @Test
    fun testTableUpgrade() = suspendTest {
        val tableV1 = db.table<TableV1>()
        val tableV2 = db.table<TableV2>()
        tableV1.delete { everything }
        val row1 = tableV1.insert(TableV1("hello", "world"))
        val row2 = tableV2.findOne { TableV2::name eq "hello" }
        assertEquals("hello", row2?.name)
        assertEquals("", row2?.fieldV2)
        assertEquals(1, tableV2.findAll().count())
    }

    @Test
    fun testTableExtrinsicData() = suspendTest {
        val table = db.table<TableExtrinsic>()
        table.delete { everything }
        table.insert(mapOf("_id" to DbKey().toHexString(), "name" to "hello", "surname" to "world"))
        val item = table.findOne()
        assertEquals("hello", item?.name)
        assertEquals("world", item?.get("surname"))
    }

    @Test
    fun testMultiColumnIndex() = suspendTest {
        val table = db.table<MultiColumnIndexTable>()
        table.delete { everything }
        table.insert(MultiColumnIndexTable(a = "a", b = "b1", c = "c1"))
        table.insert(MultiColumnIndexTable(a = "a", b = "b2", c = "c2"))
        assertFailsWith<DuplicateKeyDbException> {
            table.insert(MultiColumnIndexTable(a = "a", b = "b1", c = "c3"))
        }
        assertFailsWith<DuplicateKeyDbException> {
            table.insert(MultiColumnIndexTable(a = "a", b = "b2", c = "c4"))
        }
        table.upsert(MultiColumnIndexTable(a = "a", b = "b1", c = "c3"))
        table.upsert(MultiColumnIndexTable(a = "a", b = "b2", c = "c4"))

        assertEquals(2, table.findAll().count())
        assertEquals("c3", table.findOne { (MultiColumnIndexTable::a eq "a") AND (MultiColumnIndexTable::b eq "b1") }?.c)
        assertEquals("c4", table.findOne { (MultiColumnIndexTable::a eq "a") AND (MultiColumnIndexTable::b eq "b2") }?.c)
    }

    @Test
    fun testStoreTypes() = suspendTest {
        val table = db.table<ArrayOfDbKey>()
        table.delete { everything }
        val item = table.insert(ArrayOfDbKey(listOf()))
        item.copy(items = listOf(DbKey("000000000000000000000001"), DbKey("000000000000000000000002")))
        table.update(Partial(mapOf(ArrayOfDbKey::items.name to listOf(DbKey("000000000000000000000001"), DbKey("000000000000000000000002"))), ArrayOfDbKey::class)) { id(item._id) }
        table.update(Partial(mapOf("list" to listOf(DbKey("000000000000000000000001"), DbKey("000000000000000000000002"))), ArrayOfDbKey::class)) { id(item._id) }
        table.update(Partial(item).withOnly(ArrayOfDbKey::items)) { id(item._id) }
        //table.update(Partial(mapOf("list" to listOf("000000000000000000000001", "000000000000000000000002")), ArrayOfDbKey::class)) { id(item._id) }
    }

    @Test
    fun testUpdate1() = suspendTest {
        val table = db.table<Update1>()
        table.delete { everything }
        table.insert(Update1("a", "b", "c"))
        for (item in table.findAll()) {
            table.update(Partial(Update1::latestPublishedVersion to "d")) { Update1::_id eq item._id }
        }
        assertEquals("d", table.findAll().first().latestPublishedVersion)
    }

    data class Update1(
        @DbUnique("project_channel")
        val project: String,
        @DbUnique("project_channel")
        val channel: String,
        val latestPublishedVersion: String
    ) : DbModel.Base<Update1>()


    @Test
    fun testArrayOfCustom() = suspendTest {
        val table = db.table<ArrayOfCustom>().apply { delete { everything } }
        val item = table.insert(ArrayOfCustom(listOf()))
        table.update(Partial(mapOf(
            "items" to listOf(mapOf("a" to 1, "b" to 2, "c" to 3))
        ), ArrayOfCustom::class)) { id(item._id) }
        assertEquals("[Custom(a=1, b=2, c=3)]", table.findOne { everything }?.items?.toString())
    }

    @Test
    fun testRefs() = suspendTest {
        val refs1 = db.table<Ref1>().apply { delete { everything } }
        val refs2 = db.table<Ref2>().apply { delete { everything } }
        val rref2 = Ref2("hello")
        val ref2 = refs2.insert(rref2)
        val ref1 = refs1.insert(Ref1(listOf(ref2._id)))
        assertEquals(1, refs1.find { everything }.count())
        assertEquals(1, refs2.find { everything }.count())
        val items = refs1.find { everything }.first().items.resolved(refs2)
        assertEquals(listOf(rref2.name), items.map { it?.name })
    }

    @Test
    fun testEverything() = suspendTest {
        val simples = db.table<Simple>().apply { delete { everything } }
        val s1 = Simple(1, 2, 3)
        val s2 = Simple(10, 20, 30)
        val s3 = Simple(100, 200, 300)
        simples.insert(s1)
        simples.insert(s2)
        simples.insert(s3)
        val result = simples.findFlowPartial(skip = 1L, limit = 2L, fields = listOf(Simple::b, Simple::c), sorted = listOf(Simple::c to -1)) { everything }
        //assertEquals("""[{"b":20,"c":30},{"b":2,"c":3}]""", MiniJson.stringify(result.map { it.without(Simple::_id).data }.toList()))
        assertEquals("""[{"b":20,"c":30},{"b":2,"c":3}]""", MiniJson.stringify(result.map { it.data }.toList()))
    }

    @Test
    fun testOrIn() = suspendTest {
        val simples = db.table<Simple>().apply { delete { everything } }
        val s1 = Simple(1, 2, 3)
        val s2 = Simple(10, 20, 30)
        val s3 = Simple(100, 200, 300)
        simples.insert(s1)
        simples.insert(s2)
        simples.insert(s3)
        assertEquals(listOf(s2, s3), simples.find { (Simple::b eq 20) OR (Simple::c eq 300) }.sortedBy { it.b })
        assertEquals(listOf(s2, s3), simples.find { (Simple::b IN listOf(20, 200)) }.sortedBy { it.b })
    }

    @Test
    fun testOrIn2() = suspendTest {
        val simples = db.table<Simple>().apply { delete { everything } }
        val s1 = Simple(1, 2, 3)
        val s2 = Simple(10, 20, 30)
        val s3 = Simple(100, 200, 300)
        simples.insert(s1)
        simples.insert(s2)
        simples.insert(s3)
        assertEquals(listOf(s2, s3), simples.find { (Simple::_id eq s2._id) OR (Simple::_id eq s3._id) }.sortedBy { it.b })
        assertEquals(listOf(s2, s3), simples.find { (Simple::_id IN listOf(s2._id, s3._id)) }.sortedBy { it.b })
    }

    @Test
    fun testGe() = suspendTest {
        val simples = db.table<Simple>().apply { delete { everything } }
        val s1 = Simple(1, 2, 3)
        val s2 = Simple(10, 20, 30)
        val s3 = Simple(100, 200, 300)
        simples.insert(s1)
        simples.insert(s2)
        simples.insert(s3)
        assertEquals(listOf(s3), simples.find { (Simple::b gt 20) }.sortedBy { it.b })
        assertEquals(listOf(s2, s3), simples.find { (Simple::b ge 20) }.sortedBy { it.b })
        assertEquals(listOf(s1, s2), simples.find { (Simple::b le 20) }.sortedBy { it.b })
        assertEquals(listOf(s1), simples.find { (Simple::b lt 20) }.sortedBy { it.b })
    }

    @Test
    fun testEnum() = suspendTest {
        val simples = db.table<CustomWithEnum>().apply { delete { everything } }
        val item1 = CustomWithEnum(1, "hello", CustomEnum.HELLO)
        simples.insert(item1)
        assertEquals(1, simples.findAll().count())
        assertEquals(item1, simples.findAll().first())
        assertEquals(item1, simples.find { CustomWithEnum::menum eq CustomEnum.HELLO }.first())
        assertEquals(0, simples.find { CustomWithEnum::menum eq CustomEnum.WORLD }.count())
    }

    @Test
    fun testIntKey() = suspendTest {
        val table = db.table<MyIntKey>().apply { delete { everything } }
        table.insert(MyIntKey(DbIntRef(100L)))
        assertEquals("100", table.findAll().first().key.toString())
    }

    data class MyIntKey(
        val key: DbIntRef<MyIntKey>
    ) : DbModel.Base<MyIntKey>()

    data class CustomWithEnum(
            val a: Int,
            val c: String,
            val menum: CustomEnum,
            override val _id: DbRef<Ref1> = DbRef()
    ) : DbModel

    enum class CustomEnum { TEST, HELLO, WORLD }

    @Test
    fun test200Insert() = suspendTest {
        val simples = db.table<BigTable>().apply { delete { everything } }
        for (n in 0 until 200) simples.insert(BigTable(n))
        assertEquals(200, simples.find { everything }.size)
    }

    data class BigTable(val a: Int) : DbModel.Base<BigTable>()

    data class Custom(val a: Int, val b: Int, val c: Int)

    data class Simple(
            val a: Int, val b: Int, val c: Int,
            override val _id: DbRef<Ref1> = DbRef()
    ) : DbModel

    data class Ref1(
        val items: List<DbRef<Ref2>>,
        override val _id: DbRef<Ref1> = DbRef()
    ) : DbModel

    data class Ref2(
        val name: String,
        override val _id: DbRef<Ref2> = DbRef()
    ) : DbModel

    data class ArrayOfCustom(
        val items: List<Custom>,
        override val _id: DbKey = DbKey()
    ) : DbModel

    data class ArrayOfDbKey(
        val items: List<DbKey>,
        override val _id: DbKey = DbKey()
    ) : DbModel

    data class StoreTypesTable(
        val bool: Boolean,
        override val _id: DbKey = DbKey()
    ) : DbModel

    data class MultiColumnIndexTable(
        @DbUnique(name = "a_b")
        val a: String,
        @DbUnique(name = "a_b")
        val b: String,
        val c: String,
        override val _id: DbRef<MultiColumnIndexTable> = DbRef()
    ) : DbModel.BaseWithExtrinsic<MultiColumnIndexTable>(_id)

    data class TableExtrinsic(
        @DbUnique
        val name: String,
        override val _id: DbKey = DbKey()
    ) : DbModel, ExtrinsicData by ExtrinsicData.Mixin()

    @DbName("MyUpgradeableTable")
    data class TableV1(
        @DbUnique
        val name: String,
        val fieldV1: String,
        override val _id: DbKey = DbKey()
    ) : DbModel

    @DbName("MyUpgradeableTable")
    data class TableV2(
        @DbUnique
        val name: String,
        val fieldV2: String,
        override val _id: DbKey = DbKey()
    ) : DbModel

    data class UpsertTestModel(
            @DbUnique
            val name: String,
            val value: String,
            override val _id: DbKey = DbKey()
    ) : DbModel

    data class UniqueModel(
            @DbUnique
            val name: String,
            override val _id: DbKey = DbKey()
    ) : DbModel

    data class MyCounterTable(
            override val _id: DbKey = DbKey(),
            val counter1: Int = 1000,
            val counter2: Int = 1000
    ) : DbModel

    data class MyTestTable(
            override val _id: DbKey = DbKey(),
            val uuid: UUID,
            val date: Date,
            val long: Long,
            val int: Int,
            val string: String,
            val listString: List<String>,
            val mapString: Map<String, Any?>
    ) : DbModel
}
