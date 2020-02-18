package com.soywiz.kminiorm.memory

import com.soywiz.kminiorm.*
import kotlin.test.*
import kotlin.test.Ignore

class MemoryJdbcKMiniOrmTest : KMiniOrmBaseTests(MemoryDb()) {
    @Test
    @Ignore
    // Unsupported
    override fun testTableUpgrade() = suspendTest {
        TODO()
    }

    @Test
    @Ignore
    // Unsupported
    override fun testTableExtrinsicData() = suspendTest {
        TODO()
    }
}
