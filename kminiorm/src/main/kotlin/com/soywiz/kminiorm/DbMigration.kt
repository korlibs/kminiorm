package com.soywiz.kminiorm

interface DbMigration<T : DbBaseModel> {
    enum class Action { DROP_TABLE, ADD_COLUMN, DROP_COLUMN, ADD_INDEX, DROP_INDEX }
    suspend fun migrate(table: DbTable<T>, action: Action, column: ColumnDef<T>?): Unit
}