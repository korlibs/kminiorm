package com.soywiz.kminiorm

import kotlin.reflect.KClass

@Target(AnnotationTarget.PROPERTY)
annotation class DbMaxLength(val length: Int)

@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class DbName(val name: String)

@Target(AnnotationTarget.PROPERTY)
annotation class DbUnique(val name: String = "", val direction: DbIndexDirection = DbIndexDirection.ASC, val order: Int = 0)

@Target(AnnotationTarget.PROPERTY)
annotation class DbPrimary(val direction: DbIndexDirection = DbIndexDirection.ASC)

@Target(AnnotationTarget.PROPERTY)
annotation class DbAutoIncrement()

@Target(AnnotationTarget.PROPERTY)
annotation class DbIndex(val name: String = "", val direction: DbIndexDirection = DbIndexDirection.ASC, val order: Int = 0)

@Target(AnnotationTarget.PROPERTY)
annotation class DbIgnore

enum class DbIndexDirection(val sname: String, val sign: Int) {
    ASC("ASC", +1),
    DESC("DESC", -1)
}

@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class DbPerformMigration(val migration: KClass<out DbMigration<*>>)
