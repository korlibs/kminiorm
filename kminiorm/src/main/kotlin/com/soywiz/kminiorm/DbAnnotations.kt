package com.soywiz.kminiorm

@Target(AnnotationTarget.PROPERTY)
annotation class DbMaxLength(val length: Int)

@Target(AnnotationTarget.PROPERTY)
annotation class DbName(val name: String)

@Target(AnnotationTarget.PROPERTY)
annotation class DbUnique(val direction: DbIndexDirection = DbIndexDirection.ASC)

@Target(AnnotationTarget.PROPERTY)
annotation class DbPrimary(val direction: DbIndexDirection = DbIndexDirection.ASC)

@Target(AnnotationTarget.PROPERTY)
annotation class DbIndex(val direction: DbIndexDirection = DbIndexDirection.ASC)

@Target(AnnotationTarget.PROPERTY)
annotation class DbIgnore

enum class DbIndexDirection(val sname: String, val sign: Int) {
    ASC("ASC", +1),
    DESC("DESC", -1)
}