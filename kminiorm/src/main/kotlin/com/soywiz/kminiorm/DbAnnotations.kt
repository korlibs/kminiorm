package com.soywiz.kminiorm

@Target(AnnotationTarget.PROPERTY)
annotation class DbMaxLength(val length: Int)

@Target(AnnotationTarget.PROPERTY)
annotation class DbName(val name: String)

@Target(AnnotationTarget.PROPERTY)
annotation class DbUnique

@Target(AnnotationTarget.PROPERTY)
annotation class DbPrimary

@Target(AnnotationTarget.PROPERTY)
annotation class DbIndex

@Target(AnnotationTarget.PROPERTY)
annotation class DbIgnore
