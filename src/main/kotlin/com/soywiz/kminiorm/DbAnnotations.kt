package com.soywiz.kminiorm

@Target(AnnotationTarget.PROPERTY)
annotation class MaxLength(val length: Int)

@Target(AnnotationTarget.PROPERTY)
annotation class Name(val name: String)

@Target(AnnotationTarget.PROPERTY)
annotation class Unique

@Target(AnnotationTarget.PROPERTY)
annotation class Primary

@Target(AnnotationTarget.PROPERTY)
annotation class Index

@Target(AnnotationTarget.PROPERTY)
annotation class Ignore
