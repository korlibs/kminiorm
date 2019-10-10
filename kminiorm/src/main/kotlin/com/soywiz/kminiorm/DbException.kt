package com.soywiz.kminiorm

open class DbException(message: String, cause: Throwable? = null) : Throwable(message, cause)
open class DuplicateKeyDbException(message: String, cause: Throwable? = null) : DbException(message, cause)
