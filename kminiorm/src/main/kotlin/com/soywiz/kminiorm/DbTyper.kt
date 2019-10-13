package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import java.util.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

val DbTyper: Typer = Typer()
    .withKeepType<Date>()
    .withKeepType<UUID>()
    .withPartialTyper()
    .withDbKeyTyperUntyper()
