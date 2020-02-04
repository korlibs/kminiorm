package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import java.time.*
import java.util.*
import kotlin.reflect.*
import kotlin.reflect.full.*
import kotlin.reflect.jvm.*

val DbTyper: Typer = Typer()
    //.withKeepType<Date>()
    .withTyperUntyper<Date>(
        typer = { it, type ->
            when (it) {
                is String -> Date(it)
                is Date -> it
                else -> Date(0)
            }
        }
    )
    .withTyperUntyper<LocalDate>(
        typer = { it, type ->
            when (it) {
                is String -> LocalDate.parse(it)
                is LocalDate -> it
                else -> LocalDate.MIN
            }
        }
    )
    .withKeepType<UUID>()
    .withPartialTyper()
    .withDbKeyTyperUntyper()
